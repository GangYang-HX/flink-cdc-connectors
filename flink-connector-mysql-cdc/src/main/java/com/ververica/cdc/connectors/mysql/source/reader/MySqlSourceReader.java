/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaEvent;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.LatestFinishedSplitsSizeEvent;
import com.ververica.cdc.connectors.mysql.source.events.LatestFinishedSplitsSizeRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.SuspendBinlogReaderAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.SuspendBinlogReaderEvent;
import com.ververica.cdc.connectors.mysql.source.events.WakeupReaderEvent;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.events.WakeupReaderEvent.WakeUpTarget.SNAPSHOT_READER;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit.toNormalBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit.toSuspendedBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils.getNextMetaGroupId;

/** The source reader for MySQL source splits. */
public class MySqlSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecord, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);

    private final MySqlSourceConfig sourceConfig;
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, MySqlBinlogSplit> uncompletedBinlogSplits;
    private final int subtaskId;
    private final MySqlSourceReaderContext mySqlSourceReaderContext;
    private MySqlBinlogSplit suspendedBinlogSplit;

    public MySqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<MySqlSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, MySqlSplitState> recordEmitter,
            Configuration config,
            MySqlSourceReaderContext context,
            MySqlSourceConfig sourceConfig) {
        super(
                elementQueue,
                /**
                 * new SingleThreadFetcherManager:一个简单的fetcher管理器，做一些读取操作
                 * 流程描述：
                 *    1.SingleThreadFetcherManager.createSplitFetcher构建一个SplitFetcher（实现来Runnable）；
                 *    2.在SplitFetcher中会构建一个fetcherTask;
                 *    3.在SplitFetcher.run方法中循环调用this.runOnce()；
                 *    4.this.runOnce()会持续调用fetcherTask.run()读取数据；
                 *    5.fetcherTask.run()会调用MysqlSplitReader.fetcher()，返回reader读取的数据；
                 *    6.将读取的数据放入到elementQueue中。
                 */
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedBinlogSplits = new HashMap<>();
        this.subtaskId = context.getSourceReaderContext().getIndexOfSubtask();
        this.mySqlSourceReaderContext = context;
        this.suspendedBinlogSplit = null;
    }

    @Override
    public void start() { //启动reader
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override //当reader分配到新的split的时候，会初始化一个split的state
    protected MySqlSplitState initializedState(MySqlSplit split) {
        if (split.isSnapshotSplit()) {
            return new MySqlSnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new MySqlBinlogSplitState(split.asBinlogSplit());
        }
    }

    @Override
    public List<MySqlSplit> snapshotState(long checkpointId) {
        List<MySqlSplit> stateSplits = super.snapshotState(checkpointId);

        // unfinished splits
        List<MySqlSplit> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .collect(Collectors.toList());

        // add finished snapshot splits that didn't receive ack yet
        unfinishedSplits.addAll(finishedUnackedSplits.values());

        // add binlog splits who are uncompleted
        unfinishedSplits.addAll(uncompletedBinlogSplits.values());

        // add suspended BinlogSplit
        if (suspendedBinlogSplit != null) {
            unfinishedSplits.add(suspendedBinlogSplit);
        }
        return unfinishedSplits;
    }

    @Override //清理已处理完成的split状态
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        for (MySqlSplitState mySqlSplitState : finishedSplitIds.values()) {
            MySqlSplit mySqlSplit = mySqlSplitState.toMySqlSplit();
            if (mySqlSplit.isBinlogSplit()) {
                LOG.info(
                        "binlog split reader suspended due to newly added table, offset {}",
                        mySqlSplitState.asBinlogSplitState().getStartingOffset());

                mySqlSourceReaderContext.resetStopBinlogSplitReader();
                suspendedBinlogSplit = toSuspendedBinlogSplit(mySqlSplit.asBinlogSplit());
                context.sendSourceEventToCoordinator(new SuspendBinlogReaderAckEvent());
            } else {
                finishedUnackedSplits.put(mySqlSplit.splitId(), mySqlSplit.asSnapshotSplit());
            }
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
    }

    /**
     * 添加此reader要读取的split列表，当SplitEnumerator通过SplitEnumeratorContext分配一个split时将调用此方法。
     * 即调用context.assignSplit(SourceSplit,int) 或者 context.assignSplits(SplitsAssignment)
     * @param splits
     */
    @Override
    public void addSplits(List<MySqlSplit> splits) {
        // restore for finishedUnackedSplits
        List<MySqlSplit> unfinishedSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            LOG.info("Add Split: " + split);
            if (split.isSnapshotSplit()) { //判断是否是snapshot split
                MySqlSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                MySqlBinlogSplit binlogSplit = split.asBinlogSplit();
                // the binlog split is suspended
                if (binlogSplit.isSuspended()) { //split是否被挂起
                    suspendedBinlogSplit = binlogSplit;
                } else if (!binlogSplit.isCompletedSplit()) { //如果binlog split未完成则加入未完成的列表中，并向splitEnumerator发送binlog split meta请求的事件
                    uncompletedBinlogSplits.put(split.splitId(), split.asBinlogSplit());
                    requestBinlogSplitMetaIfNeeded(split.asBinlogSplit());
                } else {
                    uncompletedBinlogSplits.remove(split.splitId()); //从未完成的split集合删除指定split，表示未完成的split集合没有该split meta的信息
                    MySqlBinlogSplit mySqlBinlogSplit = //创建未binlog split，带有table schema信息
                            discoverTableSchemasForBinlogSplit(split.asBinlogSplit());
                    // 添加到未完成的split列表中，后续会进行read操作
                    unfinishedSplits.add(mySqlBinlogSplit);
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        }
    }

    private MySqlBinlogSplit discoverTableSchemasForBinlogSplit(MySqlBinlogSplit split) {
        final String splitId = split.splitId();
        if (split.getTableSchemas().isEmpty()) { //如果tableSchema不存在则填充，否则直接返回split
            try (MySqlConnection jdbc = //静态方法，构建一个mysqlConnection，可以认为就是一个jdbc connect;
                         DebeziumUtils.createMySqlConnection(sourceConfig)) {
                Map<TableId, TableChanges.TableChange> tableSchemas =
                        //静态方法，根据我们SourceBuilder构建的时候给定的database和tableList来构建对应的tableId和TableChange，然后我们在read的时候需要
                        TableDiscoveryUtils.discoverCapturedTableSchemas(sourceConfig, jdbc);
                LOG.info("The table schema discovery for binlog split {} success", splitId);
                return MySqlBinlogSplit.fillTableSchemas(split, tableSchemas); //静态方法，构建一个带有tableSchema的MysqlBinlogSplit
            } catch (SQLException e) {
                LOG.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "The binlog split {} has table schemas yet, skip the table schema discovery",
                    split);
            return split;
        }
    }

    // 处理source自定义事件，接收来自SplitEnumerator,与SplitEnumerator类似
    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof BinlogSplitMetaEvent) {
            LOG.debug(
                    "The subtask {} receives binlog meta with group id {}.",
                    subtaskId,
                    ((BinlogSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForBinlogSplit((BinlogSplitMetaEvent) sourceEvent);
        } else if (sourceEvent instanceof SuspendBinlogReaderEvent) {
            mySqlSourceReaderContext.setStopBinlogSplitReader();
        } else if (sourceEvent instanceof WakeupReaderEvent) {
            WakeupReaderEvent wakeupReaderEvent = (WakeupReaderEvent) sourceEvent;
            if (wakeupReaderEvent.getTarget() == SNAPSHOT_READER) {
                context.sendSplitRequest();
            } else {
                if (suspendedBinlogSplit != null) {
                    context.sendSourceEventToCoordinator(
                            new LatestFinishedSplitsSizeRequestEvent());
                }
            }
        } else if (sourceEvent instanceof LatestFinishedSplitsSizeEvent) {
            if (suspendedBinlogSplit != null) {
                final int finishedSplitsSize =
                        ((LatestFinishedSplitsSizeEvent) sourceEvent).getLatestFinishedSplitsSize();
                final MySqlBinlogSplit binlogSplit =
                        toNormalBinlogSplit(suspendedBinlogSplit, finishedSplitsSize);
                suspendedBinlogSplit = null;
                this.addSplits(Collections.singletonList(binlogSplit));
            }
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
            for (MySqlSnapshotSplit split : finishedUnackedSplits.values()) {
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    //发送请求binlogSplit meta事件
    private void requestBinlogSplitMetaIfNeeded(MySqlBinlogSplit binlogSplit) {
        final String splitId = binlogSplit.splitId();
        if (!binlogSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            BinlogSplitMetaRequestEvent splitMetaRequestEvent =
                    new BinlogSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of binlog split {} has been collected success", splitId);
            this.addSplits(Arrays.asList(binlogSplit));
        }
    }

    private void fillMetaDataForBinlogSplit(BinlogSplitMetaEvent metadataEvent) {
        MySqlBinlogSplit binlogSplit = uncompletedBinlogSplits.get(metadataEvent.getSplitId());
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo> metaDataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(FinishedSnapshotSplitInfo::deserialize)
                                .collect(Collectors.toList());
                uncompletedBinlogSplits.put(
                        binlogSplit.splitId(),
                        MySqlBinlogSplit.appendFinishedSplitInfos(binlogSplit, metaDataGroup));

                LOG.info("Fill meta data of group {} to binlog split", metaDataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder binlog meta event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(binlogSplit);
        } else {
            LOG.warn(
                    "Received binlog meta event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState.toMySqlSplit();
    }
}
