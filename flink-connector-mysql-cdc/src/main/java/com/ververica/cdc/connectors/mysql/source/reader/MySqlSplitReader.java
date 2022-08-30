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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import com.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.split.MySqlRecords;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createBinaryClient;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createMySqlConnection;

/**
 * The {@link SplitReader} implementation for the {@link MySqlSource}.
 */
public class MySqlSplitReader implements SplitReader<SourceRecord, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSplitReader.class);
    private final Queue<MySqlSplit> splits;
    private final MySqlSourceConfig sourceConfig;
    private final int subtaskId;
    private final MySqlSourceReaderContext context;

    /**
     * currentReader主要有两种实现：BinlogSplitReader和SnapshotSplitReader,大概的思路就是这里会根据不同的性质区分进行数据
     * 读取，在submitSplit的时候会创建readTask读取指定split的数据，结果会放入StatefulTaskContext的queue中，在fetch方法会
     * 先提交split，让其执行read数据，然后通过pollSplitRecords方法再调用queue.poll拉取数据，这是一个阻塞操作，如果超时则抛出
     * 中断异常。
     */
    @Nullable
    private DebeziumReader<SourceRecord, MySqlSplit> currentReader;
    @Nullable
    private String currentSplitId;

    public MySqlSplitReader(
            MySqlSourceConfig sourceConfig, int subtaskId, MySqlSourceReaderContext context) {
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.context = context;
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        //执行fetch的时候提前检查一下currentReader，并根据不同的split创建不同的对应的reader(binlog/snapshot)
        checkSplitOrStartNext();
        checkNeedStopBinlogReader();

        Iterator<SourceRecord> dataIt;
        try {
            /**
             * 调用具体的debeziumReader执行任务
             * 在reader中会调用StatefulTaskContaxt的queue的poll方法拉取数据，该方法会阻塞（也可以根据时间阻塞），
             * 如果时间间隔内没有返回数据则被中断，抛出InterruptedException
             */
            dataIt = currentReader.pollSplitRecords();
        } catch (InterruptedException e) {
            LOG.warn("fetch data failed.", e);
            throw new IOException(e);
        }
        return dataIt == null
                ? finishedSnapshotSplit() //如果没有读取到数据则返回一个空的，该方法执行后会将currentSplitId设置为null,表示该split已经执行完成
                : MySqlRecords.forRecords(currentSplitId, dataIt);
    }

    private void checkNeedStopBinlogReader() {
        if (currentReader instanceof BinlogSplitReader
                && context.needStopBinlogSplitReader()
                && !currentReader.isFinished()) {
            ((BinlogSplitReader) currentReader).stopBinlogReadTask();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySqlSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            LOG.info(
                    "Close current debezium reader {}",
                    currentReader.getClass().getCanonicalName());
            currentReader.close();
            currentSplitId = null;
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        if (canAssignNextSplit()) {
            MySqlSplit nextSplit = splits.poll();
            if (nextSplit == null) {
                return;
            }

            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currentReader instanceof BinlogSplitReader) {
                    LOG.info(
                            "This is the point from binlog split reading change to snapshot split reading");
                    currentReader.close();
                    currentReader = null;
                }
                if (currentReader == null) {
                    final MySqlConnection jdbcConnection = createMySqlConnection(sourceConfig);
                    final BinaryLogClient binaryLogClient =
                            createBinaryClient(sourceConfig.getDbzConfiguration());
                    final StatefulTaskContext statefulTaskContext =
                            new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
                    currentReader = new SnapshotSplitReader(statefulTaskContext, subtaskId);
                }
            } else {
                // point from snapshot split to binlog split
                if (currentReader != null) {
                    LOG.info("It's turn to read binlog split, close current snapshot reader");
                    currentReader.close();
                }
                final MySqlConnection jdbcConnection = createMySqlConnection(sourceConfig);
                final BinaryLogClient binaryLogClient =
                        createBinaryClient(sourceConfig.getDbzConfiguration());
                final StatefulTaskContext statefulTaskContext =
                        new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
                currentReader = new BinlogSplitReader(statefulTaskContext, subtaskId);
                LOG.info("BinlogSplitReader is created.");
            }
            //提交一个split到reader，reader会在submitSplit方法创建ReadTask对象，进行读取数据，将数据放入StatefulTaskContext的queue中，readTask放入线程池执行任务
            currentReader.submitSplit(nextSplit);
        }
    }

    private boolean canAssignNextSplit() {
        return currentReader == null || currentReader.isFinished();
    }

    private MySqlRecords finishedSnapshotSplit() {
        final MySqlRecords finishedRecords = MySqlRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
