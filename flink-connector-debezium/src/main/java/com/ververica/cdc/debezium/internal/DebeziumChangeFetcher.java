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

package com.ververica.cdc.debezium.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A Handler that convert change messages from {@link DebeziumEngine} to data in Flink. Considering
 * Debezium in different mode has different strategies to hold the lock, e.g. snapshot, the handler
 * also needs different strategy. In snapshot phase, the handler needs to hold the lock until the
 * snapshot finishes. But in non-snapshot phase, the handler only needs to hold the lock when
 * emitting the records.
 *
 * @param <T> The type of elements produced by the handler.
 *
 * 循环从handover中获取consumer从engine读取的最新数据
 */
@Internal
public class DebeziumChangeFetcher<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumChangeFetcher.class);

    private final SourceFunction.SourceContext<T> sourceContext;

    /**
     * The lock that guarantees that record emission and state updates are atomic, from the view of
     * taking a checkpoint.
     */
    private final Object checkpointLock; //保证数据发送和状态更新的一把锁

    /** The schema to convert from Debezium's messages into Flink's objects. */
    private final DebeziumDeserializationSchema<T> deserialization;

    /** A collector to emit records in batch (bundle). */
    private final DebeziumCollector debeziumCollector; //自定义的collector

    private final DebeziumOffset debeziumOffset;

    /* 用于存储在stateOffset的序列化器 */
    private final DebeziumOffsetSerializer stateSerializer;

    /* 心跳相关 */
    private final String heartbeatTopicPrefix;

    /* 是否恢复的状态，需要消费历史相关数据，同时也用作是否从binlog进行流式消费的标识*/
    private boolean isInDbSnapshotPhase;

    private final Handover handover;

    private volatile boolean isRunning = true;

    // ---------------------------------------------------------------------------------------
    // Metrics
    // ---------------------------------------------------------------------------------------

    /** Timestamp of change event. If the event is a snapshot event, the timestamp is 0L. */
    private volatile long messageTimestamp = 0L;

    /** The last record processing time. */
    private volatile long processTime = 0L;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /**
     * emitDelay = EmitTime - messageTimestamp, where the EmitTime is the time the record leaves the
     * source operator.
     */
    private volatile long emitDelay = 0L;

    // ------------------------------------------------------------------------

    public DebeziumChangeFetcher(
            SourceFunction.SourceContext<T> sourceContext,
            DebeziumDeserializationSchema<T> deserialization,
            boolean isInDbSnapshotPhase,
            String heartbeatTopicPrefix,
            Handover handover) {
        this.sourceContext = sourceContext;
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.deserialization = deserialization;
        this.isInDbSnapshotPhase = isInDbSnapshotPhase;
        this.heartbeatTopicPrefix = heartbeatTopicPrefix;
        this.debeziumCollector = new DebeziumCollector();
        this.debeziumOffset = new DebeziumOffset();
        this.stateSerializer = DebeziumOffsetSerializer.INSTANCE;
        this.handover = handover;
    }

    /**
     * Take a snapshot of the Debezium handler state.
     *
     * <p>Important: This method must be called under the checkpoint lock.
     */
    public byte[] snapshotCurrentState() throws Exception {
        // this method assumes that the checkpoint lock is held
        assert Thread.holdsLock(checkpointLock);
        if (debeziumOffset.sourceOffset == null || debeziumOffset.sourcePartition == null) {
            return null;
        }

        return stateSerializer.serialize(debeziumOffset);
    }

    /**
     * Process change messages from the {@link Handover} and collect the processed messages by
     * {@link Collector}.
     *
     * 循环拿取数据并发送到下游
     */
    public void runFetchLoop() throws Exception {
        try {
            // begin snapshot database phase
            if (isInDbSnapshotPhase) { //读取mysql历史的数据，也就是snapshot数据
                List<ChangeEvent<SourceRecord, SourceRecord>> events = handover.pollNext();

                synchronized (checkpointLock) {
                    LOG.info(
                            "Database snapshot phase can't perform checkpoint, acquired Checkpoint lock.");
                    handleBatch(events);
                    //这里防止snapshot数据无法一次读取完毕，必须保证snapshot数据读取完毕才进入binlog读取
                    while (isRunning && isInDbSnapshotPhase) {
                        handleBatch(handover.pollNext());
                    }
                }
                LOG.info("Received record from streaming binlog phase, released checkpoint lock.");
            }

            // begin streaming binlog phase 到这里表示snapshot的数据已经读取完毕，开始实时读取binlog数据
            while (isRunning) {
                // If the handover is closed or has errors, exit.
                // If there is no streaming phase, the handover will be closed by the engine.
                handleBatch(handover.pollNext()); //具体的数据处理逻辑，pollNext会阻塞
            }
        } catch (Handover.ClosedException e) {
            // ignore
        } catch (RetriableException e) {
            // Retriable exception should be ignored by DebeziumChangeFetcher,
            // refer https://issues.redhat.com/browse/DBZ-2531 for more information.
            // Because Retriable exception is ignored by the DebeziumEngine and
            // the retry is handled in io.debezium.connector.common.BaseSourceTask.poll()
            LOG.info(
                    "Ignore the RetriableException, the underlying DebeziumEngine will restart automatically",
                    e);
        }
    }

    public void close() {
        isRunning = false;
        handover.close();
    }

    // ---------------------------------------------------------------------------------------
    // Metric getter
    // ---------------------------------------------------------------------------------------

    /**
     * The metric indicates delay from data generation to entry into the system.
     *
     * <p>Note: the metric is available during the binlog phase. Use 0 to indicate the metric is
     * unavailable.
     */
    public long getFetchDelay() {
        return fetchDelay;
    }

    /**
     * The metric indicates delay from data generation to leaving the source operator.
     *
     * <p>Note: the metric is available during the binlog phase. Use 0 to indicate the metric is
     * unavailable.
     */
    public long getEmitDelay() {
        return emitDelay;
    }

    public long getIdleTime() {
        return System.currentTimeMillis() - processTime;
    }

    // ---------------------------------------------------------------------------------------
    // Helper
    // ---------------------------------------------------------------------------------------

    private void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> changeEvents)
            throws Exception {
        if (CollectionUtils.isEmpty(changeEvents)) {
            return;
        }
        this.processTime = System.currentTimeMillis();

        for (ChangeEvent<SourceRecord, SourceRecord> event : changeEvents) {
            SourceRecord record = event.value();
            updateMessageTimestamp(record);
            fetchDelay = isInDbSnapshotPhase ? 0L : processTime - messageTimestamp;

            // 通过心跳机制来更新offset
            if (isHeartbeatEvent(record)) {
                // keep offset update
                synchronized (checkpointLock) {
                    debeziumOffset.setSourcePartition(record.sourcePartition());
                    debeziumOffset.setSourceOffset(record.sourceOffset());
                }
                // drop heartbeat events
                continue;
            }

            /**
             * 根据不同的deserialization对数据做转换
             * eg:StringDebeziumDeserializationSchema,内部直接record.toString即可
             * 就是将debezium读取的record转换成我们想要的格式或者类型，debeziumCollector就是自定义的collector,
             * 在deserialize方法中会将转换完成的数据放入debeziumCollector中：out.collect(record.toString())
             */
            deserialization.deserialize(record, debeziumCollector);

            if (!isSnapshotRecord(record)) { //判断数据是否为snapshot的最后一条数据，如果是则在这条数据之后转换到binlog的Streaming流程
                LOG.debug("Snapshot phase finishes.");
                isInDbSnapshotPhase = false; // runFetchLoop方法中使用
            }

            // emit the actual records. this also updates offset state atomically
            emitRecordsUnderCheckpointLock( //具体发送数据的地方
                    debeziumCollector.records, record.sourcePartition(), record.sourceOffset());
        }
    }

    private void emitRecordsUnderCheckpointLock(
            Queue<T> records, Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) {
        // Emit the records. Use the checkpoint lock to guarantee
        // atomicity of record emission and offset state update.
        // The synchronized checkpointLock is reentrant. It's safe to sync again in snapshot mode.
        synchronized (checkpointLock) { //同步是保证数据的发送和Offset的更新是安全的，lock是可重入的
            T record;
            while ((record = records.poll()) != null) {
                emitDelay =
                        isInDbSnapshotPhase ? 0L : System.currentTimeMillis() - messageTimestamp;
                sourceContext.collect(record); //通过source的context对象将其发送到下游operator,这里传入了flink的处理逻辑，不在cdc代码之内
            }
            // update offset to state
            debeziumOffset.setSourcePartition(sourcePartition);
            debeziumOffset.setSourceOffset(sourceOffset);
        }
    }

    private void updateMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.SOURCE) == null) {
            return;
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source.schema().field(Envelope.FieldName.TIMESTAMP) == null) {
            return;
        }

        Long tsMs = source.getInt64(Envelope.FieldName.TIMESTAMP);
        if (tsMs != null) {
            this.messageTimestamp = tsMs;
        }
    }

    private boolean isHeartbeatEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null && topic.startsWith(heartbeatTopicPrefix);
    }

    private boolean isSnapshotRecord(SourceRecord record) {
        Struct value = (Struct) record.value();
        if (value != null) {
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            SnapshotRecord snapshotRecord = SnapshotRecord.fromSource(source);
            // even if it is the last record of snapshot, i.e. SnapshotRecord.LAST
            // we can still recover from checkpoint and continue to read the binlog,
            // because the checkpoint contains binlog position
            return SnapshotRecord.TRUE == snapshotRecord;
        }
        return false;
    }

    // ---------------------------------------------------------------------------------------

    //自定义collector
    private class DebeziumCollector implements Collector<T> {

        private final Queue<T> records = new ArrayDeque<>();

        @Override
        public void collect(T record) {
            records.add(record); //将数据放入队列，queue会在别的地方将数据发送到下游
        }

        @Override
        public void close() {}
    }
}
