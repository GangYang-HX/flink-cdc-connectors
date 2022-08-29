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

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.MySqlValidator;
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReaderContext;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Supplier;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.discoverCapturedTables;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.openJdbcConnection;

/**
 * The MySQL CDC Source based on FLIP-27 and Watermark Signal Algorithm which supports parallel
 * reading snapshot of table and then continue to capture data change from binlog.
 *
 * <pre>
 *     1. The source supports parallel capturing table change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source doesn't need apply any lock of MySQL.
 * </pre>
 *
 * <pre>{@code
 * MySqlSource
 *     .<String>builder()
 *     .hostname("localhost")
 *     .port(3306)
 *     .databaseList("mydb")
 *     .tableList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .serverId(5400)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link MySqlSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 *
 * snapshot : 表示的是读取数据库的历史全量数据
 * binlog : 表示当我们snapshot阶段结束后开始binlog阶段，即我们开始读取的binlog数据
 * 也就是先执行snapshot阶段，后执行binlog阶段
 *
 * MysqlSource两个接口：Source 和 ResultTypeQueryable
 *           - 主要逻辑是在source接口的实现
 *
 * Source<T, MySqlSplit, PendingSplitsState>：T为输出类型，MySqlSplit是mysql的分割器，PendingSplitsState表示Enumerator的状态对象
 */
@Internal
public class MySqlSource<T>
        implements Source<T, MySqlSplit, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final MySqlSourceConfigFactory configFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;

    /**
     * Get a MySqlParallelSourceBuilder to build a {@link MySqlSource}.
     *
     * @return a MySql parallel source builder.
     *
     * 通过构造者模式构建source所需要的参数：return new MySqlSource<>(configFactory, checkNotNull(deserializer));
     *  private final MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
     *  private DebeziumDeserializationSchema<T> deserializer;
     */
    @PublicEvolving
    public static <T> MySqlSourceBuilder<T> builder() {
        return new MySqlSourceBuilder<>();
    }

    //由MySqlSourceBuilder.build()方法创建
    MySqlSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
    }

    /**
     * MysqlSourceConfigFactory 可以根据不同的subtask创建对应的MySqlSourceConfig。然后
     * MySqlSourceConfig可以构建MysqlConnectConfig。最后MysqlConnection通过DebeziumUtil.createMysqlConnection(mysqlSource.getDbzConfigration())方法创建
     * @return
     */
    public MySqlSourceConfigFactory getConfigFactory() {
        return configFactory;
    }

    /**
     * 流批一体的source，表示有界性，新source接口的特性
     * @return
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * 构建SourceReader
     * SourceReader:对split的数据进行读取操作。
     * 比如：读取一个分区、一个块等，当然不只局限于此，也可以自定义实现
     *
     * 注意：一个split我们可以认为是一个切片，在mysql-cdc中，假想情况如下：一张表的一部分，比如开始主键1到结束主键10，那么该split就表示这些数据，
     * 在具体读取数据的时候是有readTask来去读，那么他就会通过split标记的点位（主键1～10）来进行数据的读取，一个readTask可以读取多个split。
     *
     * @param readerContext
     * @return
     * @throws Exception
     */
    @Override
    public SourceReader<T, MySqlSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id) 根据subtask索引创建对应的config
        MySqlSourceConfig sourceConfig =
                configFactory.createConfig(readerContext.getIndexOfSubtask());

        // 一个阻塞队列，多线程交互用的
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);

        final MySqlSourceReaderMetrics sourceReaderMetrics =
                new MySqlSourceReaderMetrics(metricGroup);
        sourceReaderMetrics.registerMetrics();
        MySqlSourceReaderContext mySqlSourceReaderContext =
                new MySqlSourceReaderContext(readerContext);

        /**
         * 通过Supplier函数构架一个SplitReader。解耦的作用，主要看里面的MySqlSplitReader实现即可
         */
        Supplier<MySqlSplitReader> splitReaderSupplier =
                () ->   //拿到每个reader的config和对应的subtask index
                        new MySqlSplitReader(
                                sourceConfig,
                                readerContext.getIndexOfSubtask(),
                                mySqlSourceReaderContext);

        //构建一个具体的SourceReader
        return new MySqlSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new MySqlRecordEmitter<>(
                        deserializationSchema,
                        sourceReaderMetrics,
                        sourceConfig.isIncludeSchemaChanges()),
                readerContext.getConfiguration(),
                mySqlSourceReaderContext,
                sourceConfig);
    }

    /**
     * SplitEnumerator:负责对数据源进行切分或者发现分区等，比如：发现Kafka的分区，对文件划分块等
     * @param enumContext
     * @return
     */
    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext) {

        //因为只会生成一次，所以随意生成一个SourceConfig即可
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);

        //校验mysql配置
        final MySqlValidator validator = new MySqlValidator(sourceConfig);
        validator.validate();

        final MySqlSplitAssigner splitAssigner;

        //判断开始条件如果是initial则先读取mysql table的数据（代码中叫做snapshot）,然后再继续读取binlog的数据。如果不是initial状态，则直接从binlog开始读取
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
            try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                final List<TableId> remainingTables = discoverCapturedTables(jdbc, sourceConfig);
                boolean isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
                splitAssigner =
                        new MySqlHybridSplitAssigner( //里面包含snapshot和binlog的split逻辑
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            //直接声明binlog的split逻辑
            splitAssigner = new MySqlBinlogSplitAssigner(sourceConfig);
        }

        //创建对应的SplitEnumerator，用于构建split给reader读取
        return new MySqlSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    /**
     * 恢复SplitEnumerator，比如任务故障重启，会根据不同的checkpoint恢复SplitEnumerator用于继续之前未完成的读取操作
     * @param enumContext
     * @param checkpoint
     * @return
     */
    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext, PendingSplitsState checkpoint) {
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);

        final MySqlSplitAssigner splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new MySqlHybridSplitAssigner(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint);
        } else if (checkpoint instanceof BinlogPendingSplitsState) {
            splitAssigner =
                    new MySqlBinlogSplitAssigner(
                            sourceConfig, (BinlogPendingSplitsState) checkpoint);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new MySqlSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return MySqlSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsStateSerializer(getSplitSerializer());
    }

    //返回值类型的提取
    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
