/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Savepoint;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Map;
import java.util.Objects;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;


import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.UPDATE;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.DELETE2INSERT;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.DELETE_AND_INSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * @author yansheng
 * @version 1.0
 * @Description
 * @date 2021/10/18 10:34
 * @Copyright http://www.powersi.com 2020 创智和宇
 * @since 1.0
 */
public class BatchBufferedRecords {

  /**
   * 事件类型枚举
   *
   * @author yansheng
   * @date 2021/10/20 16:52
   */
  public enum EventType {
    INSERT,
    UPDATE,
    DELETE,
    QUERY,
    OTHER;

  }

  private static final Logger log = LoggerFactory.getLogger(BatchBufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;
  private boolean closeOptimizer = true;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private RecordValidator recordValidator;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement insertPreparedStatement;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private StatementBinder insertStatementBinder;
  private StatementBinder updateStatementBinder;
  private StatementBinder deleteStatementBinder;
  private boolean deletesInBatch = false;

  public BatchBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.recordValidator = RecordValidator.create(config);
  }

  public void add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
    recordValidator.validate(record);

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (isNull(record.valueSchema())) {
      // For deletes, value and optionally value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        deletesInBatch = true;
      }
    } else if (Objects.equals(valueSchema, record.valueSchema())) {
      if (config.deleteEnabled && deletesInBatch) {
        // flush so an insert after a delete of same record isn't lost
        flush();
      }
    } else {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flush();

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          record.keySchema(),
          record.valueSchema()
      );
      fieldsMetadata = FieldsMetadata.extract(
          tableId.tableName(),
          config.pkMode,
          config.pkFields,
          config.fieldsWhitelist,
          config.fieldsBlacklist,
          schemaPair
      );
      dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata
      );
    }

    // set deletesInBatch if schema value is not null
    if (isNull(record.value()) && config.deleteEnabled) {
      deletesInBatch = true;
    }

    records.add(record);

    if (records.size() >= config.batchSize) {
      flush();
    }
  }

  public void flush() throws SQLException, AssertionError, ConnectException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return;
    }
    // log.info("Flushing {} buffered records", records.size());
    // 先按主键分组，此时主键相同的消息均在一个list中
    Map<String, List<SinkRecord>> groupByPrimaryKey = records.stream()
        .filter(x -> {
          EventType eventType = getEventType(x);
          switch (eventType) {
            case INSERT:
            case UPDATE:
            case DELETE:
              return true;
            default:
              break;
          }
          return false;
        }).collect(Collectors.groupingBy(this::getPrimaryKeyValues));
    List<ConcurrentLinkedQueue<SinkRecord>> collect = groupByPrimaryKey.values().stream()
        .map((Function<List<SinkRecord>, ConcurrentLinkedQueue<SinkRecord>>)
            ConcurrentLinkedQueue::new)
        .collect(Collectors.toList());
    while (true) {
      List<SinkRecord> recordsList = new ArrayList<>();
      // 遍历所有不同主键的消息列表，取出第一条消息
      for (ConcurrentLinkedQueue<SinkRecord> sinkRecords : collect) {
        SinkRecord poll = sinkRecords.poll();
        if (null != poll) {
          recordsList.add(poll);
        }
      }
      if (recordsList.isEmpty()) {
        break;
      }
      // 将不同主键的消息按事件类型分组
      Map<EventType, List<SinkRecord>> groupByEventType = recordsList.stream()
          .collect(Collectors.groupingBy(this::getEventType));

      // 只需保证相同主键的事件顺序性即可保证数据一致性
      // 所有相同主键的事件均会分配到同一个分区，且每个任务消费一个分区，因此不会造成数据错乱

      List<SinkRecord> insert = groupByEventType.getOrDefault(EventType.INSERT, new ArrayList<>());
      List<SinkRecord> update = groupByEventType.getOrDefault(EventType.UPDATE, new ArrayList<>());
      List<SinkRecord> delete = groupByEventType.getOrDefault(EventType.DELETE, new ArrayList<>());

      // 将update转换为delete + insert，以实现upsert的效果
      if (config.insertMode == DELETE_AND_INSERT) {
        if (!update.isEmpty()) {
          update.forEach(rcd -> {
            delete.add(new SinkRecord(
                rcd.topic(),
                rcd.kafkaPartition(),
                rcd.keySchema(),
                rcd.key(),
                null,
                null,
                rcd.kafkaOffset(),
                rcd.timestamp(),
                rcd.timestampType(),
                rcd.headers()
            ));
            insert.add(rcd);
          });
        }
      } else {
        // 批量更新
        if (!update.isEmpty()) {
          batchUpdate(update);
        }
      }
      // 批量删除
      if (!delete.isEmpty()) {
        batchDelete(delete);
      }
      // 批量插入
      if (!insert.isEmpty()) {
        batchInsert(insert);
      }
      connection.commit();
    }
    // 关闭资源
    close();
    deletesInBatch = false;
    records = new ArrayList<>();
  }

  public void batchInsert(List<SinkRecord> sinkRecords) throws SQLException {
    boolean isPostgreSql = dbDialect.name().equalsIgnoreCase("PostgreSql");
    String insertSql = dbDialect.buildInsertStatement(
        tableId,
        asColumns(fieldsMetadata.keyFieldNames),
        asColumns(fieldsMetadata.nonKeyFieldNames),
        dbStructure.tableDefinition(connection, tableId)
    );

    int size = sinkRecords.size() - 1;
    String values = insertSql.substring(insertSql.indexOf("VALUES") + 6);
    StringBuilder batchInsert = new StringBuilder(insertSql);
    for (int i = 0; i < size; i++) {
      batchInsert.append(",").append(values);
    }
    SchemaPair schemaPair = new SchemaPair(
        sinkRecords.get(0).keySchema(),
        sinkRecords.get(0).valueSchema()
    );
    insertPreparedStatement = dbDialect.createPreparedStatement(connection, batchInsert.toString());
    insertStatementBinder = dbDialect.statementBinder(
        insertPreparedStatement,
        config.pkMode,
        schemaPair,
        fieldsMetadata,
        dbStructure.tableDefinition(connection, tableId),
        config.insertMode
    );

    insertStatementBinder.bindRecords(sinkRecords);
    Savepoint savepoint = null;
    try {
      if (isPostgreSql) {
        savepoint = connection.setSavepoint();
      }
      long start = System.currentTimeMillis();
      Optional<Long> totalInsertCount = executeUpdates(insertPreparedStatement);
      if (totalInsertCount.isPresent()) {
        long cost = System.currentTimeMillis() - start;
        if (cost > 500) {
          log.warn("{} records:{} resulting in totalInsertCount:{} cost {}ms",
              config.insertMode, sinkRecords.size(), totalInsertCount.get(), cost
          );
        }
        if (totalInsertCount.filter(total -> total != sinkRecords.size()).isPresent()
            && config.insertMode == INSERT) {
          throw new ConnectException(String.format(
              "Update count (%d) did not sum up to total number of records inserted (%d)",
              totalInsertCount.get(),
              sinkRecords.size()
          ));
        }
      } else {
        log.info(
            "{} records:{} , but no count of the number of rows it affected is available",
            config.insertMode,
            records.size()
        );
      }
    } catch (SQLException e) {
      // TODO 添加更多主键冲突错误信息
      if (e.getMessage().contains("duplicate key")
          || e.getMessage().contains("Duplicate entry")
          || e.getMessage().startsWith("ORA-00001:")) {
        // postgresql需要回滚
        if (isPostgreSql) {
          connection.rollback(savepoint);
        }
        connection.setAutoCommit(false);
        insertPreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
        for (SinkRecord sinkRecord : sinkRecords) {
          insertStatementBinder = dbDialect.statementBinder(
              insertPreparedStatement,
              config.pkMode,
              schemaPair,
              fieldsMetadata,
              dbStructure.tableDefinition(connection, tableId),
              config.insertMode
          );
          insertStatementBinder.bindRecord(sinkRecord);
          if (isPostgreSql) {
            savepoint = connection.setSavepoint();
          }
          try {
            insertPreparedStatement.executeUpdate();
          } catch (SQLException ex) {
            if (ex.getMessage().contains("duplicate key")
                || ex.getMessage().contains("Duplicate entry")
                || ex.getMessage().startsWith("ORA-00001:")) {
              if (isPostgreSql && null != savepoint) {
                connection.rollback(savepoint);
              }
            } else {
              log.error("批量插入失败后尝试逐条执行时发生异常：", ex);
              throw ex;
            }
          }
        }
        connection.commit();
      } else {
        log.error("执行批量插入异常：", e);
        throw e;
      }
    }
  }

  public void batchUpdate(List<SinkRecord> sinkRecords) throws SQLException {
    boolean isBatch = true;
    String updateSql;
    try {
      // 目前仅mysql、postgresql支持批量更新SQL
      updateSql = dbDialect.buildBatchUpdateStatement(
          tableId,
          asColumns(fieldsMetadata.keyFieldNames),
          asColumns(fieldsMetadata.nonKeyFieldNames),
          dbStructure.tableDefinition(connection, tableId),
          sinkRecords.size()
      );
    } catch (Exception e) {
      isBatch = false;
      updateSql = dbDialect.buildUpdateStatement(
          tableId,
          asColumns(fieldsMetadata.keyFieldNames),
          asColumns(fieldsMetadata.nonKeyFieldNames),
          dbStructure.tableDefinition(connection, tableId)
      );
    }

    updatePreparedStatement = dbDialect.createPreparedStatement(connection, updateSql);
    updateStatementBinder = dbDialect.statementBinder(
        updatePreparedStatement,
        config.pkMode,
        new SchemaPair(
            sinkRecords.get(0).keySchema(),
            sinkRecords.get(0).valueSchema()
        ),
        fieldsMetadata,
        dbStructure.tableDefinition(connection, tableId),
        config.insertMode
    );

    if (isBatch) {
      if (dbDialect.name().equalsIgnoreCase("PostgreSql")) {
        // 只执行一次
        if (closeOptimizer) {
          // 关闭查询优化器，防止出现 Error "Multiple updates to a row by the same query is not allowed"
          try (Statement statement = connection.createStatement()) {
            Savepoint savepoint = connection.setSavepoint();
            try {
              statement.executeUpdate("set optimizer=off");
              closeOptimizer = false;
            } catch (SQLException throwables) {
              // optimizer 为greenplum的特殊参数，postgresql不存在，因此此处捕获异常不抛出
              connection.rollback(savepoint);
            }
          }
        }
      }
      updateStatementBinder.bindRecords(sinkRecords);
    } else {
      for (SinkRecord sinkRecord : sinkRecords) {
        updateStatementBinder.bindRecord(sinkRecord, UPDATE);
      }
    }

    long start = System.currentTimeMillis();
    Optional<Long> totalUpdateCount = executeUpdates(updatePreparedStatement);
    if (totalUpdateCount.isPresent()) {
      long cost = System.currentTimeMillis() - start;
      if (cost > 500) {
        log.warn("{} records:{} resulting in totalUpdateCount:{} cost {}ms",
            config.insertMode, sinkRecords.size(), totalUpdateCount.get(), cost
        );
      }
    }
    if (totalUpdateCount.filter(total -> total != sinkRecords.size()).isPresent()
        && config.insertMode == INSERT) {
      throw new ConnectException(String.format(
          "Update count (%d) did not sum up to total number of records inserted (%d)",
          totalUpdateCount.get(),
          sinkRecords.size()
      ));
    }
    if (!totalUpdateCount.isPresent()) {
      log.info(
          "{} records:{} , but no count of the number of rows it affected is available",
          config.insertMode,
          records.size()
      );
    }
  }

  public void batchDelete(List<SinkRecord> sinkRecords) throws SQLException {
    boolean isBatch = true;
    String deleteSql;
    try {
      deleteSql = dbDialect.buildBatchDeleteStatement(
          tableId,
          asColumns(fieldsMetadata.keyFieldNames),
          asColumns(fieldsMetadata.nonKeyFieldNames),
          dbStructure.tableDefinition(connection, tableId),
          sinkRecords.size()
      );
    } catch (Exception e) {
      isBatch = false;
      deleteSql = getDeleteSql();
    }

    deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
    deleteStatementBinder = dbDialect.statementBinder(
        deletePreparedStatement,
        config.pkMode,
        new SchemaPair(
            sinkRecords.get(0).keySchema(),
            sinkRecords.get(0).valueSchema()
        ),
        fieldsMetadata,
        dbStructure.tableDefinition(connection, tableId),
        config.insertMode
    );

    if (isBatch) {
      deleteStatementBinder.bindRecords(sinkRecords);
    } else {
      for (SinkRecord sinkRecord : sinkRecords) {
        deleteStatementBinder.bindRecord(sinkRecord);
      }
    }

    long start = System.currentTimeMillis();
    long totalDeleteCount = executeDeletes();
    long cost = System.currentTimeMillis() - start;
    if (cost > 500) {
      log.warn("{} records:{} resulting in  totalDeleteCount:{} cost {}ms",
          config.insertMode, sinkRecords.size(), totalDeleteCount, cost
      );
    }
  }

  private String getDeleteSql() throws SQLException {
    String sql = null;
    if (config.deleteEnabled) {
      if (config.pkMode == JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY) {
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException("Require primary keys to support delete");
        }
        try {
          sql = dbDialect.buildDeleteStatement(
              tableId,
              asColumns(fieldsMetadata.keyFieldNames),
              dbStructure.tableDefinition(connection, tableId)
          );
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Deletes to table '%s' are not supported with the %s dialect.",
              tableId,
              dbDialect.name()
          ));
        }
      } else {
        throw new ConnectException("Deletes are only supported for pk.mode record_key");
      }
    }
    return sql;
  }

  private Optional<Long> executeUpdates(PreparedStatement preparedStatement) throws SQLException {
    Optional<Long> count = Optional.empty();
    for (int updateCount : preparedStatement.executeBatch()) {
      if (updateCount != Statement.SUCCESS_NO_INFO) {
        count = count.isPresent()
            ? count.map(total -> total + updateCount)
            : Optional.of((long) updateCount);
      }
    }
    return count;
  }

  private long executeDeletes() throws SQLException {
    long totalDeleteCount = 0;
    if (nonNull(deletePreparedStatement)) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }
    return totalDeleteCount;
  }

  public void close() throws SQLException {
    if (nonNull(insertPreparedStatement)) {
      insertPreparedStatement.close();
      insertPreparedStatement = null;
    }
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }

  private String getPrimaryKeyValues(SinkRecord record) {
    SchemaPair schemaPair = new SchemaPair(
        record.keySchema(),
        record.valueSchema()
    );
    switch (config.pkMode) {
      case NONE:
        if (!fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new AssertionError();
        }
        break;

      case KAFKA: {
        assert fieldsMetadata.keyFieldNames.size() == 3;
        return record.topic() + "_" + record.kafkaPartition() + "_" + record.kafkaOffset();
      }

      case RECORD_KEY: {
        if (schemaPair.keySchema.type().isPrimitive()) {
          assert fieldsMetadata.keyFieldNames.size() == 1;
          return record.key().toString();
        } else {
          try {
            return fieldsMetadata.keyFieldNames.stream()
                .map(x -> ((Struct) record.key()).get(schemaPair.keySchema.field(x)).toString())
                .collect(Collectors.joining("_"));
          } catch (Exception e) {
            String fields = schemaPair.keySchema.fields().stream().map(Field::name)
                .collect(Collectors.joining(","));
            log.error("获取主键字段的值异常：record.key():[{}], fields:[{}], record:{}",
                record.key(), fields, record);
            throw e;
          }
        }
      }

      case RECORD_VALUE: {
        return fieldsMetadata.keyFieldNames.stream()
            .map(x -> ((Struct) record.value()).get(schemaPair.valueSchema.field(x)).toString())
            .collect(Collectors.joining("_"));
      }

      default:
        throw new ConnectException("Unknown primary key mode: " + config.pkMode);
    }
    return null;
  }

  private EventType getEventType(SinkRecord record) {
    if (isNull(record.value())) {
      if (config.insertMode == DELETE2INSERT) {
        return EventType.INSERT;
      }
      return EventType.DELETE;
    } else {
      String eventType = ((Struct) record.value()).getString(config.eventTypeField);
      if (eventType.equalsIgnoreCase("c")
          || eventType.equalsIgnoreCase("i")
          || eventType.equalsIgnoreCase("insert")) {
        return EventType.INSERT;
      } else if (eventType.equalsIgnoreCase("u")
          || eventType.equalsIgnoreCase("update")) {
        return EventType.UPDATE;
      } else if (eventType.equalsIgnoreCase("d")
          || eventType.equalsIgnoreCase("delete")) {
        if (config.insertMode == DELETE2INSERT) {
          return EventType.INSERT;
        }
        return EventType.DELETE;
      } else {
        return EventType.OTHER;
      }
    }
  }

}
