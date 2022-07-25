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

import io.confluent.connect.jdbc.util.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;
  private final Map<String,String> tableNameMapping;
  private final static ConcurrentHashMap<String, CopyOnWriteArrayList<SinkRecord>> special = new ConcurrentHashMap<>();

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
    this.tableNameMapping = destinationTableMapping(config.tableNameMapping);

  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      if (config.batchEnabled) {
        final Map<TableId, BatchBufferedRecords> bufferByTable = new HashMap<>();
        for (SinkRecord record : records) {
          final TableId tableId = destinationTable(record);
          BatchBufferedRecords buffer = bufferByTable.get(tableId);
          if (buffer == null) {
            buffer = new BatchBufferedRecords(config, tableId, dbDialect, dbStructure, connection);
            bufferByTable.put(tableId, buffer);
          }
          buffer.add(record);
        }
        for (Map.Entry<TableId, BatchBufferedRecords> entry : bufferByTable.entrySet()) {
          TableId tableId = entry.getKey();
          BatchBufferedRecords buffer = entry.getValue();
          log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
          buffer.flush();
          buffer.close();
        }
      } else {
        final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
        for (SinkRecord record : records) {
          final TableId tableId = destinationTable(record);
          BufferedRecords buffer = bufferByTable.get(tableId);
          if (buffer == null) {
            buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
            bufferByTable.put(tableId, buffer);
          }
          buffer.add(record);
        }
        for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
          TableId tableId = entry.getKey();
          BufferedRecords buffer = entry.getValue();
          log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
          buffer.flush();
          buffer.close();
        }
      }
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      }
      throw e;
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(SinkRecord record) {
    if (!StringUtils.isEmpty(config.tableNameField)) {
      String tableName = ((Struct) record.value()).getString(config.tableNameField);
      if (!StringUtils.isEmpty(config.tableSchema)) {
        tableName = config.tableSchema + "." + tableName;
      }
      return dbDialect.parseTableIdentifier(tableName);
    }
    String topic = record.topic();
    if (tableNameMapping.isEmpty()) {
      final String tableName = config.tableNameFormat.replace("${topic}", topic);
      if (tableName.isEmpty()) {
        throw new ConnectException(String.format(
                "Destination table name for topic '%s' is empty using the format string '%s'",
                topic,
                config.tableNameFormat
        ));
      }
      return dbDialect.parseTableIdentifier(tableName);
    } else {
      if (tableNameMapping.containsKey(topic)) {
        return dbDialect.parseTableIdentifier(tableNameMapping.get(topic));
      } else {
        throw new ConnectException(String.format(
                "Destination table name for topic '%s' is empty using the mapping string '%s'",
                topic,
                config.tableNameMapping
        ));
      }
    }
  }

  private Map<String, String> destinationTableMapping(String tableNameMapping) {
    try {
      Map<String, String> map = new HashMap<>();
      if (tableNameMapping.length() > 0) {
        for (String tbNameMapping : tableNameMapping.split(";")) {
          String[] mapping = tbNameMapping.split(":");
          for (String key : mapping[1].split(",")) {
            map.put(key.trim(), mapping[0]);
          }
        }
      }
      return map;
    } catch (Exception e) {
      throw new ConnectException("The target table name is incorrectly mapped. The format of "
              + "the mapping string defined by the attribute `table.name.mapping` is incorrect. "
              + "The correct format is: table1:topic1,topic2;table2:topic3,topic4");
    }
  }

  public static void putSpecial(String key, SinkRecord record) {
    if (special.containsKey(key)) {
      special.computeIfPresent(key, (k, sinkRecords) -> {
        sinkRecords.add(record);
        return sinkRecords;
      });
    } else {
      special.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>(Collections.singletonList(record)));
    }
  }

  public static CopyOnWriteArrayList<SinkRecord> getSpecial(String key) {
    return special.get(key);
  }

}
