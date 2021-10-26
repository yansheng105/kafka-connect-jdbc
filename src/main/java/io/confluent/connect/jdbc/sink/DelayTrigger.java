/*
 * Copyright 2021 创智和宇 Confluent Inc.
 *
 * @author yansheng
 * @date 2021/10/21 11:09
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author yansheng
 * @version 1.0
 * @Description
 * @date 2021/10/21 10:40
 * @Copyright http://www.powersi.com 2020 创智和宇
 * @since 1.0
 */
public class DelayTrigger {

  private final List<SinkRecord> buffer = new CopyOnWriteArrayList<>();
  private final int maxDelayBufferSize;
  private final ScheduledExecutorService pool;
  private final JdbcSinkTask jdbcSinkTask;

  public DelayTrigger(JdbcSinkTask jdbcSinkTask, int maxDelayTime, int maxDelayBufferSize) {
    this.jdbcSinkTask = jdbcSinkTask;
    this.maxDelayBufferSize = maxDelayBufferSize;
    pool = Executors.newScheduledThreadPool(1);
    pool.scheduleWithFixedDelay(this::trigger, 0, maxDelayTime, TimeUnit.MILLISECONDS);
  }

  public void put(Collection<SinkRecord> records) {
    buffer.addAll(records);
    if (buffer.size() >= maxDelayBufferSize) {
      trigger();
    }
  }

  public void trigger() {
    if (buffer.isEmpty()) {
      return;
    }
    jdbcSinkTask.doWrite(buffer);
    buffer.clear();
  }

  public void close() {
    if (null != pool && !pool.isShutdown()) {
      pool.shutdownNow();
    }
  }
}
