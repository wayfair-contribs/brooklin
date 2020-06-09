/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;

import static com.linkedin.datastream.server.FlushlessEventProducerHandler.SourcePartition;


/**
 * Tests for {@link FlushlessEventProducerHandler}
 */
public class TestFlushlessEventProducerHandler {
  private static final Long BIG_CHECKPOINT = Long.MAX_VALUE;
  private static final String TOPIC = "MyTopic";
  private static final Random RANDOM = new Random();

  @Test
  public void testSingleRecord() throws Exception {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler = new FlushlessEventProducerHandler<>(eventProducer);

    long checkpoint = 1;
    DatastreamProducerRecord record = getDatastreamProducerRecord(checkpoint, TOPIC, 1);

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);
    handler.send(record, TOPIC, 1, checkpoint, null);
    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()), Optional.empty());
    Assert.assertEquals(handler.getInFlightCount(TOPIC, 1), 1);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, 1), Optional.empty());
    eventProducer.flush();
    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);
    Assert.assertEquals(handler.getInFlightCount(TOPIC, 1), 0);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, 1).get(), new Long(checkpoint));
  }

  @Test
  public void testMultipleSends() throws Exception {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler = new FlushlessEventProducerHandler<>(eventProducer);

    // Send 1000 messages to 100 partitions
    for (int i = 0; i < 10; i++) {
      SourcePartition tp = new SourcePartition(TOPIC, i);
      for (int j = 0; j < 100; j++) {
        sendEvent(tp, handler, j);
      }
    }

    for (int i = 0; i < 999; i++) {
      eventProducer.processOne();
      long minOffsetPending = eventProducer.minCheckpoint();
      Long ackOffset = handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).orElse(-1L);

      Assert.assertTrue(ackOffset < minOffsetPending,
          "Not true that " + ackOffset + " is less than " + minOffsetPending);
    }
    eventProducer.processOne();

    for (int par = 0; par < 10; par++) {
      Assert.assertEquals(handler.getInFlightCount(TOPIC, par), 0);
    }

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);

    Assert.assertEquals(handler.getAckCheckpoint(BIG_CHECKPOINT, Comparator.naturalOrder()).get(), BIG_CHECKPOINT);
  }

  @Test
  public void testOutOfOrderAck() throws Exception {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler = new FlushlessEventProducerHandler<>(eventProducer);

    int partition = 0;
    SourcePartition tp = new SourcePartition(TOPIC, partition);

    // Send 5 messages to partition 0 with increasing checkpoints (0-4)
    for (int i = 0; i < 5; i++) {
      sendEvent(tp, handler, i);
    }

    // simulate callback for checkpoint 4
    eventProducer.process(tp, 4); // inflight result: 0, 1, 2, 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition), Optional.empty(),
        "Safe checkpoint should be empty");

    // simulate callback for checkpoint 2
    eventProducer.process(tp, 2); // inflight result: 0, 1, 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition), Optional.empty(),
        "Safe checkpoint should be empty");

    // simulate callback for checkpoint 0
    eventProducer.process(tp, 0); // inflight result: 1, 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(0),
        "Safe checkpoint should be 0");

    // simulate callback for checkpoint 1
    eventProducer.process(tp, 0); // inflight result: 3
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(2),
        "Safe checkpoint should be 1");

    // send another event with checkpoint 5
    sendEvent(tp, handler, 5); // inflight result: 3, 5
    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 2, "Number of inflight messages should be 2");

    // simulate callback for checkpoint 3
    eventProducer.process(tp, 0); // inflight result: 5
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(4),
        "Safe checkpoint should be 4");

    // simulate callback for checkpoint 5
    eventProducer.process(tp, 0); // inflight result: empty
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(5),
        "Safe checkpoint should be 5");

    Assert.assertEquals(handler.getInFlightCount(TOPIC, partition), 0, "Number of inflight messages should be 0");

    // send another event with checkpoint 6
    sendEvent(tp, handler, 6);
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(5),
        "Safe checkpoint should be 5");

    // simulate callback for checkpoint 6
    eventProducer.process(tp, 0); // inflight result: empty
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(6),
        "Safe checkpoint should be 6");
  }

  @Test
  public void testBackwardsOrderAck() throws Exception {
    RandomEventProducer eventProducer = new RandomEventProducer();
    FlushlessEventProducerHandler<Long> handler = new FlushlessEventProducerHandler<>(eventProducer);

    int partition = 0;
    SourcePartition tp = new SourcePartition(TOPIC, partition);

    // Send 1000 messages to the source partition
    for (int i = 0; i < 1000; i++) {
      sendEvent(tp, handler, i);
    }

    // acknowledge the checkpoints in backward (descending order) to simulate worst case scenario
    for (int i = 999; i > 0; i--) {
      eventProducer.process(tp, i);
      // validate that checkpoint has to be empty because oldest message was not yet acknowledged
      Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition), Optional.empty(),
          "Safe checkpoint should be empty");
    }

    // finally process the oldest message
    eventProducer.process(tp, 0);
    // validate that the checkpoint was finally updated to 999
    Assert.assertEquals(handler.getAckCheckpoint(TOPIC, partition).get(), Long.valueOf(999),
        "Safe checkpoint should be 999");
  }

  private void sendEvent(SourcePartition tp, FlushlessEventProducerHandler<Long> handler, long checkpoint) {
    DatastreamProducerRecord record = getDatastreamProducerRecord(checkpoint, tp.getKey(), tp.getValue());
    handler.send(record, tp.getSource(), tp.getPartition(), checkpoint, null);
  }

  private DatastreamProducerRecord getDatastreamProducerRecord(Long checkpoint, String topic, int partition) {
    BrooklinEnvelope emptyEnvelope = new BrooklinEnvelope("1", "value", new HashMap<>());
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(emptyEnvelope);
    builder.setPartition(partition);
    builder.setSourceCheckpoint(checkpoint.toString());
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    builder.setDestination(topic);
    return builder.build();
  }

  private static class RandomEventProducer implements DatastreamEventProducer {
    Map<SourcePartition, List<Pair<DatastreamProducerRecord, SendCallback>>> _queue = new HashMap<>();
    List<SourcePartition> tps = new ArrayList<>();
    private final double _exceptionProb;

    public RandomEventProducer() {
      this(0.0);
    }

    public RandomEventProducer(double exceptionProb) {
      _exceptionProb = exceptionProb;
    }

    @Override
    public synchronized void send(DatastreamProducerRecord record, SendCallback callback) {
      SourcePartition tp = new SourcePartition(TOPIC, record.getPartition().orElse(0));
      if (!tps.contains(tp)) {
        tps.add(tp);
      }
      _queue.computeIfAbsent(tp, x -> new ArrayList<>()).add(Pair.of(record, callback));
    }

    public synchronized void processOne() {
      int start = RANDOM.nextInt(tps.size());
      for (int i = 0; i < tps.size(); i++) {
        SourcePartition tp = tps.get((i + start) % tps.size());
        if (!_queue.get(tp).isEmpty()) {
          Pair<DatastreamProducerRecord, SendCallback> pair = _queue.get(tp).remove(0);
          DatastreamProducerRecord record = pair.getKey();
          SendCallback callback = pair.getValue();

          DatastreamRecordMetadata metadata =
              new DatastreamRecordMetadata(record.getCheckpoint(), TOPIC, record.getPartition().orElse(0));
          Exception exception = null;
          if (RANDOM.nextDouble() < _exceptionProb) {
            exception = new RuntimeException("Simulating Flakiness sending messages");
          }
          callback.onCompletion(metadata, exception);
          return;
        }
      }
    }

    public synchronized void process(SourcePartition sourcePartition, int queueIndex) {
      List<Pair<DatastreamProducerRecord, SendCallback>> messages = _queue.get(sourcePartition);
      if (messages == null) {
        throw new IllegalArgumentException(
            "There are no messages in the queue for source partition " + sourcePartition);
      }

      if (queueIndex >= messages.size()) {
        throw new IllegalArgumentException(
            "Cannot remove message at index " + queueIndex + " for source partition " + sourcePartition
                + " because messages count is " + messages.size());
      }

      Pair<DatastreamProducerRecord, SendCallback> pair = messages.remove(queueIndex);
      DatastreamProducerRecord record = pair.getKey();
      SendCallback callback = pair.getValue();

      DatastreamRecordMetadata metadata =
          new DatastreamRecordMetadata(record.getCheckpoint(), TOPIC, record.getPartition().orElse(0));
      callback.onCompletion(metadata, null);
    }

    @Override
    public synchronized void flush() {
      while (_queue.values().stream().anyMatch(l -> !l.isEmpty())) {
        processOne();
      }
    }

    public long minCheckpoint() {
      return _queue.values()
          .stream()
          .flatMap(Collection::stream)
          .map(Pair::getKey)
          .map(DatastreamProducerRecord::getCheckpoint)
          .mapToLong(Long::valueOf)
          .min()
          .getAsLong();
    }
  }
}
