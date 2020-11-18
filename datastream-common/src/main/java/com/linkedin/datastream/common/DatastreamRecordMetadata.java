/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.Objects;

/**
 * Metadata of the successfully produced datastream record
 */
public class DatastreamRecordMetadata {

  private final String _topic;
  private final int _partition;
  private final String _checkpoint;

  /**
   * Construct an instance of DatastreamRecordMetadata
   * @param  checkpoint checkpoint string
   * @param topic Kafka topic name
   * @param partition Kafka topic partition
   */
  public DatastreamRecordMetadata(String checkpoint, String topic, int partition) {
    _checkpoint = checkpoint;
    _topic = topic;
    _partition = partition;
  }

  /**
   * Source checkpoint of the produced record.
   */
  public String getCheckpoint() {
    return _checkpoint;
  }

  /**
   * Partition number to which the record was produced to.
   */
  public int getPartition() {
    return _partition;
  }

  /**
   * Topic to which the record was produced to
   */
  public String getTopic() {
    return _topic;
  }

  @Override
  public String toString() {
    return String.format("Checkpoint: %s, Topic: %s, Partition: %d", _checkpoint, _topic, _partition);
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    } else {
      final DatastreamRecordMetadata otherMetadata = (DatastreamRecordMetadata) other;
      return _partition == otherMetadata._partition &&
              Objects.equals(_topic, otherMetadata._topic) &&
              Objects.equals(_checkpoint, otherMetadata._checkpoint);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(_topic, _partition, _checkpoint);
  }
}
