/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.utils.Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME;
import static com.github.sonus21.rqueue.utils.Constants.MIN_EXECUTION_TIME;

import java.util.Collections;
import java.util.Set;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
class MappingInformation implements Comparable<MappingInformation> {
  private final Set<String> queueNames;
  private final int numRetries;
  private final boolean delayedQueue;
  private final String deadLetterQueueName;
  private final long maxJobExecutionTime;

  MappingInformation(
      Set<String> queueNames,
      boolean delayedQueue,
      int numRetries,
      String deadLetterQueueName,
      long maxJobExecutionTime) {
    this.queueNames = Collections.unmodifiableSet(queueNames);
    this.delayedQueue = delayedQueue;
    this.numRetries = numRetries;
    this.deadLetterQueueName = deadLetterQueueName;
    this.maxJobExecutionTime = maxJobExecutionTime;
  }

  Set<String> getQueueNames() {
    return queueNames;
  }

  @Override
  public int compareTo(MappingInformation o) {
    return 0;
  }

  @Override
  public String toString() {
    return String.join(", ", queueNames);
  }

  int getNumRetries() {
    return numRetries;
  }

  boolean isDelayedQueue() {
    return delayedQueue;
  }

  String getDeadLetterQueueName() {
    return deadLetterQueueName;
  }

  boolean isValid() {
    return getQueueNames().size() > 0
        && maxJobExecutionTime > MIN_EXECUTION_TIME + DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }

  public long getMaxJobExecutionTime() {
    return maxJobExecutionTime;
  }
}
