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

package com.github.sonus21.rqueue.config;

import static com.github.sonus21.rqueue.utils.Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME;
import static com.github.sonus21.rqueue.utils.Constants.MIN_EXECUTION_TIME;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.processor.MessageProcessor;
import com.github.sonus21.rqueue.processor.NoOpMessageProcessor;
import com.github.sonus21.rqueue.utils.Constants;
import java.time.Duration;
import java.util.List;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;

/**
 * This is a bare minimal factory class, that can be used to create {@link
 * RqueueMessageListenerContainer} object. Factory has multiple methods to support different types
 * of requirements.
 */
public class SimpleRqueueListenerContainerFactory {
  // Provide task executor, this can be used to provide some additional details like some threads
  // name, etc otherwise a default task executor would be created
  private AsyncTaskExecutor taskExecutor;
  // whether container should auto start or not
  private boolean autoStartup = true;
  // Redis connection factory for the listener container
  private RedisConnectionFactory redisConnectionFactory;
  // Custom requeue message handler
  private RqueueMessageHandler rqueueMessageHandler;
  // List of message converters to convert messages to/from
  private List<MessageConverter> messageConverters;
  // In case of failure how much time, we should wait for next job
  private Long backOffTime;
  // Number of workers requires for execution
  private Integer maxNumWorkers;
  // Control how much time a job takes in execution, this can be used to fast-recovery
  // when a job goes to running state then if it's not deleted within N secs then
  // it has to be re-processed, that re-process time can be controller using this field.
  // For example a job started execution at 10:30AM and executor was shutdown so this task requires
  // retry By default it will be retried in 15 minutes, but if you want to reprocess quickly/defer
  // further than this can be used to reprocess
  private long maxJobExecutionTime = Constants.MAX_JOB_EXECUTION_TIME;

  // This message processor would be called whenever a message is discarded due to retry limit
  // exhaustion
  private MessageProcessor discardMessageProcessor = new NoOpMessageProcessor();
  // This message processor would be called whenever a message is moved to dead letter queue
  private MessageProcessor deadLetterQueueMessageProcessor = new NoOpMessageProcessor();

  // Any custom message requeue message template.
  private RqueueMessageTemplate rqueueMessageTemplate;

  /**
   * Get configured task executor
   *
   * @return async task executor
   */
  public AsyncTaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  /**
   * Configures the {@link TaskExecutor} which is used to poll messages and execute them by calling
   * the handler methods. If no {@link TaskExecutor} is set, a default one is created.
   *
   * @param taskExecutor The {@link TaskExecutor} used by the container
   * @see RqueueMessageListenerContainer#createDefaultTaskExecutor()
   */
  public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
    Assert.notNull(taskExecutor, "taskExecutor can not be null");
    this.taskExecutor = taskExecutor;
  }

  public Boolean getAutoStartup() {
    return autoStartup;
  }

  /**
   * Configures if this container should be automatically started. The default value is true.
   *
   * @param autoStartup - false if the container will be manually started
   */
  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  /**
   * Return configured message handler
   *
   * @return RqueueMessageHandler object
   */
  public RqueueMessageHandler getRqueueMessageHandler() {
    return rqueueMessageHandler;
  }

  /**
   * Set message handler, this can be used to set custom message handlers that could have some
   * special features apart of the default one
   *
   * @param rqueueMessageHandler {@link RqueueMessageHandler} object
   */
  public void setRqueueMessageHandler(RqueueMessageHandler rqueueMessageHandler) {
    Assert.notNull(rqueueMessageHandler, "rqueueMessageHandler must not be null");
    this.rqueueMessageHandler = rqueueMessageHandler;
  }

  /**
   * @return The number of milliseconds the polling thread must wait before trying to recover when
   *     an error occurs (e.g. connection timeout)
   */
  public Long getBackOffTime() {
    return backOffTime;
  }

  /**
   * The number of milliseconds the polling thread must wait before trying to recover when an error
   * occurs (e.g. connection timeout). Default value is 10000 milliseconds.
   *
   * @param backOffTime in milliseconds
   */
  public void setBackOffTime(long backOffTime) {
    this.backOffTime = backOffTime;
  }

  public Integer getMaxNumWorkers() {
    return maxNumWorkers;
  }

  /**
   * Maximum number of workers, that would be used to run tasks.
   *
   * @param maxNumWorkers Maximum number of workers
   */
  public void setMaxNumWorkers(int maxNumWorkers) {
    this.maxNumWorkers = maxNumWorkers;
  }

  /** @return list of configured message converters */
  public List<MessageConverter> getMessageConverters() {
    return messageConverters;
  }

  /**
   * For message (de)serialization we might need one or more message converters, configure those
   * message converters
   *
   * @param messageConverters list of message converters
   */
  public void setMessageConverters(List<MessageConverter> messageConverters) {
    Assert.notEmpty(messageConverters, "messageConverters must not be empty");
    this.messageConverters = messageConverters;
  }

  /** @return get Redis connection factor */
  public RedisConnectionFactory getRedisConnectionFactory() {
    return redisConnectionFactory;
  }

  /**
   * Set redis connection factory, that would be used to configured message template and other
   * components
   *
   * @param redisConnectionFactory redis connection factory object
   */
  public void setRedisConnectionFactory(RedisConnectionFactory redisConnectionFactory) {
    Assert.notNull(redisConnectionFactory, "redisConnectionFactory must not be null");
    this.redisConnectionFactory = redisConnectionFactory;
  }

  /** @return message template */
  public RqueueMessageTemplate getRqueueMessageTemplate() {
    return rqueueMessageTemplate;
  }

  /**
   * Set RqueueMessageTemplate that's used to pull and push messages from/to Redis.
   *
   * @param messageTemplate a message template object
   */
  public void setRqueueMessageTemplate(RqueueMessageTemplate messageTemplate) {
    Assert.notNull(messageTemplate, "messageTemplate must not be null");
    rqueueMessageTemplate = messageTemplate;
  }

  /**
   * Creates a {@link RqueueMessageListenerContainer} container. To create this container we would
   * need redis connection factory {@link RedisConnectionFactory }as well as message handler {@link
   * RqueueMessageHandler}.
   *
   * @return an object of {@link RqueueMessageListenerContainer} object
   */
  public RqueueMessageListenerContainer createMessageListenerContainer() {
    Assert.notNull(getRqueueMessageHandler(), "rqueueMessageHandler must not be null");
    Assert.notNull(redisConnectionFactory, "redisConnectionFactory must not be null");
    if (rqueueMessageTemplate == null) {
      rqueueMessageTemplate =
          new RqueueMessageTemplate(redisConnectionFactory, maxJobExecutionTime);
    }
    RqueueMessageListenerContainer messageListenerContainer =
        new RqueueMessageListenerContainer(
            getRqueueMessageHandler(),
            rqueueMessageTemplate,
            getDiscardMessageProcessor(),
            getDeadLetterQueueMessageProcessor(),
            getMaxJobExecutionTime());
    messageListenerContainer.setAutoStartup(autoStartup);
    if (taskExecutor != null) {
      messageListenerContainer.setTaskExecutor(taskExecutor);
    }
    if (maxNumWorkers != null) {
      messageListenerContainer.setMaxNumWorkers(maxNumWorkers);
    }
    if (backOffTime != null) {
      messageListenerContainer.setBackOffTime(backOffTime);
    }
    return messageListenerContainer;
  }

  public long getMaxJobExecutionTime() {
    return maxJobExecutionTime;
  }

  /**
   * Control how much time a job takes in execution, this can be used to fast-recovery when a job
   * goes to running state then if it's not deleted within N secs then it has to be re-processed,
   * that re-process time can be controller using this.
   *
   * <p>For example a job started execution at 10:30AM and executor was shutdown so this task
   * requires retry By default it will be retried in 15 minutes, but if you want to reprocess
   * quickly/defer further than this can be used to reprocess.
   *
   * <p>Minimum time is based on the two factors <br>
   * 1. Actual Task execution time <br>
   * 2. Redis execution time and thread busyness.
   *
   * <p>NOTE: * If provided time is too small then same tasks would be running multiple times, that
   * can cause problem in the application. On the other-side if provided time is too large than the
   * task retry would be delayed.
   *
   * @param maxJobExecutionTime total job execution time
   */
  public void setMaxJobProcessTime(Duration maxJobExecutionTime) {
    long mili = maxJobExecutionTime.toMillis();
    if (mili < MIN_EXECUTION_TIME + DELTA_BETWEEN_RE_ENQUEUE_TIME) {
      throw new IllegalArgumentException("job execution time is too less");
    }
    this.maxJobExecutionTime = mili;
  }

  public MessageProcessor getDiscardMessageProcessor() {
    return discardMessageProcessor;
  }

  /**
   * This message processor would be called whenever a message is discarded due to retry limit
   * exhaust.
   *
   * @param discardMessageProcessor object of the discard message processor.
   */
  public void setDiscardMessageProcessor(MessageProcessor discardMessageProcessor) {
    Assert.notNull(discardMessageProcessor, "discardMessageProcessor cannot be null");
    this.discardMessageProcessor = discardMessageProcessor;
  }

  public MessageProcessor getDeadLetterQueueMessageProcessor() {
    return deadLetterQueueMessageProcessor;
  }

  /**
   * This message processor would be called whenever a message is moved to dead letter queue
   *
   * @param deadLetterQueueMessageProcessor object of message processor.
   */
  public void setDeadLetterQueueMessageProcessor(MessageProcessor deadLetterQueueMessageProcessor) {
    Assert.notNull(
        deadLetterQueueMessageProcessor, "deadLetterQueueMessageProcessor cannot be null");
    this.deadLetterQueueMessageProcessor = deadLetterQueueMessageProcessor;
  }
}
