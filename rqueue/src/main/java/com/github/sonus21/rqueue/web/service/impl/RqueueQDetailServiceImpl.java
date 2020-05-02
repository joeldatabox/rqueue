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

package com.github.sonus21.rqueue.web.service.impl;

import static com.github.sonus21.rqueue.utils.StringUtils.clean;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.ActionType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.utils.TimeUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
public class RqueueQDetailServiceImpl implements RqueueQDetailService {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final RqueueSystemManagerService rqueueSystemManagerService;
  private final RqueueMessageMetadataService rqueueMessageMetadataService;

  @Autowired
  public RqueueQDetailServiceImpl(
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueSystemManagerService rqueueSystemManagerService,
      RqueueMessageMetadataService rqueueMessageMetadataService) {
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.rqueueSystemManagerService = rqueueSystemManagerService;
    this.rqueueMessageMetadataService = rqueueMessageMetadataService;
  }

  @Override
  public Map<String, List<Entry<NavTab, RedisDataDetail>>> getQueueDataStructureDetails(
      List<QueueConfig> queueConfig) {
    return queueConfig
        .parallelStream()
        .collect(Collectors.toMap(QueueConfig::getName, this::getQueueDataStructureDetail));
  }

  @Override
  public List<Entry<NavTab, RedisDataDetail>> getQueueDataStructureDetail(QueueConfig queueConfig) {
    List<Entry<NavTab, RedisDataDetail>> metaData = new ArrayList<>();
    if (queueConfig == null) {
      return metaData;
    }
    String name = queueConfig.getName();
    Long pending = stringRqueueRedisTemplate.getListSize(name);
    metaData.add(
        new HashMap.SimpleEntry<>(
            NavTab.PENDING,
            new RedisDataDetail(name, DataType.LIST, pending == null ? 0 : pending)));
    String processingQueueName = QueueUtils.getProcessingQueueName(name);
    Long running = stringRqueueRedisTemplate.getZsetSize(processingQueueName);
    metaData.add(
        new HashMap.SimpleEntry<>(
            NavTab.RUNNING,
            new RedisDataDetail(
                processingQueueName, DataType.ZSET, running == null ? 0 : running)));
    if (queueConfig.isDelayed()) {
      String timeQueueName = QueueUtils.getDelayedQueueName(name);
      Long scheduled = stringRqueueRedisTemplate.getZsetSize(timeQueueName);
      metaData.add(
          new HashMap.SimpleEntry<>(
              NavTab.SCHEDULED,
              new RedisDataDetail(
                  timeQueueName, DataType.ZSET, scheduled == null ? 0 : scheduled)));
    }
    if (!CollectionUtils.isEmpty(queueConfig.getDeadLetterQueues())) {
      for (String dlq : queueConfig.getDeadLetterQueues()) {
        Long dlqSize = stringRqueueRedisTemplate.getListSize(dlq);
        metaData.add(
            new HashMap.SimpleEntry<>(
                NavTab.DEAD,
                new RedisDataDetail(dlq, DataType.LIST, dlqSize == null ? 0 : dlqSize)));
      }
    }
    return metaData;
  }

  @Override
  public List<NavTab> getNavTabs(QueueConfig queueConfig) {
    List<NavTab> navTabs = new ArrayList<>();
    if (queueConfig != null) {
      navTabs.add(NavTab.PENDING);
      if (queueConfig.isDelayed()) {
        navTabs.add(NavTab.SCHEDULED);
      }
      navTabs.add(NavTab.RUNNING);
      if (!CollectionUtils.isEmpty(queueConfig.getDeadLetterQueues())) {
        navTabs.add(NavTab.DEAD);
      }
    }
    return navTabs;
  }

  private List<TypedTuple<RqueueMessage>> readFromZset(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;

    return rqueueMessageTemplate.readFromZset(name, start, end).stream()
        .map(e -> new DefaultTypedTuple<>(e, null))
        .collect(Collectors.toList());
  }

  private List<TypedTuple<RqueueMessage>> readFromList(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromList(name, start, end).stream()
        .map(e -> new DefaultTypedTuple<>(e, null))
        .collect(Collectors.toList());
  }

  private List<TypedTuple<RqueueMessage>> readFromZetWithScore(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromZsetWithScore(name, start, end);
  }

  private interface RowBuilder {
    List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted, Double score);
  }

  private List<List<Serializable>> buildRows(
      List<TypedTuple<RqueueMessage>> rqueueMessages, RowBuilder rowBuilder) {
    if (CollectionUtils.isEmpty(rqueueMessages)) {
      return Collections.emptyList();
    }
    List<String> ids =
        rqueueMessages.stream()
            .map(e -> Objects.requireNonNull(e.getValue()).getId())
            .map(QueueUtils::getMessageMetadataKey)
            .collect(Collectors.toList());

    List<MessageMetadata> vals = rqueueMessageMetadataService.findAll(ids);
    Map<String, Boolean> msgIdToDeleted =
        vals.stream().collect(Collectors.toMap(MessageMetadata::getMessageId, e -> true));
    return rqueueMessages.stream()
        .map(
            e ->
                rowBuilder.row(
                    e.getValue(),
                    msgIdToDeleted.getOrDefault(e.getValue().getId(), false),
                    e.getScore()))
        .collect(Collectors.toList());
  }

  class ZsetRowBuilder implements RowBuilder {
    private final long currentTime;
    private final boolean timeQueue;

    ZsetRowBuilder(boolean timeQueue) {
      this.timeQueue = timeQueue;
      this.currentTime = System.currentTimeMillis();
    }

    @Override
    public List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted, Double score) {
      List<Serializable> row = new ArrayList<>();
      row.add(rqueueMessage.getId());
      row.add(rqueueMessage.toString());
      if (timeQueue) {
        row.add(TimeUtils.millisToHumanRepresentation(rqueueMessage.getProcessAt() - currentTime));
      } else {
        row.add(TimeUtils.millisToHumanRepresentation(score.longValue() - currentTime));
      }
      if (!deleted) {
        row.add(ActionType.DELETE);
      } else {
        row.add(Constants.BLANK);
      }
      return row;
    }
  }

  static class ListRowBuilder implements RowBuilder {
    private final boolean deadLetterQueue;

    ListRowBuilder(boolean deadLetterQueue) {
      this.deadLetterQueue = deadLetterQueue;
    }

    @Override
    public List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted, Double score) {
      List<Serializable> row = new ArrayList<>();
      row.add(rqueueMessage.getId());
      row.add(rqueueMessage.toString());
      if (!deadLetterQueue) {
        if (deleted) {
          row.add("");
        } else {
          row.add(ActionType.DELETE);
        }
      } else {
        row.add(TimeUtils.formatMilliToString(rqueueMessage.getReEnqueuedAt()));
      }
      return row;
    }
  }

  @Override
  public DataViewResponse getExplorePageData(
      String src, String name, DataType type, int pageNumber, int itemPerPage) {
    QueueConfig metaData = rqueueSystemManagerService.getQueueConfig(src);
    DataViewResponse response = new DataViewResponse();
    boolean deadLetterQueue = metaData.isDelayedQueue(name);
    boolean timeQueue = QueueUtils.isTimeQueue(name);
    setHeadersIfRequired(deadLetterQueue, type, response, pageNumber);

    if (deadLetterQueue) {
      response.addAction(ActionType.DELETE);
    } else {
      response.addAction(ActionType.NONE);
    }
    switch (type) {
      case ZSET:
        if (timeQueue) {
          response.setRows(
              buildRows(readFromZset(name, pageNumber, itemPerPage), new ZsetRowBuilder(true)));
        } else {
          response.setRows(
              buildRows(
                  readFromZetWithScore(name, pageNumber, itemPerPage), new ZsetRowBuilder(false)));
        }
        break;
      case LIST:
        response.setRows(
            buildRows(
                readFromList(name, pageNumber, itemPerPage), new ListRowBuilder(deadLetterQueue)));
        break;
      default:
        throw new UnknownSwitchCase(type.name());
    }
    return response;
  }

  private DataViewResponse responseForSet(String name) {
    List<Object> items = new ArrayList<>(stringRqueueRedisTemplate.getMembers(name));
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Item"));
    List<List<Serializable>> rows = new ArrayList<>();
    for (Object item : items) {
      rows.add(Collections.singletonList(item.toString()));
    }
    response.setRows(rows);
    return response;
  }

  private DataViewResponse responseForKeyVal(String name) {
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Value"));
    Object val = stringRqueueRedisTemplate.get(name);
    List<List<Serializable>> rows =
        Collections.singletonList(Collections.singletonList(String.valueOf(val)));
    response.setRows(rows);
    return response;
  }

  private DataViewResponse responseForZset(
      String name, String key, int pageNumber, int itemPerPage) {
    DataViewResponse response = new DataViewResponse();
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<List<Serializable>> rows = new ArrayList<>();
    if (!StringUtils.isEmpty(key)) {
      Double score = stringRqueueRedisTemplate.getZsetMemberScore(name, key);
      response.setHeaders(Collections.singletonList("Score"));
      rows.add(Collections.singletonList(score));
    } else {
      response.setHeaders(Arrays.asList("Item", "Score"));
      for (TypedTuple<String> tuple : stringRqueueRedisTemplate.zrangeWithScore(name, start, end)) {
        rows.add(Arrays.asList(String.valueOf(tuple.getValue()), tuple.getScore()));
      }
    }
    response.setRows(rows);
    return response;
  }

  private DataViewResponse responseForList(String name, int pageNumber, int itemPerPage) {
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Item"));
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<List<Serializable>> rows = new ArrayList<>();
    for (Object s : stringRqueueRedisTemplate.lrange(name, start, end)) {
      List<Serializable> singletonList = Collections.singletonList(String.valueOf(s));
      rows.add(singletonList);
    }
    response.setRows(rows);
    return response;
  }

  @Override
  public DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    if (StringUtils.isEmpty(name)) {
      return DataViewResponse.createErrorMessage("Data name cannot be empty.");
    }
    if (DataType.isUnknown(type)) {
      return DataViewResponse.createErrorMessage("Data type is not provided.");
    }
    switch (type) {
      case SET:
        return responseForSet(clean(name));
      case ZSET:
        return responseForZset(clean(name), clean(key), pageNumber, itemPerPage);
      case LIST:
        return responseForList(clean(name), pageNumber, itemPerPage);
      case KEY:
        return responseForKeyVal(clean(name));
      default:
        throw new UnknownSwitchCase(type.name());
    }
  }

  private void setHeadersIfRequired(
      boolean deadLetterQueue, DataType type, DataViewResponse response, int pageNumber) {
    if (pageNumber != 0) {
      return;
    }
    List<String> headers = new ArrayList<>();
    headers.add("Id");
    headers.add("Message");
    if (DataType.ZSET == type) {
      headers.add("Time Left");
    }
    if (!deadLetterQueue) {
      headers.add("Action");
    } else {
      headers.add("AddedOn");
    }
    response.setHeaders(headers);
  }

  @Override
  public List<List<Object>> getRunningTasks() {
    List<String> queues = rqueueSystemManagerService.getQueues();
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queues)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (String queue : queues) {
                  connection.zCard(QueueUtils.getProcessingQueueName(queue).getBytes());
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Processing ZSET");
    headers.add("Size");
    rows.add(headers);
    for (int i = 0; i < queues.size(); i++) {
      List<Object> row = new ArrayList<>();
      row.add(queues.get(i));
      row.add(QueueUtils.getProcessingQueueName(queues.get(i)));
      row.add(result.get(i));
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<List<Object>> getWaitingTasks() {
    List<String> queues = rqueueSystemManagerService.getQueues();
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queues)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (String queue : queues) {
                  connection.lLen(queue.getBytes());
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Size");
    rows.add(headers);
    for (int i = 0; i < queues.size(); i++) {
      List<Object> row = new ArrayList<>();
      row.add(queues.get(i));
      row.add(result.get(i));
      rows.add(row);
    }
    return rows;
  }

  @Override
  public List<List<Object>> getScheduledTasks() {
    List<String> queues = rqueueSystemManagerService.getQueues();
    List<QueueConfig> queueConfigs =
        rqueueSystemManagerService.getQueueConfigs(queues).stream()
            .filter(QueueConfig::isDelayed)
            .collect(Collectors.toList());
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueConfigs)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (QueueConfig metadata : queueConfigs) {
                  connection.zCard(QueueUtils.getDelayedQueueName(metadata.getName()).getBytes());
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Scheduled ZSET");
    headers.add("Size");
    rows.add(headers);
    for (int i = 0; i < queueConfigs.size(); i++) {
      List<Object> row = new ArrayList<>();
      QueueConfig metadata = queueConfigs.get(i);
      row.add(metadata.getName());
      row.add(QueueUtils.getDelayedQueueName(metadata.getName()));
      row.add(result.get(i));
      rows.add(row);
    }
    return rows;
  }

  private void addRows(
      List<Object> result, List<List<Object>> rows, List<Entry<String, String>> queueNameAndDlq) {
    for (int i = 0, j = 0; i < queueNameAndDlq.size(); i++) {
      Entry<String, String> entry = queueNameAndDlq.get(i);
      List<Object> row = new ArrayList<>();
      if (entry.getValue().isEmpty()) {
        row.add(entry.getKey());
        row.add("");
        row.add("");
      } else {
        if (i == 0
            || !queueNameAndDlq.get(i).getKey().equals(queueNameAndDlq.get(i - 1).getKey())) {
          row.add(entry.getKey());
        } else {
          row.add("");
        }
        row.add(entry.getValue());
        row.add(result.get(j++));
      }
      rows.add(row);
    }
  }

  @Override
  public List<List<Object>> getDeadLetterTasks() {
    List<String> queues = rqueueSystemManagerService.getQueues();
    List<Entry<String, String>> queueNameAndDlq = new ArrayList<>();
    for (QueueConfig queueConfig : rqueueSystemManagerService.getQueueConfigs(queues)) {
      if (queueConfig.hasDeadLetterQueue()) {
        for (String dlq : queueConfig.getDeadLetterQueues()) {
          queueNameAndDlq.add(new HashMap.SimpleEntry<>(queueConfig.getName(), dlq));
        }
      } else {
        queueNameAndDlq.add(new HashMap.SimpleEntry<>(queueConfig.getName(), ""));
      }
    }
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueNameAndDlq)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (Entry<String, String> entry : queueNameAndDlq) {
                  if (!entry.getValue().isEmpty()) {
                    connection.lLen(entry.getValue().getBytes());
                  }
                }
              }));
    }
    List<Object> headers = new ArrayList<>();
    headers.add("Queue");
    headers.add("Dead Letter Queue");
    headers.add("Size");
    rows.add(headers);
    addRows(result, rows, queueNameAndDlq);
    return rows;
  }
}
