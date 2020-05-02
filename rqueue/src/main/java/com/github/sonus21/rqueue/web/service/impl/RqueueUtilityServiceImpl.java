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

import static com.github.sonus21.rqueue.utils.Constants.ONE_MILLI;
import static com.github.sonus21.rqueue.utils.HttpUtils.readUrl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.MoveMessageRequest;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.MoveMessageResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RqueueUtilityServiceImpl implements RqueueUtilityService {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final RqueueSystemConfigDao rqueueSystemConfigDao;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final RqueueMessageMetadataService messageMetadataService;
  private String latestVersion = "NA";
  private long versionFetchTime = 0;

  @Autowired
  public RqueueUtilityServiceImpl(
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueSystemConfigDao rqueueSystemConfigDao,
      RqueueWebConfig rqueueWebConfig,
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueMessageMetadataService messageMetadataService) {
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.rqueueSystemConfigDao = rqueueSystemConfigDao;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.messageMetadataService = messageMetadataService;
  }

  @Override
  public BooleanResponse deleteMessage(String queueName, String id) {
    String queueConfigKey = QueueUtils.getQueueConfigKey(queueName);
    QueueConfig queueConfig = rqueueSystemConfigDao.getQConfig(queueConfigKey);
    BooleanResponse booleanResponse = new BooleanResponse();
    if (queueConfig == null) {
      booleanResponse.setCode(1);
      booleanResponse.setMessage("Queue config not found!");
      return booleanResponse;
    }
    messageMetadataService.deleteMessage(id, Duration.ofDays(Constants.DAYS_IN_A_MONTH));
    booleanResponse.setValue(true);
    return booleanResponse;
  }

  private MoveMessageResponse moveMessageToZset(MoveMessageRequest moveMessageRequest) {
    String src = moveMessageRequest.getSrc();
    String dst = moveMessageRequest.getDst();
    int requestMessageCount = moveMessageRequest.getMessageCount(rqueueWebConfig);
    String newScore = (String) moveMessageRequest.getOthers().get("newScore");
    Boolean isFixedScore = (Boolean) moveMessageRequest.getOthers().get("fixedScore");
    long scoreInMilli = 0;
    boolean fixedScore = false;
    if (newScore != null) {
      scoreInMilli = Long.parseLong(newScore);
    }
    if (isFixedScore != null) {
      fixedScore = isFixedScore;
    }
    MessageMoveResult result;
    if (moveMessageRequest.getSrcType() == DataType.ZSET) {
      result =
          rqueueMessageTemplate.moveMessageZsetToZset(
              src, dst, requestMessageCount, scoreInMilli, fixedScore);
    } else {
      result =
          rqueueMessageTemplate.moveMessageListToZset(src, dst, requestMessageCount, scoreInMilli);
    }
    MoveMessageResponse response = new MoveMessageResponse(result.getNumberOfMessages());
    response.setValue(result.isSuccess());
    return response;
  }

  private MoveMessageResponse moveMessageToList(MoveMessageRequest moveMessageRequest) {
    String src = moveMessageRequest.getSrc();
    String dst = moveMessageRequest.getDst();
    int requestMessageCount = moveMessageRequest.getMessageCount(rqueueWebConfig);
    MessageMoveResult result;
    if (moveMessageRequest.getSrcType() == DataType.ZSET) {
      result = rqueueMessageTemplate.moveMessageZsetToList(src, dst, requestMessageCount);
    } else {
      result = rqueueMessageTemplate.moveMessageListToList(src, dst, requestMessageCount);
    }
    MoveMessageResponse response = new MoveMessageResponse(result.getNumberOfMessages());
    response.setValue(result.isSuccess());
    return response;
  }

  @Override
  public MoveMessageResponse moveMessage(MoveMessageRequest moveMessageRequest) {
    String message = moveMessageRequest.validationMessage();
    if (!StringUtils.isEmpty(message)) {
      MoveMessageResponse transferResponse = new MoveMessageResponse();
      transferResponse.setCode(1);
      transferResponse.setMessage(message);
      return transferResponse;
    }
    DataType dstType = moveMessageRequest.getDstType();
    switch (dstType) {
      case ZSET:
        return moveMessageToZset(moveMessageRequest);
      case LIST:
        return moveMessageToList(moveMessageRequest);
      default:
        throw new UnknownSwitchCase(dstType.name());
    }
  }

  @Override
  public BooleanResponse deleteQueueMessages(String queueName, int remainingMessages) {
    int start = -1 * remainingMessages;
    int end = -1;
    if (remainingMessages == 0) {
      start = 2;
      end = 1;
    }
    if (stringRqueueRedisTemplate.type(queueName)
        == org.springframework.data.redis.connection.DataType.LIST) {
      stringRqueueRedisTemplate.ltrim(queueName, start, end);
      return new BooleanResponse(true);
    }
    return new BooleanResponse(false);
  }

  @Override
  public String getLatestVersion() {
    if (System.currentTimeMillis() - versionFetchTime > Constants.SECONDS_IN_A_DAY * ONE_MILLI) {
      String response = readUrl(Constants.MAVEN_REPO_LINK + "/maven-metadata.xml");
      if (response != null) {
        List<String> lines =
            Arrays.stream(response.split("\n"))
                .map(String::trim)
                .filter(e -> e.startsWith("<latest>"))
                .collect(Collectors.toList());
        if (!lines.isEmpty()) {
          latestVersion = lines.get(0);
          versionFetchTime = System.currentTimeMillis();
        }
      }
    }
    return latestVersion;
  }

  @Override
  public StringResponse getDataType(String name) {
    return new StringResponse(
        DataType.convertDataType(stringRqueueRedisTemplate.type(name)).name());
  }
}
