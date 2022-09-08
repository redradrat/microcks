/*
 * Licensed to Laurent Broudoux (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.github.microcks.minion.async.producer;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import org.jboss.logging.Logger;

import io.github.microcks.domain.EventMessage;
import io.github.microcks.minion.async.AsyncMockDefinition;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.io.UnsupportedEncodingException;

/**
 * SQS implementation of producer for async event messages.
 * @author laurent
 */
@ApplicationScoped
public class SQSProducerManager {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   private SqsClient client;

   @ConfigProperty(name = "sqs.endpoint")
   String sqsEndpoint;

   @ConfigProperty(name = "sqs.region")
   String sqsRegion;

   @ConfigProperty(name = "sqs.username")
   String sqsUsername;

   @ConfigProperty(name = "sqs.password")
   String sqsPassword;

   /**
    * Initialize the SQS client post construction.
    * @throws Exception If connection to SQS Broker cannot be done.
    */
   @PostConstruct
   public void create() throws Exception {
      try {
         client = createClient();
      } catch (Exception e) {
         logger.errorf("Cannot connect to SQS endpoint %s", );
         logger.errorf("Connection exception: %s", e.getMessage());
         throw e;
      }
   }

   /**
    * Create a SqsClient and connect it to the server.
    * @return A new SqsClient implementation initialized with configuration properties.
    * @throws Exception in case of connection failure
    */
   protected SqsClient createClient() throws Exception {
      SqsClient sqsClient = SqsClient.builder()
       .region(Region.of(sqsRegion))
       .credentialsProvider(ProfileCredentialsProvider.create())
       .build();
      return sqsClient;
   }

   /**
    * Publish a message on specified topic.
    * @param topic The destination topic for message
    * @param value The message payload
    */
   public void publishMessage(String topic, String value) {
      logger.infof("Publishing on topic {%s}, message: %s ", topic, value);
      try {
         client.publish(topic, value.getBytes("UTF-8"), 0, false);
      } catch (UnsupportedEncodingException uee) {
         logger.warnf("Message %s cannot be encoded as UTF-8 bytes, ignoring it", uee);
      } catch (MqttPersistenceException mpe) {
         mpe.printStackTrace();
      } catch (MqttException me) {
         me.printStackTrace();
      }
   }

   /**
    * Get the SQS topic name corresponding to a AsyncMockDefinition, sanitizing all parameters.
    * @param definition The AsyncMockDefinition
    * @param eventMessage The message to get topic
    * @return The topic name for definition and event
    */
   public String getTopicName(AsyncMockDefinition definition, EventMessage eventMessage) {
      logger.debugf("AsyncAPI Operation {%s}", definition.getOperation().getName());
      // Produce service name part of topic name.
      String serviceName = definition.getOwnerService().getName().replace(" ", "");
      serviceName = serviceName.replace("-", "");
      // Produce version name part of topic name.
      String versionName = definition.getOwnerService().getVersion().replace(" ", "");
      // Produce operation name part of topic name.
      String operationName = definition.getOperation().getName();
      if (operationName.startsWith("SUBSCRIBE ") || operationName.startsWith("PUBLISH ")) {
         operationName = operationName.substring(operationName.indexOf(" ") + 1);
      }

      // replace the parts
      operationName = ProducerManager.replacePartPlaceholders(eventMessage, operationName);

      // Aggregate the 3 parts using '_' as delimiter.
      return serviceName + "-" + versionName + "-" + operationName;
   }

}
