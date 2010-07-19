/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.handlers.amqp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;

/**
 * This uses the amqp (Advanced Message Queueing Protocol) java interface
 * provided by RabbitMQ. Currently, all values are hardcoded for local tests.
 * 
 * This is a source -- it polls a channel on the AMQP broker and takes messages
 * as input.
 * 
 */
public class AmqpPollingSource extends EventSource.Base {
  Channel channel;
  String exchangeName = "localhost"; // "exName";
  String routingKey = "routingKey";
  QueueingConsumer consumer; // = new QueueingConsumer(channel);
  String queueName = "queue";

  public AmqpPollingSource(String exchangeName, String routingKey)
      throws IOException {
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
  }

  public AmqpPollingSource() throws IOException {
    this("localhost", "routingKey");
  }

  public Event next() throws IOException {
    // boolean noAck = false;
    boolean noAck = true;
    GetResponse response = channel.basicGet(queueName, noAck);
    if (response == null) {
      return null;
    }

    // AMQP.BasicProperties props = response.getProps();
    byte[] bytes = response.getBody();
    WriteableEvent e = WriteableEvent.createWriteableEvent(bytes);

    long deliveryTag = response.getEnvelope().getDeliveryTag();
    channel.basicAck(deliveryTag, false);
    // acknowledge receipt of the message
    return e.getEvent();

  }

  public Event next_old() throws IOException {
    boolean noAck = false;
    channel.basicConsume(queueName, noAck, consumer);

    QueueingConsumer.Delivery delivery;
    try {
      delivery = consumer.nextDelivery();

      long deliveryTag = delivery.getEnvelope().getDeliveryTag();

      byte[] bytes = delivery.getBody();
      WriteableEvent e = WriteableEvent.createWriteableEvent(bytes);

      channel.basicAck(deliveryTag, false);
      return e.getEvent();

    } catch (InterruptedException ie) {
      throw new RuntimeException(
          "Interrupted exception in message queue consume!");
    }
  }

  /**
   * This program just sits and listens on the default sink exchange and queues
   * and counts messages, periodically outputting a count of the entries
   * received
   */
  public static void main(String[] argv) throws IOException {
    AmqpPollingSource sink = new AmqpPollingSource();
    sink.open();
    long last = System.currentTimeMillis();
    long count = 0, lastCount = 0;
    long size = 0, lastSize = 0;

    // TODO(jon) There is a problem here there is no time based display. This
    // only displays messages after some delta of messages comes in. (So if we
    // haven't reached the threshold, the program appears hung since sink.next
    // is blocking.
    while (true) {
      try {

        Event e = sink.next();
        if (e == null)
          continue;

        count++;
        size += e.getBody().length;
        long now = System.currentTimeMillis();
        if ((now - last) > 5000) {
          System.out.printf(
              "%10s %15d msgs %15d sz - delta %8dms %12d msgs %12d sz\n",
              new Date(), count, size, now - last, count - lastCount, size
                  - lastSize);
          lastCount = count;
          lastSize = size;
          last = System.currentTimeMillis();
        }

      } catch (IOException e) {
        System.out.printf("%10s %15d msgs %15d sz - delta %12d msgs %12d sz\n",
            new Date(), count, size, count - lastCount, size - lastSize);

      }
    }
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public void open() throws IOException {
    ConnectionFactory factory = new ConnectionFactory();
    Connection conn = factory.newConnection("localhost");

    channel = conn.createChannel();
    channel.exchangeDeclare(exchangeName, "direct");
    channel.queueDeclare(queueName);
    channel.queueBind(queueName, exchangeName, routingKey);
    // channel.basicQos(1000);
    consumer = new QueueingConsumer(channel);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "usage: amqpPoll(exchange, routingkey");
        String exchangeName = argv[0];
        String routingKey = argv[1];
        try {
          return new AmqpPollingSource(exchangeName, routingKey);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Failed to build a ampq poller on exchange " + exchangeName
                  + " with routing key " + routingKey);
        }
      }

    };
  }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin source.
   */
  public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
    List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
    builders.add(new Pair<String, SourceBuilder>("amqpPoll", builder()));
    return builders;
  }
}
