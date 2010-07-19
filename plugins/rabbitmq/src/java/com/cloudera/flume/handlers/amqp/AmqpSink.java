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
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * This uses the amqp (Advanced Message Queueing Protocol) java interface
 * provided by RabbitMQ. Currently, all values are hardcoded for local tests.
 * 
 * This is a sink -- it takes messages from a program as input and ships them
 * off via AMQP to the AMQP broker (tcp port 5672).
 * 
 * This currently doesn't seem feasible becuase it has terrible throughput.
 * 
 * TODO (jon) add more options to map user specified priority settings to AMQP
 * messaging settings.
 */
public class AmqpSink extends EventSink.Base {
  Channel channel; // Note channels are *not* thread safe!
  String exchangeName;
  String routingKey;
  String queueName;
  Connection conn;

  public AmqpSink(String exchangeName, String routingKey) throws IOException {
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
    this.queueName = "queue";

  }

  public AmqpSink() throws IOException {
    this("localhost", "routingKey");
  }

  public void append(Event e) throws IOException {
    byte[] messageBodyBytes = (new WriteableEvent(e)).toBytes();

    synchronized (channel) {
      // persistent and immediate (no queueing)
      // channel.basicPublish(exchangeName, routingKey, true, true, //
      // MessageProperties.BASIC, messageBodyBytes);

      channel.basicPublish(exchangeName, routingKey, MessageProperties.BASIC,
          messageBodyBytes);
    }
  }

  public void close() throws IOException {
    synchronized (channel) {
      channel.close();
    }
    conn.close();
  }

  @Override
  public void open() throws IOException {
    ConnectionFactory factory = new ConnectionFactory();
    conn = factory.newConnection("localhost");

    channel = conn.createChannel();
    channel.exchangeDeclare(exchangeName, "direct");
    channel.queueDeclare(queueName);
    channel.queueBind(queueName, exchangeName, routingKey);
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "usage: amqp(exchange, routingkey");
        String exchangeName = argv[0];
        String routingKey = argv[1];
        try {
          return new AmqpSink(exchangeName, routingKey);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Failed to build a ampq sink on exchange " + exchangeName
                  + " with routing key " + routingKey);
        }
      }

    };
  }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin sink.
   */
  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("amqp", builder()));
    return builders;
  }

}
