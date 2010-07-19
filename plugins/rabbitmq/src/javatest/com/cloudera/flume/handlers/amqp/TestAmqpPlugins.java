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

import org.junit.Test;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeIdException;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.cloudera.flume.conf.SourceFactoryImpl;

/**
 */
public class TestAmqpPlugins {

  @Test
  public void testAmqpPoll() throws FlumeSpecException, IOException {
    FlumeConfiguration.get().set(FlumeConfiguration.PLUGIN_CLASSES,
        "com.cloudera.flume.handlers.amqp.AmqpPollingSource");
    FlumeBuilder.setSourceFactory(new SourceFactoryImpl()); // will load plugins
    FlumeBuilder.buildSource("amqpPoll(\"exchange\",\"key\")");
  }

  @Test
  public void testAmqpSub() throws FlumeSpecException, IOException {
    FlumeConfiguration.get().set(FlumeConfiguration.PLUGIN_CLASSES,
        "com.cloudera.flume.handlers.amqp.AmqpSubscriptionSource");
    FlumeBuilder.setSourceFactory(new SourceFactoryImpl()); // will load plugins
    FlumeBuilder.buildSource("amqpSub(\"exchange\",\"key\")");
  }

  @Test
  public void testAmqpSink() throws FlumeSpecException, IOException {
    FlumeConfiguration.get().set(FlumeConfiguration.PLUGIN_CLASSES,
        "com.cloudera.flume.handlers.amqp.AmqpSink");
    FlumeBuilder.setSinkFactory(new SinkFactoryImpl()); // will load plugins
    FlumeBuilder.buildSink("amqp(\"exchange\",\"key\")");
  }

  @Test(expected = FlumeIdException.class)
  public void testAmqpPollFail() throws FlumeSpecException, IOException {
    FlumeConfiguration.get().set(FlumeConfiguration.PLUGIN_CLASSES, "");
    FlumeBuilder.setSourceFactory(new SourceFactoryImpl()); // will load plugins
    FlumeBuilder.buildSource("amqpPoll(\"exchange\",\"key\")");
  }

  @Test(expected = FlumeIdException.class)
  public void testAmqpSubFail() throws FlumeSpecException, IOException {
    FlumeConfiguration.get().set(FlumeConfiguration.PLUGIN_CLASSES, "");
    FlumeBuilder.setSourceFactory(new SourceFactoryImpl()); // will load plugins
    FlumeBuilder.buildSource("amqpSub(\"exchange\",\"key\")");
  }

  @Test(expected = FlumeIdException.class)
  public void testAmqpSinkFail() throws FlumeSpecException, IOException {
    FlumeConfiguration.get().set(FlumeConfiguration.PLUGIN_CLASSES, "");
    FlumeBuilder.setSinkFactory(new SinkFactoryImpl()); // will load plugins
    FlumeBuilder.buildSink("amqp(\"exchange\",\"key\")");
  }

}
