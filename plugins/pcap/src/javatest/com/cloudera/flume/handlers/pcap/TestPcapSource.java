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
package com.cloudera.flume.handlers.pcap;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeIdException;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SourceFactoryImpl;
import com.cloudera.flume.core.EventSource;

/**
 * Pcap related source stuff has been moved out of core and moved into a
 * "plugin" so that it can be dynamically loaded. There curerntly is a problem
 * with this related to loading jni dlls/so associated with the java.
 */
public class TestPcapSource {

  /**
   * This should fail throwing a flume spec/id exception
   */
  @Test(expected = FlumeIdException.class)
  public void testFailPcapLoad() throws FlumeSpecException {
    FlumeBuilder.buildSource("pcapfile(\"+ExampleData.PCAPFILE1+\")");
  }

  /**
   * This tests that the plugin directory loads jars. This pcap example actually
   * loads a native dll/so which is a problem which requires environment values
   * to be set.
   * 
   * This test will pass if you add ./plugin/pcap/lib to the LD_LIBRARY_PATH
   * (linux) or PATH (windows). It is marked as ingnored for now because this is
   * not generally the case.
   **/
  @Ignore
  @Test
  public void testPcapLoad() throws FlumeSpecException, IOException {
    FlumeConfiguration.get().set(
        FlumeConfiguration.PLUGIN_CLASSES,
        "com.cloudera.flume.handlers.pcap.PCapEventSource,"
            + "com.cloudera.flume.handlers.pcap.PCapFileEventSource");
    FlumeBuilder.setSourceFactory(new SourceFactoryImpl()); // will load plugins
    EventSource src = FlumeBuilder.buildSource("pcapfile(\""
        + ExampleData.PCAPFILE1 + "\")");

    int count = 0;
    src.open();
    while (src.next() != null) {
      count++;
    }
    src.close();
    assertEquals(31711, count);
  }
}
