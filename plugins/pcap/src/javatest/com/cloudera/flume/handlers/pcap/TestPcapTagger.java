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

import java.util.regex.Pattern;

import junit.framework.TestCase;

/**
 * This is a quick test that when run shows some tags (doesn't actually test).
 */
public class TestPcapTagger extends TestCase {
  Pattern p = Pattern.compile("tcp_port_31337.\\d+.\\d+-\\d+-\\d+.hdfs");

  public void testPcapTagger() {

    PcapTagger pcap = new PcapTagger("tcp port 31337");

    pcap.newTag();
    System.out.println(pcap.getTag());
    assertTrue(p.matcher(pcap.getTag()).matches());

    pcap.newTag();
    System.out.println(pcap.getTag());
    assertTrue(p.matcher(pcap.getTag()).matches());

    pcap.newTag();
    System.out.println(pcap.getTag());
    assertTrue(p.matcher(pcap.getTag()).matches());
  }
}
