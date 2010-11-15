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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jpcap.JpcapCaptor;
import jpcap.packet.Packet;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;

/**
 * This reads a Pcap file saved off by an application such as wireshark.
 */
public class PCapFileEventSource extends EventSource.Base {
  // open a file to read saved packets
  String fname;
  JpcapCaptor captor;

  public PCapFileEventSource(String fname) {
    this.fname = fname;
  }

  @Override
  public void close() throws IOException {
    captor.close();
  }

  @Override
  public Event next() throws IOException {
    // read a packet from the opened file
    Packet packet = this.captor.getPacket();
    // if some error occurred or EOF has reached, break the loop
    if (packet == null || packet == Packet.EOF)
      return null;

    return PacketEventAdaptor.convert(fname, packet);

  }

  @Override
  public void open() throws IOException {
    this.captor = JpcapCaptor.openFile(fname);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        if (argv.length != 1) {
          throw new IllegalArgumentException("usage: pcapfile(filename)");
        }

        return new PCapFileEventSource(argv[0]);
      }

    };
  }

  public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
    List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
    builders.add(new Pair<String, SourceBuilder>("pcapfile", builder()));
    return builders;
  }
}
