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

import java.nio.ByteBuffer;

import jpcap.packet.IPPacket;
import jpcap.packet.Packet;
import jpcap.packet.UDPPacket;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventBaseImpl;
import com.cloudera.util.NetUtils;

/**
 * This is a wrapper for Packets and put a event interface on them.
 * 
 * TODO (jon) This is only really focused on the udp packets.
 */
public class PacketEventAdaptor extends EventBaseImpl {

  Packet p;
  long nanos;

  public PacketEventAdaptor(String source, Packet p) {
    this.p = p;
    this.nanos = System.nanoTime();

    set("service", source.getBytes());

    if (p instanceof UDPPacket) {
      UDPPacket udp = (UDPPacket) p;
      set("dst_port", ByteBuffer.allocate(4).putInt(udp.dst_port).array());
      set("src_port", ByteBuffer.allocate(4).putInt(udp.src_port).array());
    }

    if (p instanceof IPPacket) {
      // TODO(jon) I'm ignoring all the other fields in the packet.
      IPPacket ip = (IPPacket) p;
      set("dst_ip", ip.dst_ip.getAddress());
      set("src_ip", ip.src_ip.getAddress());
    }

  }

  public static Event convert(String source, Packet packet) {
    return new PacketEventAdaptor(source, packet);
  }

  @Override
  public byte[] getBody() {
    return p.data;
  }

  @Override
  public String getHost() {
    return NetUtils.localhost();
  }

  @Override
  public long getNanos() {
    return nanos;
  }

  @Override
  public Priority getPriority() {
    return Priority.INFO;
  }

  @Override
  public long getTimestamp() {
    // TODO (jon) figure out if use is just the usec part or includes seconds.
    return p.usec / 1000;
  }

}
