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
import java.net.InetAddress;

import jpcap.JpcapCaptor;
import jpcap.packet.Packet;
import jpcap.packet.UDPPacket;
import junit.framework.TestCase;

import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This is using the jpcap library for reading .pcap files.
 * 
 * TODO (jon) This currently fails on hudson because the jni dll is built for
 * 64bit x86 code here and the hudson box is a 32bit x86 machine.
 */
public class TestPcapReader extends TestCase {
  public static final String COD4_SAMPLE = "src/javatest/data/cod4-090804.pcap";

  public void testPcapRead() throws IOException {
    // open a file to read saved packets
    JpcapCaptor captor = JpcapCaptor.openFile(COD4_SAMPLE);

    while (true) {
      // read a packet from the opened file
      Packet packet = captor.getPacket();
      // if some error occurred or EOF has reached, break the loop
      if (packet == null || packet == Packet.EOF)
        break;
      // otherwise, print out the packet
      System.out.println(packet);
    }

    captor.close();
  }

  public String toReadable(byte[] bs) {
    StringBuilder bld = new StringBuilder();
    for (byte b : bs) {
      if (b < 32 || b >= 127) {
        bld.append('.');
      } else {
        bld.append(new Character((char) b));
      }
    }
    return bld.toString();
  }

  public void testCod4Read() throws IOException {
    // open a file to read saved packets
    JpcapCaptor captor = JpcapCaptor.openFile(COD4_SAMPLE);

    // These IPs are in the data set (not hard coded to this this particular
    // machine)
    // local machine is:
    InetAddress local = InetAddress.getByName("192.168.42.101");
    InetAddress svr = InetAddress.getByName("192.168.42.19");

    while (true) {
      // read a packet from the opened file
      Packet packet = captor.getPacket();
      // if some error occurred or EOF has reached, break the loop
      if (packet == null || packet == Packet.EOF)
        break;

      if (packet instanceof UDPPacket) {
        UDPPacket udp = (UDPPacket) packet;
        // otherwise, print out the packet
        byte[] data = udp.data;

        if (udp.src_port != 28960)
          continue;

        // only deal with packets from this client.
        // if (udp.dst_ip.equals(local))
        if (udp.dst_ip.equals(svr))
          continue;

        // if (udp.src_ip.equals(""))
        System.out.println(udp);
        System.out.printf("%10d", data.length);
        for (int i = 0; i < data.length; i++) {
          if (i % 8 == 0)
            System.out.print(" ");

          System.out.printf("%02x", data[i]);
        }
        System.out.printf("\n%10s %s\n", "", toReadable(data));
      }

    }

    captor.close();
  }

  public void testPcapSource() throws IOException {
    EventSource src = new PCapFileEventSource(COD4_SAMPLE);
    src.open();
    CounterSink snk = new CounterSink("packets");
    snk.open();
    EventUtil.dumpAll(src, snk);
    System.out.println(snk.getCount());
    assertEquals(148638, snk.getCount());
  }

}
