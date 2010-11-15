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
import jpcap.NetworkInterface;
import jpcap.packet.Packet;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This connects to the jpcap library (which connects to a native windows .dll
 * or native linux .so via jni calls on the win pcap or pcap api.
 * 
 * Under a generic linux setup this requires root access in order to access the
 * raw network interface.
 * 
 * A decent reference for the filter language syntax is here:
 * http://www.cs.ucr.edu/~marios/ethereal-tcpdump.pdf
 */
public class PCapEventSource extends EventSource.Base {

  JpcapCaptor captor;
  NetworkInterface nic;
  int snaplen = 65535;
  boolean promiscuous = false;
  int to_ms = 200;
  String filter = null;

  /**
   * This selects a likely nic using a heuristic.
   */
  public PCapEventSource(String filter) throws IOException {
    this.filter = filter;
    this.nic = bestGuess();
  }

  /**
   * This requires the user to specify a network interface to sniff packets
   * from, using the jpcap library.
   */
  public PCapEventSource(NetworkInterface nic, String filter) {
    this.nic = nic;
    this.filter = filter;
  }

  /**
   * These are hard coded heuristics that attempt to get a NIC that actually
   * talks to the network.
   * 
   * TODO (jon) is there a better heuristic?
   */
  public static NetworkInterface bestGuess() throws IOException {
    NetworkInterface[] devices = JpcapCaptor.getDeviceList();
    if (devices.length == 0) {
      throw new IOException(
          "Need to have permissions to get nic list, (root, Administrator, etc).");
    }

    NetworkInterface nic = null;
    for (NetworkInterface dev : devices) {
      // take the first so we will have at least something.
      if (nic == null) {
        nic = dev;
      }

      // Keep the first nic that starts with eth or wlan (for linux)
      if (dev.name.startsWith("eth") || dev.name.startsWith("wlan")) {
        nic = dev;
        break;
      }

      // Keep the first nic that starts with en (for mac)
      // TODO (jon) this is untested (also not sure if there is a jni library
      // for mac pcap)
      if (dev.name.startsWith("en")) {
        nic = dev;
        break;
      }

      // Windows has terrible names! For now: Keep the one that has "Driver"
      // (many of the virtual ones say "Adapter" the true one says Driver...
      if (dev.description.contains("Ethernet Driver")) {
        nic = dev;
        break;
      }

    }
    return nic;
  }

  @Override
  public void close() throws IOException {
    captor.close();
  }

  @Override
  public Event next() throws IOException {
    Packet pkt = null;
    while (pkt == null) {
      pkt = captor.getPacket(); // this will timeout as null.
    }

    Event epkt = PacketEventAdaptor.convert(nic.name, pkt);
    return epkt;
  }

  @Override
  public void open() throws IOException {

    if (filter == null || filter.trim().length() == 0) {
      throw new IOException("pcap filter must be specified!");
    }

    captor = JpcapCaptor.openDevice(nic, snaplen, promiscuous, to_ms);
    captor.setFilter(filter, true);

  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length != 2,
            "usage: pcap(nic, filter)");
        // TODO (jon) replace by requiring arg to pick nic
        NetworkInterface nic;
        try {
          nic = bestGuess();
          String filter = argv[1];

          return new PCapEventSource(nic, filter);
        } catch (IOException e) {
          throw new IllegalArgumentException("Unable to open pcap on nic "
              + argv[0] + " with filter " + argv[1]);
        }

      }

    };
  }

  public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
    List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
    builders.add(new Pair<String, SourceBuilder>("pcap", builder()));
    return builders;
  }
}
