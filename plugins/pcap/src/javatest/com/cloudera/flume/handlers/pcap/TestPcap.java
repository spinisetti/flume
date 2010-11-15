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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import jpcap.JpcapCaptor;
import jpcap.NetworkInterface;
import jpcap.NetworkInterfaceAddress;
import junit.framework.TestCase;

import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;

/**
 * This tests packet sniffing, and catches live packets. It requires root/admin
 * perms on the network device and network connectivity.
 * 
 */
public class TestPcap extends TestCase {

  /**
   * Dump nic information
   */
  public String nicDump(NetworkInterface nic) {
    StringBuilder b = new StringBuilder();
    // print out its name and description
    b.append(": " + nic.name + "(" + nic.description + ")");

    // print out its datalink name and description
    b.append(" datalink: " + nic.datalink_name + "(" + nic.datalink_description
        + ")");

    // print out its MAC address
    b.append(" MAC address:");
    for (byte m : nic.mac_address)
      b.append(Integer.toHexString(m & 0xff) + ":");
    b.append("\n");

    // print out its IP address, subnet mask and broadcast address
    for (NetworkInterfaceAddress a : nic.addresses)
      b.append(" address:" + a.address + " " + a.subnet + " " + a.broadcast
          + "\n");

    return b.toString();
  }

  public void testNICs() {
    NetworkInterface[] devices = JpcapCaptor.getDeviceList();
    if (devices.length == 0) {
      fail("Need to have permissions to get nic list, (root, Administrator, etc).");
    }
    for (NetworkInterface nic : devices) {
      System.out.println(nicDump(nic));
    }
  }

  /**
   * This uses a heuristic to pick the default network port. It sniffs on port
   * 80 and then grabs http://google.com. At least 10 packets need to be
   * captured to be successful.
   */
  public void testPcapEventSource() throws IOException, InterruptedException {
    NetworkInterface[] devices = JpcapCaptor.getDeviceList();
    if (devices.length == 0) {
      fail("Need to have permissions to get nic list, (root, Administrator, etc).");
    }

    NetworkInterface nic = PCapEventSource.bestGuess();
    if (nic == null) {
      fail("no network devices that I recognize!");
    }

    System.out.println("Picked nic:\n" + nicDump(nic));

    // filter for only tcp packets
    PCapEventSource src = new PCapEventSource(nic, "tcp port 80");
    src.open();
    ConsoleEventSink snk = new ConsoleEventSink();
    snk.open();

    // Get some data from the net on tcp port 80.
    Thread t = new Thread() {
      public void run() {
        try {
          URL url = new URL("http://google.com");
          URLConnection c = url.openConnection();

          InputStreamReader reader = new InputStreamReader(c.getInputStream());
          BufferedReader br = new BufferedReader(reader);
          String s = null;
          while ((s = br.readLine()) != null) {
            System.out.println(s);
          }

        } catch (IOException e) {

        }
      }
    };
    t.start();

    EventUtil.dumpN(10, src, snk);
    t.join();
  }
}
