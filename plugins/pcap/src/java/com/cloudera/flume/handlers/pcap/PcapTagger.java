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
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.rolling.Tagger;

/**
 * This is a tagger that creates unique the service name for batches of pcap
 * captured packet.
 * 
 * TODO (jon) need to solidify a naming convention for these.
 */
public class PcapTagger implements Tagger {
  private static final DateFormat dateFormat = new SimpleDateFormat(
      "yyyyMMdd-HHmmssSSSZ");
  private final static Pattern p = Pattern
      .compile(".*\\.(\\d+-\\d+-\\d+)\\.hdfs");

  public static final String A_FILTER = "pcap_filter";

  Date last;
  String lastTag;
  String filter;

  public PcapTagger(String filter) {
    super();
    this.filter = filter.replace(" ", "_"); // a little bit of escaping
  }

  public String createTag(String name) {

    String f;
    // DateFormats are not thread safe
    synchronized (dateFormat) {
      f = dateFormat.format(new Date());
    }
    if (name.length() > 200) {
      name = name.substring(0, 200); // concatenate long prefixes
    }

    String fname = String.format("%s.0.%s.hdfs", name, f);
    return fname;
  }

  public static Date extractDate(String s) {
    if (s == null)
      return null;

    Matcher m = p.matcher(s);

    if (!m.find()) {
      return null;
    }
    String date = m.group(1);

    // see: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6231579
    synchronized (dateFormat) {
      Date d = dateFormat.parse(date, new ParsePosition(0));
      return d;
    }
  }

  @Override
  public String getTag() {
    return lastTag;
  }

  @Override
  public String newTag() {

    String prefix = filter;
    Date now = new Date();
    String f;
    // DateFormats are not thread safe
    synchronized (dateFormat) {
      f = dateFormat.format(now);
    }
    if (prefix.length() > 200) {
      prefix = prefix.substring(0, 200); // concatenate long prefixes
    }

    lastTag = String.format("%s.0.%s.hdfs", prefix, f);

    this.last = now;
    return lastTag;
  }

  @Override
  public Date getDate() {
    return last;
  }

  @Override
  public void annotate(Event e) {
    e.set(A_FILTER, filter.getBytes());
    e.set(A_TXID, ByteBuffer.allocate(8).putLong(last.getTime()).array());
  }

}
