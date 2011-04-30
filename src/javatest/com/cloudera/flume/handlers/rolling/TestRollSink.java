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
package com.cloudera.flume.handlers.rolling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Level;
import org.codehaus.jettison.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.EscapedCustomDfsSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportTestUtils;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

public class TestRollSink {
  public static final Logger LOG = LoggerFactory.getLogger(TestRollSink.class);

  @Before
  public void setDebug() {
    // log4j specific debugging level
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  /**
   * Tests that the rolling event sink correctly tags the output filename.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testEscapedFilenameCloseFlushes() throws IOException,
      InterruptedException {
    Tagger tagger = new ProcessTagger() {
      @Override
      public String getTag() {
        return "-testtag";
      }

      @Override
      public String newTag() {
        return "-testtag";
      }
    };
    final File f = FileUtil.mktempdir();
    RollSink snk = new RollSink(new Context(), "test", new TimeTrigger(tagger,
        10000), 250) {
      @Override
      protected EventSink newSink(Context ctx) throws IOException {
        return new EscapedCustomDfsSink("file:///" + f.getPath(),
            "sub-%{service}%{rolltag}");
      }
    };

    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();
    File fo = new File(f.getPath() + "/sub-foo-testtag");
    assertTrue(fo.exists());
    FileUtil.rmr(f);
  }

  /**
   * Tests that the rolling event sink correctly tags the output filename.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testEscapedFilename() throws IOException, InterruptedException {
    Tagger tagger = new ProcessTagger() {
      @Override
      public String getTag() {
        return "-testtag";
      }

      @Override
      public String newTag() {
        return "-testtag";
      }
    };
    final File f = FileUtil.mktempdir();
    RollSink snk = new RollSink(new Context(), "escapedCustomDfs(\"file:///"
        + f.getPath() + "\", \"sub-%{service}%{rolltag}\")", new TimeTrigger(
        tagger, 10000), 250);

    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();
    File fo = new File(f.getPath() + "/sub-foo-testtag");
    assertTrue(fo.exists());
    FileUtil.rmr(f);
  }

  @Test
  public void testAutoRoll() throws IOException, InterruptedException {
    RollSink snk = new RollSink(new ReportTestingContext(), "counter(\"foo\")",
        2000, 10000); // two
    // second sleeper, but check period is really long

    Event e = new EventImpl("this is a test message".getBytes());
    snk.open();
    snk.append(e);
    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("foo");
    Clock.sleep(3000); // sleep 3s

    // the roller automatically flushed!
    assertEquals(1, cnt.getCount());
    snk.close();
  }

  /**
   * This verifies that the roller's trigger works multiple times, and at about
   * the right frequency.
   */
  @Test
  public void testMultiTimedRoll() throws IOException, InterruptedException {
    RollSink snk = new RollSink(new ReportTestingContext(), "counter(\"foo\")",
        200, 100);
    // 200 ms auto forced roll threshold.

    snk.open();
    Clock.sleep(100); // sleep until about 100 ms; no flush yet.
    assertEquals(Long.valueOf(0), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    Clock.sleep(200); // auto flush
    assertEquals(Long.valueOf(1), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    Clock.sleep(200); // auto flush.
    assertEquals(Long.valueOf(2), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    Clock.sleep(200); // auto flush.
    assertEquals(Long.valueOf(3), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));
    snk.close();
  }

  /**
   * This verifies that the roller's trigger works multiple times, and at about
   * the right frequency.
   */
  @Test
  public void testMultiCountRoll() throws IOException, InterruptedException {
    RollSink snk = new RollSink(new ReportTestingContext(), "counter(\"foo\")",
        new SizeTrigger(10, new ProcessTagger()), 100);
    // every 10 body-bytes we should roll

    snk.open();

    assertEquals(Long.valueOf(0), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    // a 10 byte body
    Event e = new EventImpl("0123456789".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(1), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    // 5 bytes (no trigger)
    e = new EventImpl("01234".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(1), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));
    // 5 more bytes (ok trigger)
    e = new EventImpl("01234".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(2), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    // 27 bytes but only on trigger
    e = new EventImpl("012345678901234567890123456".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(3), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    // 5 bytes (no trigger)
    e = new EventImpl("01234".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(3), snk.getMetrics().getLongMetric(
        RollSink.A_ROLLS));

    snk.close();
  }

  /**
   * Test metrics
   * 
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testGetRollMetrics() throws JSONException, FlumeSpecException,
      IOException, InterruptedException {
    ReportTestUtils.setupSinkFactory();

    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "roll(100) { one } ");
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLFAILS));
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLS));
    assertEquals("one", rpt.getStringMetric(RollSink.A_ROLLSPEC));
    assertNull(rpt.getStringMetric("One.name"));

    // need to open to have sub sink show up
    snk.open();

    ReportEvent all = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(all).toString());
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLFAILS));
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLS));
    assertEquals("one", rpt.getStringMetric(RollSink.A_ROLLSPEC));
    assertEquals("One", all.getStringMetric("One.name"));

    snk.close();
  }

  @Test
  public void testNPEInRoll() throws IOException, InterruptedException {
    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("appendNPE", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return new EventSink.Base() {
          public String getName() {
            return "appendNPE";
          }

          public void append(Event e) {
            throw new NullPointerException("force NPE");
          }
        };
      }
    });
    FlumeBuilder.setSinkFactory(sf);
    RollSink snk = new RollSink(new ReportTestingContext(), "appendNPE",
        new SizeTrigger(10, new ProcessTagger()), 100);
    snk.open();
    snk.append(new EventImpl("dummy".getBytes()));
    snk.close();
  }

  @Test
  public void testInterruptInRoll() throws IOException, InterruptedException {
    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("appendInterrupt", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return new EventSink.Base() {
          public String getName() {
            return "appendInterrupt";
          }

          public void append(Event e) throws InterruptedException {
            throw new InterruptedException("force Interrupted Exception");
          }
        };
      }
    });
    FlumeBuilder.setSinkFactory(sf);

    RollSink snk = new RollSink(new ReportTestingContext(), "counter(\"foo\")",
        new SizeTrigger(10, new ProcessTagger()), 100);
    snk.open();
    snk.append(new EventImpl("dummy".getBytes()));
    snk.close();
  }

  @Test
  public void testIllegalStateInRoll() throws IOException, InterruptedException {
    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("appendIllegalException", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return new EventSink.Base() {
          public String getName() {
            return "appendIllegalException";
          }

          public void append(Event e) throws InterruptedException {
            throw new IllegalStateException("force Runtime Exception");
          }
        };
      }
    });
    FlumeBuilder.setSinkFactory(sf);

    RollSink snk = new RollSink(new ReportTestingContext(),
        "appendIllegalException", new SizeTrigger(10, new ProcessTagger()), 100);
    snk.open();
    snk.append(new EventImpl("dummy".getBytes()));
    snk.close();
  }
}
