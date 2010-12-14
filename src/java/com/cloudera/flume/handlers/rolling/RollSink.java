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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This rolling configurations based on a trigger such as a period of time or a
 * certain size of file. When a roll happens, the current instance of the
 * subordinate configuration is closed and a new instance of the sink is opened.
 */
public class RollSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(RollSink.class);

  final String fspec;
  EventSink curSink;
  final RollTrigger trigger;
  protected TriggerThread triggerThread = null;

  private static int threadInitNumber = 0;
  final long checkLatencyMs; // default 4x a second
  private Context ctx; // roll context

  // reporting attributes and counters
  public final static String A_ROLLS = "rolls";
  public final static String A_ROLLFAILS = "rollfails";
  public final String A_ROLLSPEC = "rollspec";
  public final String rollattr;
  public final static String DEFAULT_ROLL_TAG = "rolltag";

  final AtomicLong rolls = new AtomicLong();
  final AtomicLong rollfails = new AtomicLong();

  public RollSink(Context ctx, String spec, long maxAge, long checkMs) {
    this(ctx, spec, maxAge, checkMs, DEFAULT_ROLL_TAG);
  }

  public RollSink(Context ctx, String spec, long maxAge, long checkMs,
      String rolltag) {
    this.ctx = ctx;
    this.rollattr = rolltag;
    this.fspec = spec;
    this.trigger = new TimeTrigger(new ProcessTagger(), maxAge);
    this.checkLatencyMs = checkMs;
    LOG.info("Created RollSink: maxAge=" + maxAge + "ms trigger=[" + trigger
        + "] checkPeriodMs = " + checkLatencyMs + " spec='" + fspec + "'");
  }

  public RollSink(Context ctx, String spec, RollTrigger trigger, long checkMs) {
    this.ctx = ctx;
    this.rollattr = DEFAULT_ROLL_TAG;
    this.fspec = spec;
    this.trigger = trigger;
    this.checkLatencyMs = checkMs;
    LOG.info("Created RollSink: trigger=[" + trigger + "] checkPeriodMs = "
        + checkLatencyMs + " spec='" + fspec + "'");
  }

  private static synchronized int nextThreadNum() {
    return threadInitNumber++;
  }

  /**
   * This thread wakes up to check if a batch should be committed, no matter how
   * small it is.
   */
  class TriggerThread extends Thread {
    final CountDownLatch doneLatch = new CountDownLatch(1);
    final CountDownLatch startedLatch = new CountDownLatch(1);

    TriggerThread() {
      super("Roll-TriggerThread-" + nextThreadNum());
    }

    void doStart() {
      this.start();

      try {
        startedLatch.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for batch timeout thread to start");
      }
    }

    public void run() {
      startedLatch.countDown();
      while (!isInterrupted()) {
        // TODO there should probably be a lcok on Roll sink but until we handle
        // interruptions throughout the code, we cannot because this causes a
        // deadlock
        if (trigger.isTriggered()) {
          trigger.reset();

          LOG.debug("Rotate started by triggerthread... ");
          rotate();
          LOG.debug("Rotate stopped by triggerthread... ");
          continue;
        }

        try {
          Clock.sleep(checkLatencyMs);
        } catch (InterruptedException e) {
          LOG.warn("TriggerThread interrupted");
          doneLatch.countDown();
          return;
        }
      }
      LOG.info("TriggerThread shutdown");
      doneLatch.countDown();
    }
  };

  protected EventSink newSink(Context ctx) throws IOException {
    try {
      // TODO (jon) add roll-specific context information.
      return new CompositeSink(ctx, fspec);
    } catch (FlumeSpecException e) {
      // check done prior to construction.
      throw new IllegalArgumentException("This should never happen:"
          + e.getMessage());
    }
  };

  // This is a large synchronized section. Won't fix until it becomes a problem.
  @Override
  public void append(Event e) throws IOException {
    Preconditions.checkState(curSink != null,
        "Attempted to append when rollsink not open");

    if (trigger.isTriggered()) {
      trigger.reset();
      LOG.debug("Rotate started by append... ");
      rotate();
      LOG.debug("... rotate completed by append.");
    }
    String tag = trigger.getTagger().getTag();

    e.set(rollattr, tag.getBytes());
    synchronized (this) {
      curSink.append(e);
      trigger.append(e);
      super.append(e);
    }
  }

  synchronized public boolean rotate() {
    try {
      rolls.incrementAndGet();
      if (curSink == null) {
        // wtf, was closed or never opened
        LOG.error("Attempting to rotate an already closed roller");
        return false;
      }
      curSink.close();
      curSink = newSink(ctx);
      curSink.open();

      LOG.debug("rotated sink ");
    } catch (IOException e1) {
      // TODO This is an error condition that needs to be handled -- could be
      // due to resource exhaustion.
      LOG.error("Failure when attempting to rotate and open new sink: "
          + e1.getMessage());
      rollfails.incrementAndGet();
      return false;
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing RollSink '" + fspec + "'");

    // TODO triggerThread can race with an open call, but we really need to
    // avoid having the await while locked!
    if (triggerThread != null) {
      try {
        triggerThread.interrupt();
        triggerThread.doneLatch.await();
      } catch (InterruptedException e) {
        LOG
            .warn("Interrupted while waiting for batch timeout thread to finish");
      }
    }

    synchronized (this) {
      if (curSink == null) {
        LOG.info("double close '" + fspec + "'");
        return;
      }
      curSink.close();
      curSink = null;
    }
  }

  @Override
  synchronized public void open() throws IOException {
    Preconditions.checkState(curSink == null,
        "Attempting to open already open RollSink '" + fspec + "'");
    LOG.info("opening RollSink  '" + fspec + "'");
    trigger.getTagger().newTag();
    triggerThread = new TriggerThread();
    triggerThread.doStart();

    try {
      curSink = newSink(ctx);
      curSink.open();
    } catch (IOException e1) {
      LOG.warn("Failure when attempting to open initial sink", e1);
    }
  }

  @Override
  public String getName() {
    return "Roll";
  }

  @Override
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(A_ROLLS, rolls.get());
    rpt.setLongMetric(A_ROLLFAILS, rollfails.get());
    rpt.setStringMetric(A_ROLLSPEC, fspec);
    return rpt;
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    if (curSink != null) {
      curSink.getReports(namePrefix + getName() + ".", reports);
    }
  }

  /**
   * This is currently only used in tests.
   */
  @Deprecated
  public String getCurrentTag() {
    return trigger.getTagger().getTag();
  }

  /**
   * Builder for a spec based rolling sink. (most general version, does not
   * necessarily output to files!).
   */
  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length >= 2 && argv.length <= 3,
            "roll(rollmillis[, checkmillis[, rolltag]]) { sink }");
        String spec = argv[0];
        long rollmillis = Long.parseLong(argv[1]);

        long checkmillis = 250; // TODO (jon) parameterize 250 argument.
        if (argv.length >= 3) {
          checkmillis = Long.parseLong(argv[2]);
        }

        String rolltag = DEFAULT_ROLL_TAG;
        if (argv.length >= 4) {
          rolltag = argv[3];
        }

        try {
          // check sub spec to make sure it works.
          FlumeBuilder.buildSink(ctx, spec);

          // ok it worked, instantiate the roller
          return new RollSink(ctx, spec, rollmillis, checkmillis, rolltag);
        } catch (FlumeSpecException e) {
          throw new IllegalArgumentException("Failed to parse/build " + spec, e);
        }
      }
    };
  }
}
