package com.cloudera.flume.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Driver.DriverState;
import com.cloudera.flume.core.connector.DirectDriver;

public class TestDriverSemantics {

  @Test
  public void testDriver() throws FlumeSpecException, IOException {
    Context ctx = new Context();
    EventSink snk = FlumeBuilder.buildSink(ctx, "null");
    EventSource src = FlumeBuilder.buildSource("null");
    Driver d = new DirectDriver("test", src, snk);
    assertEquals(DriverState.HELLO, d.getState());
    d.start();
    assertEquals(DriverState.RUNNING, d.getState());
    d.stop();

    assertEquals(DriverState.STOPPED, d.getState());

  }
}
