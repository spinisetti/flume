package com.cloudera.flume.core;

public class InterruptSinks {
  public static EventSink appendSink() {
    return new EventSink.Base() {
      public void append(Event e) throws InterruptedException {
        throw new InterruptedException("Interrupt");
      }
    };
  }

  public static EventSink openSink() {
    return new EventSink.Base() {
      public void open() throws InterruptedException {
        throw new InterruptedException("Interrupt");
      }
    };

  }

  public static EventSink closeSink() {
    return new EventSink.Base() {
      boolean open;

      public void open() {
        open = true;
      }

      public void close() throws InterruptedException {
        if (open) {
          open = false;
          throw new InterruptedException("Interrupt");
        }
        open = false;

      }
    };

  }
}
