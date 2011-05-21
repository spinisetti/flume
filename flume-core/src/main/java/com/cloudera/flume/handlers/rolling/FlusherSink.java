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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.EventSink;
import com.google.common.base.Preconditions;

/**
 * This rolling configurations based on a trigger such as a period of time or a
 * certain size of file. When a roll happens, the current instance of the
 * subordinate configuration is closed and a new instance of the sink is opened.
 */
public class FlusherSink extends RollSink {  
  static final Logger LOG = LoggerFactory.getLogger(FlusherSink.class);

  public FlusherSink(Context ctx, String spec, long maxAge, long checkMs) {
    super(ctx, spec, maxAge, checkMs);
  }

  synchronized public boolean rotate() throws InterruptedException {
    try {
      rolls.incrementAndGet();
      if (curSink == null) {
        // was closed or never opened
        LOG.error("Attempting to rotate an already closed roller");
        return false;
      }
      curSink.flush();

      LOG.debug("flushed sink ");
    } catch (IOException e1) {
      // TODO This is an error condition that needs to be handled -- could be
      // due to resource exhaustion.
      LOG.error("Failure when attempting to rotate and open new sink: "
          + e1.getMessage());
      return false;
    }
    return true;
  }

  @Override
  public String getName() {
    return "FlushRoll";
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
            "flusher(rollmillis[, checkmillis]) { sink }");
        String spec = argv[0];
        long rollmillis = Long.parseLong(argv[1]);

        long checkmillis = 250; // TODO (jon) parameterize 250 argument.
        if (argv.length >= 3) {
          checkmillis = Long.parseLong(argv[2]);
        }

        try {
          // check sub spec to make sure it works.
          FlumeBuilder.buildSink(ctx, spec);

          // ok it worked, instantiate the roller
          return new FlusherSink(ctx, spec, rollmillis, checkmillis);
        } catch (FlumeSpecException e) {
          throw new IllegalArgumentException("Failed to parse/build " + spec, e);
        }
      }
    };
  }
}
