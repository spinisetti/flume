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
package com.cloudera.flume;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.agent.LogicalNodeManager;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;

@Path("/logicalnodes")
public class LogicalNodeManagerResource {
  private final LogicalNodeManager lns;

  /**
   * Must be public for jersey to instantiate and process.
   */
  public LogicalNodeManagerResource() {
    this.lns = FlumeNode.getInstance().getLogicalNodeManager();
  }

  @GET
  @Path("nodes")
  @Produces("text/html")
  public String getOldReport() {
    return lns.getReport().toHtml();
  }

  /**
   * Wrapper class to provide XmlRootElement.
   */
  @XmlRootElement
  public static class LogicalNodes {
    public Collection<LogicalNode> nodes;

    public LogicalNodes() {
      nodes = new ArrayList<LogicalNode>();
    }

    public LogicalNodes(Collection<LogicalNode> cfgs) {
      this.nodes = cfgs;
    }
  }

  // public JAXBElement<Reportable> getLogicalNode(Reportable r) {
  // return new JAXBElement<Reportable>(new QName("reportable"),
  // Reportable.class, r);
  // }

  public static class ReportableAdapter extends
      XmlAdapter<Reportable, ReportEventType> {

    public ReportableAdapter() {

    }

    @Override
    public Reportable marshal(ReportEventType rpt) throws Exception {
      // This is read only, so we won't need to marshall
      return null;
    }

    @Override
    public ReportEventType unmarshal(Reportable r) throws Exception {
      if (r == null) {
        return null;
      }

      ReportEvent rpt = r.getMetrics(); // TODO check if this breaks external dependencies
      ReportEventType ret = new ReportEventType();
      ret.longEntries = new ArrayList<LongEntry>();
      for (Entry<String, Long> e : rpt.getAllLongMetrics().entrySet()) {
        ret.longEntries.add(new LongEntry(e.getKey(), e.getValue()));
      }

      ret.doubleEntries = new ArrayList<DoubleEntry>();
      for (Entry<String, Double> e : rpt.getAllDoubleMetrics().entrySet()) {
        ret.doubleEntries.add(new DoubleEntry(e.getKey(), e.getValue()));
      }

      ret.stringEntries = new ArrayList<StringEntry>();
      for (Entry<String, String> e : rpt.getAllStringMetrics().entrySet()) {
        ret.stringEntries.add(new StringEntry(e.getKey(), e.getValue()));
      }
      return ret;
    }

  }

  @XmlRootElement
  public static class ReportEventType {
    public List<LongEntry> longEntries;
    public List<DoubleEntry> doubleEntries;
    public List<StringEntry> stringEntries;
  }

  static class DoubleEntry {
    @XmlAttribute
    public String key;
    @XmlValue
    public Double val;

    DoubleEntry(String k, Double v) {
      this.key = k;
      this.val = v;
    }
  }

  static class StringEntry {
    @XmlAttribute
    public String key;
    @XmlValue
    public String val;

    StringEntry(String k, String v) {
      this.key = k;
      this.val = v;
    }
  }

  static class LongEntry {
    @XmlAttribute
    public String key;
    @XmlValue
    public Long val;

    LongEntry(String k, Long v) {
      this.key = k;
      this.val = v;
    }
  }

  // @Provider
  // public static class LogicalNodeJAXBContextProvider implements
  // ContextResolver<JAXBContext> {
  // private JAXBContext context = null;
  // @Override
  // public JAXBContext getContext(Class<?> cls) {
  // if (cls != LogicalNode.class) {
  // return null;
  // }
  // if (context == null) {
  // try {
  // context = JAXBContext.newInstance(LogicalNode.class);
  //          
  // }
  // }
  // }
  //
  // };

  // The Java method will process HTTP GET requests
  @GET
  @Produces("application/json")
  public LogicalNodes configs() {
    return new LogicalNodes(lns.getNodes());
    // LogicalNodes nodes = new LogicalNodes();
    // for (Reportable r : lns.getNodes()) {
    // nodes.nodes.add(r);
    // }
    // return nodes;
  }

  /**
   * Sub path for only getting data specific for a partricular node.
   * 
   * @param node
   * @return
   */
  @GET
  @Path("{node}")
  @Produces("application/json")
  public @XmlJavaTypeAdapter(ReportableAdapter.class)
  LogicalNode getConfig(@PathParam("node") String node) {
    LogicalNode ln = lns.get(node);
    // return ln.getReport();
    // return ln.getName();
    return ln;
  }

}
