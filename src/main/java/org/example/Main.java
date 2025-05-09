package org.example;

import static org.example.Tools.getIgniteConfiguration;

import java.util.Collection;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) {
    Ignite ignite = startIgnite();

    if (getNodesCount(ignite) == 2)
      startListener(ignite);

    if (getNodesCount(ignite) == 3)
      startWriter(ignite);
  }

  private static Ignite startIgnite() {
    IgniteConfiguration cfg = getIgniteConfiguration();

    Ignite ignite = Ignition.getOrStart(cfg);

    ignite.events().localListen((IgnitePredicate<Event>)evt -> {
        LOG.info("{}, current number of nodes: {}", evt.name(),
          getNodesCount(ignite));
        return true;

      }, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT,
      EventType.EVT_NODE_FAILED);

    LOG.info("This node is started, id: {}", ignite.cluster().localNode().id());

    return ignite;
  }

  private static int getNodesCount(Ignite ignite) {
    Collection<BaselineNode> baselineNodes =
      ignite.cluster().currentBaselineTopology();

    if (baselineNodes == null)
      return 0;

    return baselineNodes.size();
  }

  private static void startListener(Ignite ignite) {
    IgniteAtomicLong igniteAtomicLong =
      ignite.atomicLong("sow-subscribe-listener", 0, true);

    if (!igniteAtomicLong.compareAndSet(0, 1))
      throw new RuntimeException(
        "Listener has already started. Restart the whole cluster??");

    new DemoListener(ignite).start();

    LOG.info("Created the listener.");
  }

  private static void startWriter(Ignite ignite) {
    IgniteAtomicLong igniteAtomicLong =
      ignite.atomicLong("sow-subscribe-writer", 0, true);

    if (!igniteAtomicLong.compareAndSet(0, 1))
      throw new RuntimeException(
        "Writer has already started. Restart the whole cluster??");

    new DemoWriter(ignite).start();

    LOG.info("Created the writer.");
  }
}