package org.example;

import static org.example.Tools.getCacheConfig;
import static org.example.Tools.spinWait;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.cache.Cache;
import javax.cache.event.EventType;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DemoListener {
  private static final Logger LOG = LogManager.getLogger();

  private final Ignite ignite;

  public DemoListener(Ignite ignite) {
    this.ignite = ignite;
  }

  public void start() {
    new Thread(this::run, "LSTNR").start();
  }

  private void run() {
    try {
      CacheConfiguration<String, DataA> cacheConfig = getCacheConfig();
      IgniteCache<String, DataA> cache = ignite.getOrCreateCache(cacheConfig);

      // Wait until the cache got some the initial data:
      while (cache.size(CachePeekMode.PRIMARY) < 150)
        spinWait(1_000);

      LOG.info("Running the query . . .");

      ContinuousQuery<String, DataA> continuousQuery = new ContinuousQuery<>();
      continuousQuery.setLocal(false);

      ScanQuery<String, DataA> initialQuery = new ScanQuery<>();
      initialQuery.setLocal(false);
      continuousQuery.setInitialQuery(initialQuery);

      Map<String, Integer> versions = new ConcurrentHashMap<>();

      continuousQuery.setLocalListener(events -> events.forEach(event -> {
        String key = event.getKey();
        DataA value = event.getValue();

        Integer previousVersion = versions.put(key, value.version);
        if (previousVersion == null
          && event.getEventType() != EventType.CREATED)
          LOG.warn("{}: first event was {} with v{}", event.getEventType(), key,
            value.version);

        if (previousVersion != null && previousVersion > value.version)
          LOG.warn("{}: unordered {} with v{} -> v{}", event.getEventType(), key,
            previousVersion, value.version);

        //        LOG.info("Raw event: {} - {} - v{}", event.getEventType(), key,
        //          value.version);
      }));

      QueryCursor<Cache.Entry<String, DataA>> cursor =
        cache.query(continuousQuery);

      for (Cache.Entry<String, DataA> entry : cursor) {
        String key = entry.getKey();
        DataA value = entry.getValue();

        Integer previousVersion = versions.put(key, value.version);
        if (previousVersion != null)
          LOG.warn("{}: expected init with v{}, but there already was v{}", key,
            value.version, previousVersion);

        //        LOG.info("Raw event: INIT - {} - v{}", key, value.version);
      }
    } catch (Throwable err) {
      LOG.error("Error", err);
    }
  }
}
