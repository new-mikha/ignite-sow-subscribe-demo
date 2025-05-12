package org.example;

import static org.example.Tools.getCacheConfig;

import java.util.Base64;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DemoWriter {

  private static final Logger LOG = LogManager.getLogger();

  private final Ignite ignite;

  public DemoWriter(Ignite ignite) {
    this.ignite = ignite;
  }

  public void start() {
    new Thread(this::run, "WRTR").start();
  }

  private void run() {
    try {
      CacheConfiguration<String, DataA> cacheConfig = getCacheConfig();
      IgniteCache<String, DataA> cache = ignite.getOrCreateCache(cacheConfig);

      byte[] bytes = new byte[1024];
      for (int i = 0; i < 10_000; i++) {
        randomiseBytes(i, bytes);
        String data = Base64.getEncoder().encodeToString(bytes);

        String key = "key-" + (i % 300);
        DataA value = new DataA(data, i);

        cache.putAsync(key, value);
      }

      LOG.info("Finished writing & updating data WITH CACHE-PUT-ASYNC.");
    } catch (Throwable err) {
      LOG.error("Error", err);
    }
  }

  private static void randomiseBytes(long seed, byte[] bytes) {
    for (int i = 0; i < bytes.length; i++)
      bytes[i] = (byte)(i ^ seed);
  }
}
