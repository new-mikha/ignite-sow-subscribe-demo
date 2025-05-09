package org.example;

import static org.example.Tools.getCacheConfig;

import java.util.Base64;
import java.util.Collection;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.stream.StreamReceiver;
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

      try (IgniteDataStreamer<String, DataA> streamer = ignite.dataStreamer(
        cacheConfig.getName()))
      {

        streamer.allowOverwrite(true);
        streamer.keepBinary(true);
        streamer.skipStore(true);
        streamer.autoFlushFrequency(1_000);

        // Need to cast it to untyped - while the streamer itself is typed
        // for <String, DataA>, it also has keepBinary(true), so the receiver
        // actually gets <BinaryObject, BinaryObject> pairs:
        streamer.receiver((StreamReceiver)DemoWriter::receive);

        byte[] bytes = new byte[1024];
        for (int i = 0; i < 10_000; i++) {
          randomiseBytes(i, bytes);
          String data = Base64.getEncoder().encodeToString(bytes);

          String key = "key-" + (i % 300);
          DataA value = new DataA(data, i);

          streamer.addData(key, value);

          //spinWait(1_000);
        }
      }

      LOG.info("Finished writing & updating data.");
    } catch (Throwable err) {
      LOG.error("Error", err);
    }
  }

  private static void randomiseBytes(long seed, byte[] bytes) {
    for (int i = 0; i < bytes.length; i++)
      bytes[i] = (byte)(i ^ seed);
  }

  private static void receive(IgniteCache<BinaryObject, BinaryObject> cache,
    Collection<Map.Entry<BinaryObject, BinaryObject>> entries)
  {
    for (Map.Entry<BinaryObject, BinaryObject> e : entries) {
      cache.invokeAsync(e.getKey(), (entry, args) -> {
        entry.setValue(e.getValue());
        return null;
      });
    }
  }

}
