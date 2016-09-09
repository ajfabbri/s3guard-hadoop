package org.apache.hadoop.fs.s3a.s3guard;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LinkedHashMap that implements a maximum size and LRU eviction policy.
 */
public class LruHashMap<K, V> extends LinkedHashMap<K,V> {
  final int maxSize;
  public LruHashMap(int initialCapacity, int maxSize) {
    super(initialCapacity);
    this.maxSize = maxSize;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() > maxSize;
  }

  /**
   * get() plus side-effect of making the element Most Recently Used.
   * @param key lookup key
   * @return value
   */

  public V mruGet(K key) {
    V val = remove(key);
    if (val != null)
      put(key, val);
    return val;
  }
}
