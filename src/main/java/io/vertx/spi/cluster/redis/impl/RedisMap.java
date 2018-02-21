
package io.vertx.spi.cluster.redis.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import io.vertx.core.Vertx;

/**
 * @see org.redisson.codec.JsonJacksonCodec
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisMap<K, V> implements Map<K, V> {
	// private static final Logger log = LoggerFactory.getLogger(RedisMap.class);

	protected final RMap<K, V> map;

	public RedisMap(Vertx vertx, RMap<K, V> map) {
		Objects.requireNonNull(map, "map");
		this.map = map;
	}

	public RedisMap(Vertx vertx, RedissonClient redisson, String name) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.map = redisson.getMap(name);
		// log.debug("name={}", name);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		return map.put(key, value);
	}

	@Override
	public V remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return map.entrySet();
	}
}
