
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

	protected final Vertx vertx;
	protected final RMap<K, V> map;

	public RedisMap(Vertx vertx, RMap<K, V> map) {
		Objects.requireNonNull(map, "map");
		this.vertx = vertx;
		this.map = map;
	}

	public RedisMap(Vertx vertx, RedissonClient redisson, String name) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.vertx = vertx;
		this.map = redisson.getMap(name);
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
		// "map.keySet()" <b>DOESN'T</b> fetch all of them as {@link #readAllKeySet()} does.
		return map.readAllKeySet();
	}

	@Override
	public Collection<V> values() {
		// "map.values()" <b>DOESN'T</b> fetch all of them as {@link #readAllValues()} does.
		return map.readAllValues();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		// "map.entrySet()" <b>DOESN'T</b> fetch all of them as {@link #readAllEntrySet()} does.
		return map.readAllEntrySet();
	}
}
