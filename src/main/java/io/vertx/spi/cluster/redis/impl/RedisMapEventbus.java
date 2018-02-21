
package io.vertx.spi.cluster.redis.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisMapEventbus extends RedisMap<String, String> {
	private static final Logger log = LoggerFactory.getLogger(RedisMapEventbus.class);

	private int timeToLiveSeconds = 10; // TTL seconds

	private final RedisClusterManager clusterManager;
	private final RMapCache<String, String> mapAsync;
	private final RedisMapEventbusTTLMonitor ttlMonitor;

	private int removedListeneId;
	private int expiredListenerId;
	private int createdListenerId;

	public RedisMapEventbus(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson, String name) {
		super(vertx, redisson.getMapCache(name));
		this.clusterManager = clusterManager;
		this.mapAsync = (RMapCache<String, String>) map;
		this.ttlMonitor = new RedisMapEventbusTTLMonitor(vertx, this.clusterManager, redisson, this, this.mapAsync);
		log.debug("name={}", name);
	}

	protected int getTimeToLiveSeconds() {
		return timeToLiveSeconds;
	}

	@Override
	public int size() {
		int size = super.size();
		log.debug("size={}", size);
		return size;
	}

	@Override
	public boolean isEmpty() {
		boolean isEmpty = super.isEmpty();
		log.debug("isEmpty={}", isEmpty);
		return isEmpty;
	}

	@Override
	public boolean containsKey(Object key) {
		boolean containsKey = super.containsKey(key);
		log.debug("key={}, containsKey={}", key, containsKey);
		return containsKey;
	}

	@Override
	public boolean containsValue(Object value) {
		boolean containsValue = super.containsValue(value);
		log.debug("value={}, containsValue={}", value, containsValue);
		return containsValue;
	}

	@Override
	public String get(Object key) {
		String value = super.get(key);
		log.debug("key={}, value={}", key, value);
		return value;
	}

	@Override
	public String put(String key, String value) {
		// Thread.dumpStack();
		try {
			String previous = mapAsync.put(key, value, timeToLiveSeconds, TimeUnit.SECONDS);
			ttlMonitor.resetTTL(key);
			log.debug("key={}, value={}, previous={}", key, value, previous);
			return previous;
		} catch (Exception e) {
			log.warn("retry: k=" + key + ", v=" + value, e);
			// throw new RuntimeException("k=" + key + ", v=" + value, e);
			String previous = super.put(key, value);
			log.debug("key={}, value={}, previous={}", key, value, previous);
			return previous;
		}
	}

	@Override
	public String remove(Object key) {
		// Thread.dumpStack();
		String deletedValue = super.remove(key);
		log.debug("key={}, deletedValue={}", key, deletedValue);
		return deletedValue;
	}

	@Override
	public void putAll(Map<? extends String, ? extends String> m) {
		log.debug("m={}", m);
		super.putAll(m);
	}

	@Override
	public void clear() {
		log.debug("...");
		super.clear();
	}

	@Override
	public Set<String> keySet() {
		Set<String> keySet = super.keySet();
		log.debug("keySet.size={}", keySet.size());
		return keySet;
	}

	@Override
	public Collection<String> values() {
		Collection<String> values = super.values();
		log.debug("values={}", values);
		return values;
	}

	@Override
	public Set<Entry<String, String>> entrySet() {
		Set<Entry<String, String>> entrySet = super.entrySet();
		log.debug("entrySet.size={}", entrySet.size());
		return entrySet;
	}

	public void attachListener(NodeListener nodeListener) {
		this.removedListeneId = mapAsync.addListener(new EntryRemovedListener<String, String>() {
			@Override
			public void onRemoved(EntryEvent<String, String> event) {
				Boolean sameSource = event.getSource().equals(mapAsync);
				if (!sameSource) {
					log.debug(
							"listenerId={}, sameSource={}, removed key.class={}, key={}, removed value.class={}, value={}",
							removedListeneId, sameSource, event.getKey().getClass().getName(), event.getKey(),
							event.getValue().getClass().getName(), event.getValue());
				}
				String nodeID = event.getKey().toString();
				log.debug("removed(nodeLeft): nodeID={}", nodeID);
				nodeListener.nodeLeft(nodeID);
			}
		});

		//
		this.expiredListenerId = mapAsync.addListener(new EntryExpiredListener<String, String>() {
			@Override
			public void onExpired(EntryEvent<String, String> event) {
				Boolean sameSource = event.getSource().equals(mapAsync);
				if (!sameSource) {
					log.debug(
							"listenerId={},sameSource={}, expired key.class={}, key={}, expired value.class={}, value={}",
							expiredListenerId, sameSource, event.getKey().getClass().getName(), event.getKey(),
							event.getValue().getClass().getName(), event.getValue());
				}
				String nodeID = event.getKey().toString();
				log.debug("expired(nodeLeft): nodeID={}", nodeID);
				nodeListener.nodeLeft(nodeID);
			}
		});

		//
		this.createdListenerId = mapAsync.addListener(new EntryCreatedListener<String, String>() {
			@Override
			public void onCreated(EntryEvent<String, String> event) {
				Boolean sameSource = event.getSource().equals(mapAsync);
				if (!sameSource) {
					log.debug(
							"listenerId={},sameSource={}, created key.class={}, key={}, created value.class={}, value={}",
							createdListenerId, sameSource, event.getKey().getClass().getName(), event.getKey(),
							event.getValue().getClass().getName(), event.getValue());
				}
				String nodeID = event.getKey().toString();
				log.debug("created(nodeAdded): nodeID={}}", nodeID);
				nodeListener.nodeAdded(nodeID);
			}
		});
	}

	public void stop() {
		try {
			mapAsync.removeListener(removedListeneId);
			mapAsync.removeListener(expiredListenerId);
			mapAsync.removeListener(createdListenerId);
		} catch (Exception e) {
			log.warn(e.toString());
		} finally {
			ttlMonitor.stop();
		}
	}
}
