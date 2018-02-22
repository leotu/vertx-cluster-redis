
package io.vertx.spi.cluster.redis.impl;

import java.util.concurrent.TimeUnit;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * CLUSTER_MAP_NAME = "__vertx.haInfo"
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisMapHaInfo extends RedisMap<String, String> {
	private static final Logger log = LoggerFactory.getLogger(RedisMapHaInfo.class);

	private int timeToLiveSeconds = 10; // TTL seconds

	private final RedisClusterManager clusterManager;
	private final RMapCache<String, String> mapAsync;
	private final RedisMapHaInfoTTLMonitor ttlMonitor;

	public RedisMapHaInfo(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson, String name) {
		super(vertx, redisson.getMapCache(name));
		this.clusterManager = clusterManager;
		this.mapAsync = (RMapCache<String, String>) map;
		this.ttlMonitor = new RedisMapHaInfoTTLMonitor(vertx, this.clusterManager, redisson, this);
	}

	protected RMapCache<String, String> getMapAsync() {
		return mapAsync;
	}

	protected int getTimeToLiveSeconds() {
		return timeToLiveSeconds;
	}

	public void setTimeToLiveSeconds(int timeToLiveSeconds) {
		this.timeToLiveSeconds = timeToLiveSeconds;
	}

	public void attachListener(NodeListener nodeListener) {
		ttlMonitor.attachListener(nodeListener);
	}

	public void close() {
		ttlMonitor.stop();
	}

	/**
	 * @return previous
	 */
	@Override
	public String put(String key, String value) {
		try {
			return mapAsync.put(key, value, timeToLiveSeconds, TimeUnit.SECONDS);
		} catch (Exception e) {
			String previous = super.put(key, value);
			log.warn("retry: key={}, value={}, previous={} when error={}", key, value, previous, e.toString());
			return previous;
		}
	}
}
