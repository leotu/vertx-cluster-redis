package io.vertx.spi.cluster.redis.impl;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisMapEventbusTTLMonitor {
	private static final Logger log = LoggerFactory.getLogger(RedisMapEventbusTTLMonitor.class);

	private int refreshIntervalSeconds = 5;
	private final ConcurrentMap<String, Long> resetTTL = new ConcurrentHashMap<>(); // Timer ID

	private final Vertx vertx;
	private final RedisClusterManager clusterManager;
	private final RedisMapEventbus redisMapEventbus;
	// private final RScheduledExecutorService scheduledService;
	private final RMapCache<String, String> mapAsync;

	public RedisMapEventbusTTLMonitor(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson,
			RedisMapEventbus redisMapEventbus, RMapCache<String, String> mapAsync) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(redisMapEventbus, "redisMapEventbus");
		Objects.requireNonNull(mapAsync, "mapAsync");
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.redisMapEventbus = redisMapEventbus;
		this.mapAsync = mapAsync;
		// this.scheduledService = redisson.getExecutorService("haInfo");
	}

	protected void resetTTL(String k) {
		String nodeId = clusterManager.getNodeID();
		resetTTL.computeIfAbsent(k, key -> {
			int intervalSeconds = redisMapEventbus.getTimeToLiveSeconds() / 2 < refreshIntervalSeconds
					? redisMapEventbus.getTimeToLiveSeconds() / 2
					: refreshIntervalSeconds;
			long delay = intervalSeconds * 1000; // milliseconds
			log.debug("*** nodeId={}, k={}, delay={}, intervalSeconds={}", nodeId, k, delay, intervalSeconds);
			if (clusterManager.isInactive()) {
				log.warn("inactive nodeId={}, k={}", nodeId, k);
				return null;
			}
			long timeId = vertx.setPeriodic(delay, id -> {
				// log.debug("nodeId={}, k={}, delay={}, now={}", nodeId, k, delay, new Date());
				if (clusterManager.isInactive()) {
					log.warn("inactive nodeId={}, k={}, id={}", nodeId, k, id);
					vertx.cancelTimer(id);
					resetTTL.remove(k, id);
				} else {
					mapAsync.getAsync(k).whenComplete((v, e) -> {
						if (e != null) {
							log.warn("nodeId={}, k={}, id={}, error={}", nodeId, k, id, e.toString());
							// vertx.cancelTimer(id);
							// resetTTL.remove(k, id);
						} else {
							if (v != null) {
								// log.debug("nodeId={}, k={}, v={}", nodeId, k, v);
								String vAfterPut = mapAsync.put(k, v, redisMapEventbus.getTimeToLiveSeconds(),
										TimeUnit.SECONDS);
								if (!v.equals(vAfterPut)) {
									log.debug("(!v.equals(vAfterPut)), nodeId={}, k={}, v={}, vAfterPut={}", nodeId, k,
											v, vAfterPut);
								}
							} else {
								log.info("nodeId={}, k={}, v is null", nodeId, k);
							}
						}
					});
				}
			});
			log.debug("nodeId={}, k={}, refreshIntervalSeconds={}, delay={}, timeId={}", nodeId, k,
					refreshIntervalSeconds, delay, timeId);
			return null; //timeId;
		});
	}

	public void stop() {
		// scheduledService.shutdown();
		resetTTL.forEach((k, timeId) -> {
			vertx.cancelTimer(timeId);
		});
		resetTTL.clear();
	}

}
