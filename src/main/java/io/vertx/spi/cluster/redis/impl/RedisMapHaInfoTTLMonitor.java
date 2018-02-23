package io.vertx.spi.cluster.redis.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryEvent.Type;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * TTL: "__vertx.haInfo"
 * 
 * @see org.redisson.api.RScheduledExecutorService
 * @see org.redisson.api.RedissonClient#getExecutorService
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisMapHaInfoTTLMonitor {
	private static final Logger log = LoggerFactory.getLogger(RedisMapHaInfoTTLMonitor.class);

	static private boolean debug = false;

	private int refreshIntervalSeconds = 5;

	// <nodeId, Timer ID>
	private final ConcurrentMap<String, Long> resetTTL = new ConcurrentHashMap<>();

	private final Vertx vertx;
	private final RedisClusterManager clusterManager;
	private final RedisMapHaInfo redisMapHaInfo;
	private final RMapCache<String, String> mapAsync;

	private int removedListeneId;
	private int expiredListenerId;
	private int createdListenerId;
	private int updatedListenerId;

	private EntryCreatedListener<String, String> nodeCreatedNofity; // join?

	private boolean syncSubs = false;
	private final AtomicReference<Date> faultTimeMaker = new AtomicReference<>();

	public RedisMapHaInfoTTLMonitor(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson,
			RedisMapHaInfo redisMapHaInfo) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(redisMapHaInfo, "redisMapHaInfo");
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.redisMapHaInfo = redisMapHaInfo;
		this.mapAsync = redisMapHaInfo.getMapAsync();
	}

	/**
	 * Included self node ID notify
	 * 
	 * @see io.vertx.core.impl.HAManager#nodeAdded
	 * @see io.vertx.core.impl.HAManager#nodeLeft
	 */
	public void attachListener(NodeListener nodeListener) {
		this.removedListeneId = mapAsync.addListener(new EntryRemovedListener<String, String>() {
			@Override
			public void onRemoved(EntryEvent<String, String> event) {
				String nodeID = event.getKey();
				if (debug) {
					log.debug("********** removed(nodeLeft): nodeID={}, clusterManager.nodeID={}", nodeID,
							clusterManager.getNodeID());
				}
				nodeListener.nodeLeft(nodeID);
			}
		});

		//
		this.expiredListenerId = mapAsync.addListener(new EntryExpiredListener<String, String>() {
			@Override
			public void onExpired(EntryEvent<String, String> event) {
				String nodeID = event.getKey();
				if (debug) {
					log.debug("********** expired(nodeLeft): nodeID={}, clusterManager.nodeID={}", nodeID,
							clusterManager.getNodeID());
				}
				nodeListener.nodeLeft(nodeID);
			}
		});

		//
		this.createdListenerId = mapAsync.addListener(new EntryCreatedListener<String, String>() {
			{
				nodeCreatedNofity = this;
			}

			@Override
			public void onCreated(EntryEvent<String, String> event) {
				String nodeID = event.getKey();
				if (debug) {
					log.debug("********** created(nodeAdded): nodeID={}, clusterManager.nodeID={}", nodeID,
							clusterManager.getNodeID());
				}
				nodeListener.nodeAdded(nodeID);
			}
		});

		//
		this.updatedListenerId = mapAsync.addListener(new EntryUpdatedListener<String, String>() {
			@Override
			public void onUpdated(EntryEvent<String, String> event) {
				if (clusterManager.getNodeID().equals(event.getKey())) { // only work on self's node
					String nodeID = event.getKey();
					if (debug) {
						log.debug("********** onUpdated(clusterNodeAttached): nodeID={}, clusterManager.nodeID={}",
								nodeID, clusterManager.getNodeID());
					}
					clusterNodeAttached(nodeID, event.getValue());
				}
			}
		});
	}

	/**
	 * Should only be checked once when server startup.
	 */
	private void clusterNodeAttached(String nodeId, String value) {
		JsonObject haInfo = new JsonObject(value);
		JsonObject serverID = haInfo.getJsonObject(NonPublicAPI.EB_SERVER_ID_HA_KEY);
		if (serverID != null && !syncSubs) {
			syncSubs = true;
			mapAsync.removeListener(updatedListenerId);
			updatedListenerId = 0;
			//
			List<String> nodes = clusterManager.getNodes();
			if (nodes.size() == 1 && nodes.get(0).equals(clusterManager.getNodeID())) {
				EntryEvent<String, String> event = new EntryEvent<>(mapAsync, Type.CREATED, nodeId, value, value);
				if (debug) {
					log.debug("********** nodeCreatedNofity.onCreated, nodeId={}, serverID={}, value={}", nodeId,
							serverID, value);
				}
				nodeCreatedNofity.onCreated(event);
			}
			resetHaInfoTTL();
		}
	}

	protected void resetHaInfoTTL() {
		String nodeId = clusterManager.getNodeID();
		if (debug) {
			log.debug("nodeId={}, resetTTL={}", nodeId, resetTTL);
		}
		resetTTL.computeIfAbsent(nodeId, key -> {
			int nodeTTL = redisMapHaInfo.getTimeToLiveSeconds();
			// int refreshDelay = 15 * 1000; // debugging lost node
			int refreshDelay = (nodeTTL / 2.0) < refreshIntervalSeconds ? (nodeTTL * 1000) / 2
					: refreshIntervalSeconds * 1000; // milliseconds
			if (debug) {
				log.debug("nodeId={}, nodeTTL seconds={}, refresh delay milliseconds={}, current Nodes' TTL={}", nodeId,
						nodeTTL, refreshDelay, resetTTL);
			}
			long timeId = vertx.setPeriodic(refreshDelay, id -> {
				if (clusterManager.isInactive()) {
					if (debug) {
						log.warn("inactive nodeId={}, id={}, faultTimeMaker={}", nodeId, id, faultTimeMaker.get());
					}
					vertx.cancelTimer(id);
					resetTTL.remove(nodeId, id);
					faultTimeMaker.set(null);
				} else {
					refreshAction(nodeId);
				}
			});
			return timeId;
		});
	}

	/**
	 * Faster
	 * <p/>
	 * newOne will fire EntryCreatedListener(...)
	 */
	private void refreshAction(String nodeId) {
		JsonObject haInfo = NonPublicAPI.getHaInfo(vertx);
		if (haInfo == null) {
			log.warn("(haInfo == null)");
			return;
		}
		String v = haInfo.encode();
		mapAsync.fastPutAsync(nodeId, v, redisMapHaInfo.getTimeToLiveSeconds(), TimeUnit.SECONDS)
				.whenComplete((newOne, e) -> {
					if (e == null) {
						if (newOne) {
							log.info("newOne(addHaInfoIfLost): {}, nodeId: {}, value: {}", newOne, nodeId, v);
						}
					} else {
						log.warn("nodeId: {}, error: {}", nodeId, e.toString());
						faultTimeMaker.compareAndSet(null, new Date());
					}
				});
	}

	/**
	 * Stable
	 * <p/>
	 * get --> put
	 */
	@SuppressWarnings("unused")
	private void refreshAction_(String nodeId) {
		mapAsync.getAsync(nodeId).whenComplete((v, e) -> {
			if (e != null) {
				log.warn("nodeId: {}, error: {}", nodeId, e.toString());
				faultTimeMaker.compareAndSet(null, new Date());
			} else {
				if (v == null) {
					log.info("(v == null), nodeId: {}", nodeId);
					checkRejoin(faultTimeMaker.getAndSet(null), v);
				} else {
					mapAsync.fastPutAsync(nodeId, v, redisMapHaInfo.getTimeToLiveSeconds(), TimeUnit.SECONDS)
							.whenComplete((newOne, e2) -> {
								if (e2 == null) {
									if (newOne) {
										log.warn(
												"newOne(addHaInfoIfLost): {}, nodeId: {}, value: {}, previous value: {}",
												newOne, nodeId, v);
									}
								} else {
									log.warn("nodeId: {}, error: {}", nodeId, e2.toString());
									faultTimeMaker.compareAndSet(null, new Date());
								}
							});

					// =======================
					// mapAsync.putAsync(nodeId, v, redisMapHaInfo.getTimeToLiveSeconds(), TimeUnit.SECONDS)
					// .whenComplete((previous, e2) -> {
					// if (e2 == null) {
					// if (!v.equals(previous)) {
					// log.warn("(!v.equals(previous)), nodeId={}, v={}, previous={}",
					// nodeId, v, previous);
					// }
					// } else {
					// faultTimeMaker.compareAndSet(null, new Date());
					// }
					// });
				}
			}
		});
	}

	/**
	 * NonPublicSupportAPI.addHaInfoIfLost(...) will fire EntryCreatedListener(...)
	 */
	private void checkRejoin(Date faultTime, String haInfo) {
		String nodeID = clusterManager.getNodeID();
		if (clusterManager.isInactive()) {
			if (debug) {
				log.warn("inactive nodeID={}, haInfo={}, faultTime={}", nodeID, haInfo, faultTime);
			}
			return;
		}
		if (haInfo == null) { // rejoin
			if (debug) {
				log.debug("call addHaInfoIfLost(...), nodeID={}, haInfo={}", nodeID, haInfo);
			}
			NonPublicAPI.addHaInfoIfLost(vertx, nodeID);
		}
		//
		if (faultTime != null && haInfo != null) {
			int timeToLiveSeconds = redisMapHaInfo.getTimeToLiveSeconds();
			LocalDateTime now = LocalDateTime.now();
			LocalDateTime faultTimeWithTTL = faultTime.toInstant().plusSeconds(timeToLiveSeconds)
					.atZone(ZoneId.systemDefault()).toLocalDateTime();

			if (now.isAfter(faultTimeWithTTL)) {
				log.debug("nodeID: {}, now: {}, faultTimeWithTTL: {}, now.isAfter(faultTimeWithTTL): {}", nodeID, now,
						faultTimeWithTTL, now.isAfter(faultTimeWithTTL));
			}
		}
	}

	public int getRefreshIntervalSeconds() {
		return refreshIntervalSeconds;
	}

	public void setRefreshIntervalSeconds(int refreshIntervalSeconds) {
		this.refreshIntervalSeconds = refreshIntervalSeconds;
	}

	public void stop() {
		try {
			mapAsync.removeListener(removedListeneId);
			mapAsync.removeListener(expiredListenerId);
			mapAsync.removeListener(createdListenerId);
			if (updatedListenerId != 0) {
				mapAsync.removeListener(updatedListenerId);
			}
		} catch (Exception e) {
			log.warn(e.toString());
		}
		resetTTL.forEach((k, timeId) -> {
			vertx.cancelTimer(timeId);
		});
		resetTTL.clear();
	}

}
