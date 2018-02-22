package io.vertx.spi.cluster.redis.impl;

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
	private final RedisMapHaInfo redisMapEventbus;
	// private final RScheduledExecutorService scheduledService;
	private final RMapCache<String, String> mapAsync;

	private int removedListeneId;
	private int expiredListenerId;
	private int createdListenerId;
	private int updatedListenerId;

	private EntryCreatedListener<String, String> nodeCreatedNofity; // join?

	private boolean syncSubs = false;

	private final AtomicReference<Date> faultTimeMaker = new AtomicReference<>();
	// private int checkRejoinCounter = 0;

	public RedisMapHaInfoTTLMonitor(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson,
			RedisMapHaInfo redisMapEventbus, RMapCache<String, String> mapAsync) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(redisMapEventbus, "redisMapEventbus");
		Objects.requireNonNull(mapAsync, "mapAsync");
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.redisMapEventbus = redisMapEventbus;
		this.mapAsync = mapAsync;
		// this.scheduledService = redisson.getExecutorService("HaInfoTTLMonitor");
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
				Boolean sameSource = event.getSource().equals(mapAsync);
				if (!sameSource) {
					log.debug(
							"removedListeneId={}, clusterManager.nodeID={}, sameSource={}, removed key.class={}, key={}, removed value.class={}, value={}",
							removedListeneId, clusterManager.getNodeID(), sameSource,
							event.getKey().getClass().getName(), event.getKey(), event.getValue().getClass().getName(),
							event.getValue());
				}
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
				Boolean sameSource = event.getSource().equals(mapAsync);
				if (!sameSource) {
					log.debug(
							"expiredListenerId={}, clusterManager.nodeID={}, sameSource={}, expired key.class={}, key={}, expired value.class={}, value={}",
							expiredListenerId, clusterManager.getNodeID(), sameSource,
							event.getKey().getClass().getName(), event.getKey(), event.getValue().getClass().getName(),
							event.getValue());
				}
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
				Boolean sameSource = event.getSource().equals(mapAsync);
				if (!sameSource) {
					log.debug(
							"createdListenerId={}, clusterManager.nodeID={}, sameSource={}, created key.class={}, key={}, created value.class={}, value={}",
							createdListenerId, clusterManager.getNodeID(), sameSource,
							event.getKey().getClass().getName(), event.getKey(), event.getValue().getClass().getName(),
							event.getValue());
				}
				String nodeID = event.getKey();
				if (debug) {
					log.debug("********** created(nodeAdded): nodeID={}, clusterManager.nodeID={}", nodeID,
							clusterManager.getNodeID());
				}
				nodeListener.nodeAdded(nodeID);
				resetTTL(nodeID, event.getValue());
			}
		});

		//
		this.updatedListenerId = mapAsync.addListener(new EntryUpdatedListener<String, String>() {
			@Override
			public void onUpdated(EntryEvent<String, String> event) {
				Boolean sameSource = event.getSource().equals(mapAsync);
				if (!sameSource) {
					log.debug(
							"listenerId={},clusterManager.nodeID={}, sameSource={}, updated key.class={}, key={}, updated value.class={}, value={}",
							updatedListenerId, clusterManager.getNodeID(), sameSource,
							event.getKey().getClass().getName(), event.getKey(), event.getValue().getClass().getName(),
							event.getValue());
				}

				if (!clusterManager.getNodeID().equals(event.getKey())) { // filter must be self's node
					if (debug) {
						log.debug("Skip: clusterManager.nodeID, key={}, value={}", clusterManager.getNodeID(),
								event.getKey(), event.getValue());
					}
					return;
				}
				String nodeID = event.getKey();
				if (debug) {
					log.debug("********** onUpdated(?): nodeID={}, clusterManager.nodeID={}", nodeID,
							clusterManager.getNodeID());
				}
				fireClusterFirstNodeAttached(nodeID, event.getValue());
			}
		});
	}

	/**
	 * Should only be checked once when server startup.
	 */
	private void fireClusterFirstNodeAttached(String nodeId, String value) {
		JsonObject haInfo = new JsonObject(value);
		JsonObject serverID = haInfo.getJsonObject(NonPublicSupportAPI.EB_SERVER_ID_HA_KEY);
		if (serverID != null && !syncSubs) {
			syncSubs = true;
			if (debug) {
				log.debug("removeListener: updatedListenerId={}", updatedListenerId);
			}
			mapAsync.removeListener(updatedListenerId);
			updatedListenerId = 0;
			//
			List<String> nodes = clusterManager.getNodes();
			if (nodes.size() == 1 && nodes.get(0).equals(clusterManager.getNodeID())) {
				EntryEvent<String, String> event = new EntryEvent<>(mapAsync, Type.CREATED, nodeId, value, value);
				if (debug) {
					log.debug("********** nodeCreatedNofity nodeId={}, serverID={}, value={}", nodeId, serverID, value);
				}
				nodeCreatedNofity.onCreated(event);
			}

		}
	}

	protected void resetTTL(String nodeId, String value) {
		resetTTL.computeIfAbsent(nodeId, key -> {
			if (clusterManager.isInactive()) {
				if (debug) {
					log.debug("inactive nodeId={}, faultTimeMaker={}", nodeId, faultTimeMaker.get());
				}
				faultTimeMaker.set(null);
				return null; // skip
			}

			int timeToLiveSeconds = redisMapEventbus.getTimeToLiveSeconds();
			int delay = (timeToLiveSeconds / 2.0) < refreshIntervalSeconds ? (timeToLiveSeconds * 1000) / 2
					: refreshIntervalSeconds * 1000; // milliseconds

			long timeId = vertx.setPeriodic(delay, id -> {
				if (clusterManager.isInactive()) {
					if (debug) {
						log.warn("inactive nodeId={}, id={}, faultTimeMaker={}", nodeId, id, faultTimeMaker.get());
					}
					vertx.cancelTimer(id);
					resetTTL.remove(nodeId, id);
					faultTimeMaker.set(null);
				} else {
					// checkRejoinCounter++;
					// log.debug("nodeId={}, k={}, delay={}, now={}", nodeId, k, delay, new Date());
					mapAsync.getAsync(nodeId).whenComplete((v, e) -> {
						if (e != null) {
							log.warn("nodeId={}, id={}, error={}", nodeId, id, e.toString());
							faultTimeMaker.compareAndSet(null, new Date());
						} else {
							if (v == null) {
								if (debug) {
									log.debug("nodeId={}, v is null", nodeId, nodeId);
								}
								// checkRejoin(faultTimeMaker.getAndSet(null), v);
							} else {
								// v={"verticles":[],"group":"__DISABLED__","server_id":{"host":"192.168.1.14","port":18060}}
								// log.debug("nodeId={}, v.class={}, v={}, checkRejoinCounter={}", nodeId,
								// v.getClass().getName(), v, checkRejoinCounter);
								// if (checkRejoinCounter == 10) { // XXX: debug: connected
								// checkRejoin(faultTimeMaker.getAndSet(null), v);
								// checkRejoinCounter = 0;
								// } else if (checkRejoinCounter == 5) { // XXX: debug: disconnected
								// faultTimeMaker.compareAndSet(null, new Date());
								// } else {
								// if (true) {
								// return;
								// }
								mapAsync.putAsync(nodeId, v, redisMapEventbus.getTimeToLiveSeconds(), TimeUnit.SECONDS)
										.whenComplete((previous, e2) -> {
											if (e2 == null) {
												if (!v.equals(previous)) {
													log.warn("(!v.equals(previous)), nodeId={}, v={}, previous={}",
															nodeId, v, previous);
												}
												// checkRejoin(faultTimeMaker.getAndSet(null), v);
											} else {
												faultTimeMaker.compareAndSet(null, new Date());
											}
										});
								// }
							}
						}
					});
				}
			});
			if (debug) {
				log.debug("nodeId={}, refreshIntervalSeconds={}, delay={}, timeId={}", nodeId, refreshIntervalSeconds,
						delay, timeId);
			}
			return timeId;
		});
	}

	// private void checkRejoin(Date faultTime, String value) {
	// if (debug) {
	// return;
	// }
	// String nodeID = clusterManager.getNodeID();
	//
	// if (faultTime == null && value != null) {
	// log.debug("nodeID={}, (faultTime == null && value != null)", nodeID);
	// return;
	// }
	// if (faultTime != null && value != null) {
	// int timeToLiveSeconds = redisMapEventbus.getTimeToLiveSeconds();
	// LocalDateTime now = LocalDateTime.now();
	// LocalDateTime faultTimeWithTTL = faultTime.toInstant().plusSeconds(timeToLiveSeconds)
	// .atZone(ZoneId.systemDefault()).toLocalDateTime();
	//
	// log.debug("nodeID={}, now={}, faultTimeWithTTL={}, now.isAfter(faultTimeWithTTL)={}", nodeID, now,
	// faultTimeWithTTL, now.isAfter(faultTimeWithTTL));
	// } else {
	// log.debug("nodeID={}, faultTime={}, value={}", nodeID, faultTime, value);
	// }
	// }

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
		// scheduledService.shutdown();
		resetTTL.forEach((k, timeId) -> {
			vertx.cancelTimer(timeId);
		});
		resetTTL.clear();
	}

}
