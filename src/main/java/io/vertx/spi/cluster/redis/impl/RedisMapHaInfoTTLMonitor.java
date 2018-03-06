/*
 * Copyright (c) 2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
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

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.NonPublicAPI;
import io.vertx.spi.cluster.redis.NonPublicAPI.ClusteredEventBusAPI;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * TTL: "__vertx.haInfo"
 * 
 * @see org.redisson.api.RScheduledExecutorService
 * @see org.redisson.api.RedissonClient#getExecutorService
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class RedisMapHaInfoTTLMonitor {
	private static final Logger log = LoggerFactory.getLogger(RedisMapHaInfoTTLMonitor.class);

	static private boolean debug = false;

	private int refreshIntervalSeconds = 5;

	private final ConcurrentMap<String, Long> resetTTL = new ConcurrentHashMap<>();// <nodeId, Timer ID>
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
	protected void attachListener(NodeListener nodeListener) {
		if (removedListeneId == 0) {
			removedListeneId = mapAsync.addListener(new EntryRemovedListener<String, String>() {
				@Override
				public void onRemoved(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					if (debug) {
						log.debug("removed(nodeLeft): nodeId={}, clusterManager.nodeId={}", nodeId, clusterManager.getNodeID());
					}
					nodeListener.nodeLeft(nodeId);
				}
			});
		}

		if (expiredListenerId == 0) {
			expiredListenerId = mapAsync.addListener(new EntryExpiredListener<String, String>() {
				@Override
				public void onExpired(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					if (debug) {
						log.debug("expired(nodeLeft): nodeId={}, clusterManager.nodeId={}", nodeId, clusterManager.getNodeID());
					}
					nodeListener.nodeLeft(nodeId);
				}
			});
		}

		if (createdListenerId == 0) {
			createdListenerId = mapAsync.addListener(new EntryCreatedListener<String, String>() {
				{
					nodeCreatedNofity = this;
				}

				@Override
				public void onCreated(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					if (debug) {
						log.debug("created(nodeAdded): nodeId={}, clusterManager.nodeId={}", nodeId, clusterManager.getNodeID());
					}
					nodeListener.nodeAdded(nodeId);
				}
			});
		}

		if (updatedListenerId == 0) {
			updatedListenerId = mapAsync.addListener(new EntryUpdatedListener<String, String>() {
				@Override
				public void onUpdated(EntryEvent<String, String> event) {
					if (clusterManager.getNodeID().equals(event.getKey())) { // only work on self's node
						String nodeId = event.getKey();
						if (debug) {
							log.debug("onUpdated(clusterNodeAttached): nodeId={}, clusterManager.nodeId={}", nodeId,
									clusterManager.getNodeID());
						}
						clusterNodeAttached(nodeId, event.getValue());
					}
				}
			});
		}
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
					log.debug("nodeCreatedNofity.onCreated, nodeId={}, serverID={}, value={}", nodeId, serverID, value);
				}
				nodeCreatedNofity.onCreated(event);
			}
			resetHaInfoTTL();
		}
	}

	protected void resetHaInfoTTL() {
		String nodeId = clusterManager.getNodeID();
		if (debug) {
			log.debug("redisMapHaInfo.timeToLiveSeconds={}, nodeId={}, resetTTL={}", redisMapHaInfo.getTimeToLiveSeconds(),
					nodeId, resetTTL);
		}
		if (redisMapHaInfo.getTimeToLiveSeconds() <= 0) {
			return;
		}
		resetTTL.computeIfAbsent(nodeId, key -> {
			int nodeTTL = redisMapHaInfo.getTimeToLiveSeconds();
			// int refreshDelay = 30 * 1000; // debugging lost node
			int refreshDelay = (nodeTTL / 3.0) < refreshIntervalSeconds ? (nodeTTL * 1000) / 3
					: refreshIntervalSeconds * 1000; // milliseconds
			if (refreshDelay < 2000) {
				refreshDelay = 2000;
			}
			if (debug) {
				log.debug("nodeId={}, nodeTTL seconds={}, refresh delay milliseconds={}, current Nodes' TTL={}", nodeId,
						nodeTTL, refreshDelay, resetTTL);
			}
			long timeId = vertx.setPeriodic(refreshDelay, id -> {
				if (!clusterManager.isActive()) {
					if (debug) {
						log.warn("!isActive(), nodeId={}, id={}, faultTimeMaker={}", nodeId, id, faultTimeMaker.get());
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
		JsonObject haInfo = ClusteredEventBusAPI.getHaInfo(ClusteredEventBusAPI.getHAManager(clusterManager.getEventBus()));
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
	@Deprecated
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
										log.warn("newOne(addHaInfoIfLost): {}, nodeId: {}, value: {}, previous value: {}", newOne, nodeId,
												v);
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
		String nodeId = clusterManager.getNodeID();
		if (!clusterManager.isActive()) {
			if (debug) {
				log.warn("!isActive(), nodeId={}, haInfo={}, faultTime={}", nodeId, haInfo, faultTime);
			}
			return;
		}
		if (haInfo == null) { // rejoin
			if (debug) {
				log.debug("call addHaInfoIfLost(...), nodeId={}, haInfo={}", nodeId, haInfo);
			}
			NonPublicAPI.addHaInfoIfLost(ClusteredEventBusAPI.getHAManager(clusterManager.getEventBus()), nodeId); // XXX
		}

		if (faultTime != null && haInfo != null) {
			int timeToLiveSeconds = redisMapHaInfo.getTimeToLiveSeconds();
			LocalDateTime now = LocalDateTime.now();
			LocalDateTime faultTimeWithTTL = faultTime.toInstant().plusSeconds(timeToLiveSeconds)
					.atZone(ZoneId.systemDefault()).toLocalDateTime();

			if (now.isAfter(faultTimeWithTTL)) {
				log.debug("nodeId: {}, now: {}, faultTimeWithTTL: {}, now.isAfter(faultTimeWithTTL): {}", nodeId, now,
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

	private void detachListener() {
		if (removedListeneId != 0) {
			mapAsync.removeListener(removedListeneId);
			removedListeneId = 0;
		}
		if (expiredListenerId != 0) {
			mapAsync.removeListener(expiredListenerId);
			expiredListenerId = 0;
		}
		if (createdListenerId != 0) {
			mapAsync.removeListener(createdListenerId);
			createdListenerId = 0;
		}
		if (updatedListenerId != 0) {
			mapAsync.removeListener(updatedListenerId);
			updatedListenerId = 0;
		}
	}

	private void stopScheduler() {
		resetTTL.forEach((k, timeId) -> {
			vertx.cancelTimer(timeId);
		});
		resetTTL.clear();
	}

	protected void stop() {
		detachListener();
		stopScheduler();
	}
}
