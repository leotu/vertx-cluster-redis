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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

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
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.Factory.NodeAttachListener;
import io.vertx.spi.cluster.redis.impl.NonPublicAPI.ClusteredEventBusAPI;

/**
 * TTL: "__vertx.haInfo"
 * 
 * @see org.redisson.api.RScheduledExecutorService
 * @see org.redisson.api.RedissonClient#getExecutorService
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class RedisMapHaInfoTTLMonitor implements NodeAttachListener {
	private static final Logger log = LoggerFactory.getLogger(RedisMapHaInfoTTLMonitor.class);

	final private int refreshIntervalSeconds;

	private final ConcurrentMap<String, Long> resetTTL = new ConcurrentHashMap<>();// <nodeId, Timer ID>
	private final Vertx vertx;
	private final ClusterManager clusterManager;
	private final RedisMapHaInfo redisMapHaInfo;
	private final RMapCache<String, String> mapAsync;

	private int removedListeneId;
	private int expiredListenerId;
	private int createdListenerId;
	private int updatedListenerId;

	private EntryCreatedListener<String, String> nodeCreatedNofity; // join?

	private boolean syncSubs = false;
	protected final String name;

	public RedisMapHaInfoTTLMonitor(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson,
			RedisMapHaInfo redisMapHaInfo, String name, int refreshIntervalSeconds) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(redisMapHaInfo, "redisMapHaInfo");
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.redisMapHaInfo = redisMapHaInfo;
		this.name = name;
		this.refreshIntervalSeconds = refreshIntervalSeconds;
		this.mapAsync = redisMapHaInfo.getMapAsync();
	}

	/**
	 * Included self node ID notify
	 * 
	 * @see io.vertx.core.impl.HAManager#nodeAdded
	 * @see io.vertx.core.impl.HAManager#nodeLeft
	 */
	public void attachListener(NodeListener nodeListener) {
		if (removedListeneId == 0) {
			removedListeneId = mapAsync.addListener(new EntryRemovedListener<String, String>() {
				@Override
				public void onRemoved(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					nodeListener.nodeLeft(nodeId);
				}
			});
		}

		if (expiredListenerId == 0) {
			expiredListenerId = mapAsync.addListener(new EntryExpiredListener<String, String>() {
				@Override
				public void onExpired(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
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
				nodeCreatedNofity.onCreated(event);
			}
			resetHaInfoTTL();
		}
	}

	protected void resetHaInfoTTL() {
		if (redisMapHaInfo.getTimeToLiveSeconds() <= 0) {
			return;
		}
		String nodeId = clusterManager.getNodeID();
		resetTTL.computeIfAbsent(nodeId, key -> {
			int nodeTTL = redisMapHaInfo.getTimeToLiveSeconds();
			// int refreshDelay = 30 * 1000; // debugging lost node
			int refreshDelay = (nodeTTL / 3.0) < refreshIntervalSeconds ? (nodeTTL * 1000) / 3
					: refreshIntervalSeconds * 1000; // milliseconds
			if (refreshDelay < 2000) {
				refreshDelay = 2000;
			}
			long timeId = vertx.setPeriodic(refreshDelay, id -> {
				if (!clusterManager.isActive()) {
					vertx.cancelTimer(id);
					resetTTL.remove(nodeId, id);
					// faultTimeMaker.set(null);
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
		JsonObject haInfo = ClusteredEventBusAPI
				.haInfo(ClusteredEventBusAPI.haManager(ClusteredEventBusAPI.eventBus(vertx)));
		if (haInfo == null) {
			log.warn("(haInfo == null)");
			return;
		}
		String v = haInfo.encode();
		mapAsync.fastPutAsync(nodeId, v, redisMapHaInfo.getTimeToLiveSeconds(), TimeUnit.SECONDS)
				.whenComplete((newOne, e) -> {
					if (e == null) {
						if (newOne) {
							log.debug("newOne(addHaInfoIfLost): {}, nodeId: {}, value: {}", newOne, nodeId, v);
						}
					} else {
						log.warn("nodeId: {}, error: {}", nodeId, e.toString());
					}
				});
	}

	// /**
	// * NonPublicSupportAPI.addHaInfoIfLost(...) will fire EntryCreatedListener(...)
	// */
	// private void checkRejoin(String haInfo) {
	// String nodeId = clusterManager.getNodeID();
	// if (!clusterManager.isActive()) {
	// return;
	// }
	// if (haInfo == null) { // rejoin
	// NonPublicAPI.addHaInfoIfLost(ClusteredEventBusAPI.haManager(ClusteredEventBusAPI.eventBus(vertx)), nodeId); // XXX
	// }
	// }

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

	@Override
	public String toString() {
		return super.toString() + "{name=" + name + "}";
	}
}
