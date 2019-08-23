/*
 * Copyright (c) 2019 The original author or authors
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

import java.util.Map;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.Factory.NodeAttachListener;

/**
 * 
 * @see io.vertx.core.impl.HAManager
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class RedisMapHaInfo extends RedisMap<String, String> implements NodeAttachListener {
	private static final Logger log = LoggerFactory.getLogger(RedisMapHaInfo.class);

	private final ClusterManager clusterManager;
	private NodeListener nodeListener;
	private int removedListeneId;
	private int expiredListenerId;
	private int createdListenerId;
	private int updatedListenerId;

	public RedisMapHaInfo(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson, String name) {
		super(vertx, redisson, name, null);
		this.clusterManager = clusterManager;
		this.attachListener();
	}

	/**
	 * @see org.redisson.client.codec.StringCodec
	 */
	@Override
	protected RMapCache<String, String> createMap(RedissonClient redisson, String name, Codec codec) {
		return redisson.getMapCache(name, new StringCodec());
	}

	protected RMapCache<String, String> getMapAsync() {
		return (RMapCache<String, String>) super.map;
	}

	@Override
	public void attachListener(NodeListener nodeListener) {
		log.debug("...");
		this.nodeListener = nodeListener;
	}

	public void detachListener() {
		RMapCache<String, String> mapAsync = getMapAsync();
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

	@Override
	public void putAll(Map<? extends String, ? extends String> m) {
		throw new UnsupportedOperationException("putAll");
	}

	/**
	 * Included self node ID notify
	 * 
	 * @see io.vertx.core.impl.HAManager#nodeAdded
	 * @see io.vertx.core.impl.HAManager#nodeLeft
	 */
	private void attachListener() {
		log.info("### current nodeID: {}, nodeListener: {}", clusterManager.getNodeID(), nodeListener);
		RMapCache<String, String> mapAsync = getMapAsync();
		if (removedListeneId == 0) {
			removedListeneId = mapAsync.addListener(new EntryRemovedListener<String, String>() {
				@Override
				public void onRemoved(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					log.info("### self: {}, onRemoved: {}", clusterManager.getNodeID(), nodeId);
					if (nodeListener != null) {
						nodeListener.nodeLeft(nodeId);
					}
				}
			});
		}

		if (expiredListenerId == 0) {
			expiredListenerId = mapAsync.addListener(new EntryExpiredListener<String, String>() {
				@Override
				public void onExpired(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					log.info("### self: {}, onExpired: {}", clusterManager.getNodeID(), nodeId);
					if (nodeListener != null) {
						nodeListener.nodeLeft(nodeId);
					}
				}
			});
		}

		if (createdListenerId == 0) {
			createdListenerId = mapAsync.addListener(new EntryCreatedListener<String, String>() {
				@Override
				public void onCreated(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					log.info("### self: {}, onCreated: {}", clusterManager.getNodeID(), nodeId);
					if (nodeListener != null) {
						nodeListener.nodeAdded(nodeId);
					}
				}
			});
		}

		if (updatedListenerId == 0) {
			updatedListenerId = mapAsync.addListener(new EntryUpdatedListener<String, String>() {
				@Override
				public void onUpdated(EntryEvent<String, String> event) {
					String nodeId = event.getKey();
					log.info("### self: {}, onUpdated: {}", clusterManager.getNodeID(), nodeId);
					if (clusterManager.getNodeID().equals(nodeId)) { // only work on self's node
						log.info("### Skip onUpdated is self: {}", nodeId);
					} else {
						nodeListener.nodeAdded(nodeId);
					}
				}
			});
		}
	}
}
