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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.redisson.api.RedissonClient;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.Factory;
import io.vertx.spi.cluster.redis.impl.NonPublicAPI.ClusteredEventBusAPI;

/**
 *
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class FactoryImpl implements Factory {
	private static final Logger log = LoggerFactory.getLogger(FactoryImpl.class);

	public static final String CLUSTER_MAP_NAME = NonPublicAPI.HA_CLUSTER_MAP_NAME;
	public static final String SUBS_MAP_NAME = NonPublicAPI.EB_SUBS_MAP_NAME;

	@Override
	public <K, V> AsyncMap<K, V> createAsyncMap(Vertx vertx, RedissonClient redisson, String name) {
		return new RedisAsyncMap<>(vertx, redisson, name);
	}

	@Override
	public <K, V> AsyncMultiMap<K, V> createAsyncMultiMap(Vertx vertx, RedissonClient redisson, String name) {
		return new RedisAsyncMultiMap<>(vertx, redisson, name);
	}

	@Override
	public AsyncMultiMap<String, ClusterNodeInfo> createAsyncMultiMapSubs(Vertx vertx, ClusterManager clusterManager,
			RedissonClient redisson, String name) {
		return new RedisAsyncMultiMapSubs(vertx, clusterManager, redisson, name);
	}

	@Override
	public <K, V> Map<K, V> createMap(Vertx vertx, RedissonClient redisson, String name) {
		return new RedisMap<>(vertx, redisson, name);
	}

	@Override
	public Map<String, String> createMapHaInfo(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson,
			String name, int timeToLiveSeconds, int refreshIntervalSeconds) {
		return new RedisMapHaInfo(vertx, clusterManager, redisson, name, timeToLiveSeconds, refreshIntervalSeconds);
	}

	@Override
	public <K, V> AsyncMultiMap<K, V> createLocalCachedAsyncMultiMap(Vertx vertx, ClusterManager clusterManager,
			RedissonClient redisson, AsyncMultiMap<K, V> delegate, int timeoutInSecoinds, String topicName) {
		return new LocalCachedAsyncMultiMap<>(vertx, clusterManager, redisson, delegate, timeoutInSecoinds, topicName);
	}

	@SuppressWarnings({ "serial", "unchecked" })
	@Override
	public PendingMessageProcessor createPendingMessageProcessor(Vertx vertx, ClusterManager clusterManager,
			AsyncMultiMap<String, ClusterNodeInfo> subs) {

		ClusteredEventBus eventBus = ClusteredEventBusAPI.eventBus(vertx);
		AtomicReference<PendingMessageProcessor> pendingProcessorRef = new AtomicReference<>();

		ConcurrentMap<ServerID, Object> newConnections = new ConcurrentHashMap<ServerID, Object>() {

			/**
			 * @param key is ServerID type
			 * @param value is ConnectionHolder type
			 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder#close
			 */
			@Override
			public boolean remove(Object serverID, Object connHolder) {
				boolean wasRemoved = super.remove(serverID, connHolder);
				if (wasRemoved) {
					pendingProcessorRef.get().run((ServerID) serverID, connHolder);
				} else {
					log.debug("skip pendingProcessor serverID: {}, was removed nothing.", serverID);
				}
				return wasRemoved;
			}
		};

		PendingMessageProcessor pendingProcessor = new PendingMessageProcessorImpl(vertx, clusterManager, eventBus, subs,
				newConnections);
		pendingProcessorRef.set(pendingProcessor);

		ConcurrentMap<ServerID, Object> oldOne = (ConcurrentMap<ServerID, Object>) ClusteredEventBusAPI
				.connections(eventBus);
		ClusteredEventBusAPI.setConnections(eventBus, newConnections); // reset to create new Instance
		newConnections.putAll(oldOne);

		return pendingProcessor;
	}
}
