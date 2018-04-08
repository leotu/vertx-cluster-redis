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
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;

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
 * @see org.redisson.api.RLocalCachedMap
 * @see org.redisson.Redisson#getLocalCachedMap
 * @see org.redisson.api.LocalCachedMapOptions
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class FactoryImpl implements Factory {
	private static final Logger log = LoggerFactory.getLogger(FactoryImpl.class);

	public static final String CLUSTER_MAP_NAME = NonPublicAPI.HA_CLUSTER_MAP_NAME;
	public static final String SUBS_MAP_NAME = NonPublicAPI.EB_SUBS_MAP_NAME;

	private final SpecifyCodec specify = new SpecifyCodec();

	@Override
	public <K, V> AsyncMap<K, V> createAsyncMap(Vertx vertx, RedissonClient redisson, String name) {
		NameWithCodec nameWithCodec = specify.selectCodecByName(name, new RedisMapCodec());
		return new RedisAsyncMap<>(vertx, redisson, nameWithCodec.name, nameWithCodec.codec);
	}

	@Override
	public <K, V> AsyncMultiMap<K, V> createAsyncMultiMap(Vertx vertx, RedissonClient redisson, String name) {
		NameWithCodec nameWithCodec = specify.selectCodecByName(name, new JsonJacksonCodec());
		return new RedisAsyncMultiMap<>(vertx, redisson, nameWithCodec.name, nameWithCodec.codec);
	}

	@Override
	public <K, V> Map<K, V> createMap(Vertx vertx, RedissonClient redisson, String name) {
		NameWithCodec nameWithCodec = specify.selectCodecByName(name, new JsonJacksonCodec());
		return new RedisMap<>(vertx, redisson, nameWithCodec.name, nameWithCodec.codec);
	}

	//
	@Override
	public AsyncMultiMap<String, ClusterNodeInfo> createAsyncMultiMapSubs(Vertx vertx, ClusterManager clusterManager,
			RedissonClient redisson, String name) {
		return new RedisAsyncMultiMapSubs(vertx, clusterManager, redisson, name);
	}

	@Override
	public Map<String, String> createMapHaInfo(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson,
			String name, int timeToLiveSeconds, int refreshIntervalSeconds) {
		return new RedisMapHaInfo(vertx, clusterManager, redisson, name, timeToLiveSeconds, refreshIntervalSeconds);
	}

	// @Override
	// public <K, V> AsyncMultiMap<K, V> createLocalCachedAsyncMultiMap(Vertx vertx, ClusterManager clusterManager,
	// RedissonClient redisson, AsyncMultiMap<K, V> delegate, int timeoutInSecoinds, String topicName) {
	// return new LocalCachedAsyncMultiMap<>(vertx, clusterManager, redisson, delegate, timeoutInSecoinds, topicName);
	// }

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

	private class NameWithCodec {
		final public String name;
		final public Codec codec;

		public NameWithCodec(String name, Codec codec) {
			this.name = name;
			this.codec = codec;
		}
	}

	private enum Type {
		DEFAULT(""), KEY_STRING("@key:String"), VAL_STRING("@val:String"), VAL_JSON("@val:Json");

		final private String value;

		private Type(String value) {
			this.value = value;
		}
	}

	private class SpecifyCodec {

		private class WhichType {
			private String name;
			private Type keyType = Type.DEFAULT;
			private Type valType = Type.DEFAULT;
		}

		private WhichType parseType(String name) {
			int idx = name.indexOf(Type.KEY_STRING.value);
			WhichType types = new WhichType();
			if (idx != -1) {
				types.keyType = Type.KEY_STRING;
				name = name.substring(0, idx) + name.substring(idx + Type.KEY_STRING.value.length());
			}
			idx = name.indexOf(Type.VAL_STRING.value);
			if (idx != -1) {
				types.valType = Type.VAL_STRING;
				name = name.substring(0, idx) + name.substring(idx + Type.VAL_STRING.value.length());
			} else {
				idx = name.indexOf(Type.VAL_JSON.value);
				if (idx != -1) {
					types.valType = Type.VAL_JSON;
					name = name.substring(0, idx) + name.substring(idx + Type.VAL_JSON.value.length());
				}
			}
			types.name = name;
			return types;
		}

		private NameWithCodec selectCodecByName(String name, Codec def) {
			WhichType types = parseType(name);

			Codec codec;
			if (types.keyType == Type.KEY_STRING && types.valType == Type.VAL_STRING) {
				codec = StringCodec.INSTANCE;
			} else if (types.keyType == Type.KEY_STRING && types.valType == Type.VAL_JSON) {
				codec = new KeyValueCodec(//
						JsonJacksonCodec.INSTANCE.getValueEncoder(), //
						JsonJacksonCodec.INSTANCE.getValueDecoder(), //
						StringCodec.INSTANCE.getMapKeyEncoder(), //
						StringCodec.INSTANCE.getMapKeyDecoder(), //
						JsonJacksonCodec.INSTANCE.getValueEncoder(), //
						JsonJacksonCodec.INSTANCE.getValueDecoder());
			} else if (types.keyType == Type.KEY_STRING) {
				RedisMapCodec valCodec = new RedisMapCodec();
				codec = new KeyValueCodec(//
						valCodec.getValueEncoder(), // JsonJacksonCodec.INSTANCE.getValueEncoder(), //
						valCodec.getValueDecoder(), // JsonJacksonCodec.INSTANCE.getValueDecoder(), //
						StringCodec.INSTANCE.getMapKeyEncoder(), //
						StringCodec.INSTANCE.getMapKeyDecoder(), //
						valCodec.getValueEncoder(), // JsonJacksonCodec.INSTANCE.getValueEncoder(), //
						valCodec.getValueDecoder()); // JsonJacksonCodec.INSTANCE.getValueDecoder());
			} else {
				codec = def;
			}
			log.debug("old name: '{}', new name: '{}', keyType: {}, valType :{}, codec: '{}'", name, types.name,
					types.keyType, types.valType, codec);
			return new NameWithCodec(types.name, codec);
		}
	}

}
