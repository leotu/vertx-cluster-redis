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
package io.vertx.spi.cluster.redis;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.NonPublicAPI.ClusteredEventBusAPI;
import io.vertx.spi.cluster.redis.NonPublicAPI.ClusteredEventBusAPI.ConnectionHolderAPI;
import io.vertx.spi.cluster.redis.NonPublicAPI.LocalCached;
import io.vertx.spi.cluster.redis.impl.LocalCachedAsyncMultiMap;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMap;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMap;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMapSubs;
import io.vertx.spi.cluster.redis.impl.RedisMap;
import io.vertx.spi.cluster.redis.impl.RedisMapHaInfo;

/**
 * https://github.com/redisson/redisson/wiki/11.-Redis-commands-mapping
 * 
 * @see io.vertx.core.impl.VertxFactoryImpl#clusteredVertx
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class RedisClusterManager implements ClusterManager {
	private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

	private Vertx vertx;
	private final RedissonClient redisson;

	private String nodeId;
	private ClusteredEventBus eventBus;

	private AtomicBoolean active = new AtomicBoolean();
	private boolean disableTTL = false; // XXX
	private NodeListener nodeListener;

	private RedisMapHaInfo haInfo;
	private final int haInfoTimeToLiveSeconds = disableTTL ? 0 : 10; // TTL seconds
	private final int haInfoRefreshIntervalSeconds = 5; // TTL Refresh seconds

	private AsyncMultiMap<String, ClusterNodeInfo> subs;
	private boolean enableCacheSubs = false; // XXX
	private String cacheSubsTopicName = "cacheSubsTopic";
	private final int cacheSubsTimeoutInSecoinds = 15;

	private ConcurrentMap<String, Map<?, ?>> mapCache = new ConcurrentHashMap<>();
	private ConcurrentMap<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();
	private ConcurrentMap<String, AsyncMultiMap<?, ?>> asyncMultiMapCache = new ConcurrentHashMap<>();

	private static final String CLUSTER_MAP_NAME = NonPublicAPI.HA_CLUSTER_MAP_NAME;
	private static final String SUBS_MAP_NAME = NonPublicAPI.EB_SUBS_MAP_NAME;

	public RedisClusterManager(RedissonClient redisson, String nodeId) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(nodeId, "nodeId");
		this.redisson = redisson;
		this.nodeId = nodeId;
	}

	public RedissonClient getRedisson() {
		return redisson;
	}

	private void readyEventBus(ClusteredEventBus eventBus, AsyncMultiMap<String, ClusterNodeInfo> subs) {
		this.eventBus = eventBus;

		@SuppressWarnings("unchecked")
		ConcurrentMap<ServerID, Object> oldOne = (ConcurrentMap<ServerID, Object>) ClusteredEventBusAPI
				.connections(this.eventBus);
		@SuppressWarnings("serial")
		ConcurrentMap<ServerID, Object> newOne = new ConcurrentHashMap<ServerID, Object>() {
			PendingMessageProcessor pendingProcessor = new PendingMessageProcessor(vertx, RedisClusterManager.this, eventBus,
					subs, this);

			/**
			 * @param key is ServerID type
			 * @param value is ConnectionHolder type
			 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder#close
			 */
			@Override
			public boolean remove(Object serverID, Object connHolder) {
				boolean ok = super.remove(serverID, connHolder);
				if (ok) {
					Queue<ClusteredMessage<?, ?>> pending = ConnectionHolderAPI.pending(connHolder);
					ServerID holderServerID = ConnectionHolderAPI.serverID(connHolder);
					if (!serverID.equals(holderServerID)) {
						log.warn("(!serverID.equals(holderServerID), serverID: {}, holderServerID: {}", serverID, holderServerID);
					}
					if (pending != null && !pending.isEmpty()) {
						pendingProcessor.run((ServerID) serverID, pending);
					}
				}
				return ok;
			}
		};
		ClusteredEventBusAPI.setConnections(this.eventBus, newOne); // reset to new Instance
		if (!oldOne.isEmpty()) {
			newOne.putAll(oldOne);
		}
	}

	/**
	 *
	 */
	@Override
	public void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}

	/**
	 * EventBus been created !
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
		vertx.executeBlocking(future -> {
			if (name.equals(SUBS_MAP_NAME)) {
				if (subs == null) {
					subs = new RedisAsyncMultiMapSubs(vertx, this, redisson, name);
					if (enableCacheSubs) {
						subs = new LocalCachedAsyncMultiMap<String, ClusterNodeInfo>(vertx, this, redisson, subs,
								cacheSubsTimeoutInSecoinds, cacheSubsTopicName);
					}
					readyEventBus(ClusteredEventBusAPI.eventBus(vertx), subs);
				}
				future.complete((AsyncMultiMap<K, V>) subs);
			} else {
				AsyncMultiMap<K, V> asyncMultiMap = (AsyncMultiMap<K, V>) asyncMultiMapCache.computeIfAbsent(name,
						key -> new RedisAsyncMultiMap<K, V>(vertx, redisson, name));
				future.complete(asyncMultiMap);
			}

		}, resultHandler);
	}

	@Override
	public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
		if (name.equals(CLUSTER_MAP_NAME)) {
			log.error("name cannot be \"{}\"", name);
			resultHandler.handle(Future.failedFuture(new IllegalArgumentException("name cannot be \"" + name + "\"")));
			return;
		}
		vertx.executeBlocking(future -> {
			@SuppressWarnings("unchecked")
			AsyncMap<K, V> asyncMap = (AsyncMap<K, V>) asyncMapCache.computeIfAbsent(name,
					key -> new RedisAsyncMap<K, V>(vertx, redisson, name));
			future.complete(asyncMap);
		}, resultHandler);
	}

	/**
	 *
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<K, V> getSyncMap(String name) {
		if (name.equals(CLUSTER_MAP_NAME)) {
			if (haInfo == null) {
				haInfo = new RedisMapHaInfo(vertx, this, redisson, name, haInfoTimeToLiveSeconds, haInfoRefreshIntervalSeconds);
			}
			return (Map<K, V>) haInfo;
		} else {
			Map<K, V> map = (Map<K, V>) mapCache.computeIfAbsent(name, key -> new RedisMap<K, V>(vertx, redisson, name));
			return map;
		}
	}

	@Override
	public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
		try {
			RLock lock = redisson.getLock(name); // getFairLock ?
			lock.tryLockAsync(timeout, TimeUnit.MILLISECONDS).whenComplete((v, e) -> resultHandler
					.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(new RedisLock(lock))));
		} catch (Exception e) {
			log.info("nodeId: " + nodeId + ", name: " + name + ", timeout: " + timeout, e);
			resultHandler.handle(Future.failedFuture(e));
		}
	}

	@Override
	public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
		try {
			RAtomicLong counter = redisson.getAtomicLong(name);
			resultHandler.handle(Future.succeededFuture(new RedisCounter(counter)));
		} catch (Exception e) {
			log.info("nodeId: " + nodeId + ", name: " + name, e);
			resultHandler.handle(Future.failedFuture(e));
		}
	}

	@Override
	public String getNodeID() {
		return nodeId;
	}

	/**
	 * @see io.vertx.core.impl.HAManager#addHaInfoIfLost
	 */
	@Override
	public List<String> getNodes() {
		return haInfo.keySet().stream().map(e -> e.toString()).collect(Collectors.toList());
	}

	private void clearLocalCached() {
		if (subs != null && subs instanceof LocalCached) {
			((LocalCached) subs).discard();
		}
	}

	/**
	 * 
	 * @see io.vertx.core.impl.HAManager#nodeAdded
	 * @see io.vertx.core.impl.HAManager#nodeLeft
	 */
	@Override
	public void nodeListener(NodeListener nodeListener) {
		this.nodeListener = new NodeListener() {
			@Override
			synchronized public void nodeAdded(String nodeId) {
				clearLocalCached();
				nodeListener.nodeAdded(nodeId);
			}

			/**
			 * The method won't delete it's own subs
			 */
			@Override
			synchronized public void nodeLeft(String nodeId) {
				clearLocalCached();
				nodeListener.nodeLeft(nodeId);
			}
		};
		this.haInfo.attachListener(this.nodeListener);
	}

	// private void startLocalMapCache() {
	// this.timerId = vertx.setPeriodic(TimeUnit.MINUTES.toMillis(1), id -> {
	// String keys[] = mapCache.keySet().toArray(new String[0]);
	// Stream.of(keys).forEach(key -> {
	// Map<?, ?> map = mapCache.get(key);
	// if (map.isEmpty()) {
	// mapCache.remove(key);
	// }
	// });
	//
	// //
	// keys = asyncMapCache.keySet().toArray(new String[0]);
	// Stream.of(keys).forEach(key -> {
	// AsyncMap<?, ?> map = asyncMapCache.get(key);
	// map.size(ar -> {
	// if (ar.succeeded() && ar.result() == 0) {
	// asyncMapCache.remove(key);
	// }
	// });
	// });
	//
	// // FIXME: asyncMultiMapCache
	// });
	// }

	/**
	 *
	 */
	@Override
	public void join(Handler<AsyncResult<Void>> resultHandler) {
		if (active.compareAndSet(false, true)) {
			vertx.executeBlocking(future -> {
				synchronized (RedisClusterManager.this) {
					clearLocalCached();
					future.complete();
				}
			}, resultHandler);
		} else {
			// throw new IllegalStateException("Already activated");
			log.warn("Already activated, nodeId: {}", nodeId);
			vertx.getOrCreateContext().runOnContext(v -> Future.<Void>succeededFuture().setHandler(resultHandler));
		}
	}

	/**
	 *
	 */
	@Override
	public void leave(Handler<AsyncResult<Void>> resultHandler) {
		if (active.compareAndSet(true, false)) {
			vertx.executeBlocking(future -> {
				synchronized (RedisClusterManager.this) {
					clearLocalCached();
					future.complete();
				}
			}, resultHandler);
		} else {
			// throw new IllegalStateException("Already inactive");
			log.warn("Already activated, nodeId: {}", nodeId);
			vertx.getOrCreateContext().runOnContext(v -> Future.<Void>succeededFuture().setHandler(resultHandler));
		}
	}

	@Override
	public boolean isActive() {
		return active.get();
	}

	/**
	 * Lock implement
	 */
	private class RedisCounter implements Counter {
		private final RAtomicLong counter;

		public RedisCounter(RAtomicLong counter) {
			this.counter = counter;
		}

		@Override
		public void get(Handler<AsyncResult<Long>> resultHandler) {
			// Context context = vertx.getOrCreateContext();

			// counter.getAsync().whenComplete((v, e) -> {
			// if (e != null) {
			// context.runOnContext(vv -> Future.<Long>failedFuture(e).setHandler(resultHandler));
			// } else {
			// context.runOnContext(vv -> Future.<Long>succeededFuture(v).setHandler(resultHandler));
			// }
			// });

			// counter.getAsync().whenComplete((v, e) -> context.runOnContext(vv -> //
			// resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v))));

			counter.getAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
			counter.incrementAndGetAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
			counter.getAndIncrementAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
			counter.decrementAndGetAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
			counter.addAndGetAsync(value)
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
			counter.getAndAddAsync(value)
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
			counter.compareAndSetAsync(expected, value)
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}
	}

	/**
	 * Lock implement
	 */
	private class RedisLock implements Lock {
		private final RLock lock;

		public RedisLock(RLock lock) {
			this.lock = lock;
		}

		@Override
		public void release() {
			lock.unlock();
		}
	}

}
