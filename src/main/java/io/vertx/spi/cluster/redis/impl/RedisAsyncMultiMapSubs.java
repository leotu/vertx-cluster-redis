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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.spi.cluster.redis.AsyncLocalLock;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * SUBS_MAP_NAME = "__vertx.subs"
 * <p/>
 * When last node disconnected will still keep it's subs address. (Don't remove last node subs, "__vertx.subs" are not
 * empty !)
 * 
 * @see io.vertx.core.net.impl.ServerID
 * @see org.redisson.codec.JsonJacksonCodec
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisAsyncMultiMapSubs extends RedisAsyncMultiMap<String, ClusterNodeInfo> {
	private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMapSubs.class);

	static private boolean debug = false;

	private final RedisClusterManager clusterManager;

	public RedisAsyncMultiMapSubs(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson, String name) {
		super(vertx, redisson, name);
		this.clusterManager = clusterManager;
	}

	@Override
	public void add(String k, ClusterNodeInfo v, Handler<AsyncResult<Void>> completionHandler) {
		super.add(k, v, completionHandler);
	}

	@Override
	public void get(String k, Handler<AsyncResult<ChoosableIterable<ClusterNodeInfo>>> resultHandler) {
		super.get(k, resultHandler);
	}

	@Override
	public void remove(String k, ClusterNodeInfo v, Handler<AsyncResult<Boolean>> completionHandler) {
		super.remove(k, v, completionHandler);
	}

	@Override
	public void removeAllForValue(ClusterNodeInfo v, Handler<AsyncResult<Void>> completionHandler) {
		removeAllMatching(value -> value == v || value.equals(v), completionHandler);
	}

	/**
	 * Remove values which satisfies the given predicate in all keys.
	 * 
	 * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus#setClusterViewChangedHandler
	 */
	@Override
	public void removeAllMatching(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler) {
		batchRemoveAllMatching(p, completionHandler);
	}

	private void batchRemoveAllMatching(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler) {
		List<Map.Entry<String, ClusterNodeInfo>> deletedList = new ArrayList<>();
		mmap.entries().forEach(entry -> {
			String key = entry.getKey();
			ClusterNodeInfo value = entry.getValue();
			if (p.test(value)) { // XXX: "!members.contains(ci.nodeId)"
				deletedList.add(entry);
				if (debug) {
					log.debug("add remove key={}, value.class={}, value={}", key, value.getClass().getName(), value);
				}
			} else {
				if (debug) {
					log.debug("skip remove key={} value.class={}, value={}", key, value.getClass().getName(), value);
				}
			}
		});

		if (!deletedList.isEmpty()) {
			RBatch batch = redisson.createBatch();
			deletedList.forEach(entry -> {
				mmap.removeAsync(entry.getKey(), entry.getValue());
			});

			batch.atomic().skipResult().executeAsync().whenCompleteAsync((result, e) -> {
				if (e != null) {
					log.warn("error: {}", e.toString());
					completionHandler.handle(Future.failedFuture(e));
				} else { // XXX: skipResult() ==> result.class=<null>, result=null
					completionHandler.handle(Future.succeededFuture());
				}
			});
		}
	}

	/**
	 * Remove values which satisfies the given predicate in all keys.
	 * 
	 * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus#setClusterViewChangedHandler
	 */
	// @Override
	@Deprecated
	public void removeAllMatching_(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler) {
		final List<Lock> lockList = new ArrayList<>();
		final String nodeID = clusterManager.getNodeID();
		batchRemoveAllMatching_(p, ar -> {
			if (!lockList.isEmpty()) {
				log.warn("(!lockList.isEmpty()), nodeID: {}, lockList.size: {}", nodeID, lockList.size());
				releaseLock(lockList); // try again !
			}
			completionHandler.handle(ar.failed() ? Future.failedFuture(ar.cause()) : Future.succeededFuture());
		}, lockList);
	}

	@SuppressWarnings("rawtypes")
	@Deprecated
	private void batchRemoveAllMatching_(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler,
			final List<Lock> lockList) {
		List<Map.Entry<String, ClusterNodeInfo>> deletedList = new ArrayList<>();
		final String nodeID = clusterManager.getNodeID();

		int timeoutInSeconds = 3; // XXX
		Map<String, Future<Lock>> acquireLocks = new HashMap<>(); // <nodeID, ...>
		mmap.entries().forEach(entry -> {
			String key = entry.getKey();
			ClusterNodeInfo value = entry.getValue();
			String deleteNodeId = value.nodeId;
			if (p.test(value)) { // XXX: "!members.contains(ci.nodeId)"
				deletedList.add(entry);
				if (debug) {
					log.debug("prepare add remove nodeID={}, key={}, deleteNodeId={}", nodeID, key, deleteNodeId);
				}

				if (!acquireLocks.containsKey(deleteNodeId)) {
					Future<Lock> f = Future.future();
					acquireLocks.put(deleteNodeId, f);
					AsyncLocalLock.acquireLockWithTimeout(vertx, deleteNodeId, timeoutInSeconds, f);
				}
			} else {
				if (debug) {
					log.debug("prepare skip remove nodeID={}, key={}, deleteNodeId={}", nodeID, key, deleteNodeId);
				}
			}
		});

		if (!deletedList.isEmpty()) {
			if (debug) {
				log.debug("nodeID={}, acquireLocks: {}", nodeID, acquireLocks.keySet());
			}
			RBatch batch = redisson.createBatch();
			deletedList.forEach(entry -> {
				String key = entry.getKey();
				ClusterNodeInfo value = entry.getValue();
				if (debug) {
					log.debug("nodeID={}, key: {}, value: {}", nodeID, key, value);
				}
				mmap.removeAsync(key, value).whenComplete((ok, e) -> {
					if (debug) {
						log.debug("prepare removeAsync, nodeID={}, key: {}, value: {}, ok: {}", nodeID, key, value, ok);
					}
					if (e != null) {
						log.warn("prepare removeAsync failed, nodeID: {}, key: {}, value: {}, error: {}", nodeID, key, value,
								e.toString());
					}
				});
			});
			if (debug) {
				log.debug("*** nodeID={}, deletedList.size: {}, acquireLocks.size={}", nodeID, deletedList.size(),
						acquireLocks.size());
			}
			CompositeFuture.join(new ArrayList<Future>(acquireLocks.values())).setHandler(ar -> {
				if (ar.failed()) {
					log.warn("nodeID: {}, deletedList.size: {},  acquireLocks.size: {}, lockList.size: {}, error: {}", nodeID,
							deletedList.size(), acquireLocks.size(), lockList.size(), ar.cause().toString());
				}
				acquireLocks.entrySet().forEach(entry -> {
					String deleteNodeId = entry.getKey();
					Future<Lock> f = entry.getValue();
					if (f.succeeded()) {
						lockList.add(f.result());
					} else {
						if (debug) {
							log.debug("nodeID={}, deleteNodeId={}, error={}", nodeID, deleteNodeId, f.cause().toString());
						}
					}
				});
				if (debug) {
					log.debug("nodeID={}, deletedList.size={}, acquireLocks.size={}, lockList.size={}", nodeID,
							deletedList.size(), acquireLocks.size(), lockList.size());
				}
				if (acquireLocks.size() != lockList.size()) {
					log.warn("(acquireLocks.size() != lockList.size()), nodeID: {}, acquireLocks.size: {}, lockList.size: {}",
							nodeID, acquireLocks.size(), lockList.size());
				}

				batch.atomic().skipResult().executeAsync().whenCompleteAsync((result, e) -> {
					if (e != null) {
						log.warn("nodeID: {}, error: {}", nodeID, e.toString());
						releaseLock(lockList);
						completionHandler.handle(Future.failedFuture(e));
					} else { // XXX: skipResult() ==> result.class=<null>, result=null
						releaseLock(lockList);
						completionHandler.handle(Future.succeededFuture());
					}
				});

			});
		} else {
			if (debug) {
				log.debug("nodeID={}, deletedList.size={}", nodeID, deletedList.size());
			}
		}
	}

	private void releaseLock(List<io.vertx.core.shareddata.Lock> lockList) {
		lockList.forEach(lock -> {
			AsyncLocalLock.releaseLock(lock);
		});
		lockList.clear();
	}

	// public void close() {
	// }
}
