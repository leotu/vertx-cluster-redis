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
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.redisson.api.RBatch;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;

/**
 * SUBS_MAP_NAME = "__vertx.subs"
 * <p/>
 * When last node disconnected will still keep it's subs address. (Don't remove last node subs, "__vertx.subs" are not
 * empty !)
 * 
 * @see io.vertx.core.net.impl.ServerID
 * @see org.redisson.codec.JsonJacksonCodec
 * @see io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class RedisAsyncMultiMapSubs extends RedisAsyncMultiMap<String, ClusterNodeInfo> {
	private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMapSubs.class);

	@SuppressWarnings("unused")
	private final ClusterManager clusterManager;

	public RedisAsyncMultiMapSubs(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson, String name) {
		super(vertx, redisson, name, null);
		this.clusterManager = clusterManager;
	}

	/**
	 * 
	 * @see org.redisson.client.codec.StringCodec
	 * @see org.redisson.codec.JsonJacksonCodec
	 */
	@Override
	protected RSetMultimap<String, ClusterNodeInfo> createMultimap(RedissonClient redisson, String name, Codec codec) {
		RSetMultimap<String, ClusterNodeInfo> mmap = redisson.getSetMultimap(name, new KeyValueCodec( //
				JsonJacksonCodec.INSTANCE.getValueEncoder(), //
				JsonJacksonCodec.INSTANCE.getValueDecoder(), //
				StringCodec.INSTANCE.getMapKeyEncoder(), //
				StringCodec.INSTANCE.getMapKeyDecoder(), //
				JsonJacksonCodec.INSTANCE.getValueEncoder(), //
				JsonJacksonCodec.INSTANCE.getValueDecoder()) //
		);
		return mmap;
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
		Context context = vertx.getOrCreateContext();
		batchRemoveAllMatching(p, ar -> {
			if (ar.failed()) {
				context.runOnContext(vd -> completionHandler.handle(Future.failedFuture(ar.cause())));
			} else {
				context.runOnContext(vd -> completionHandler.handle(Future.succeededFuture(ar.result())));
			}
		});
	}

	private void batchRemoveAllMatching(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler) {
		List<Map.Entry<String, ClusterNodeInfo>> deletedList = new ArrayList<>();
		mmap.entries().forEach(entry -> {
			ClusterNodeInfo value = entry.getValue();
			if (p.test(value)) { // XXX: "!members.contains(ci.nodeId)"
				deletedList.add(entry);
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

	// /**
	// * Remove values which satisfies the given predicate in all keys.
	// *
	// * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus#setClusterViewChangedHandler
	// */
	// // @Override
	// @Deprecated
	// public void removeAllMatching_(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler) {
	// final List<Lock> lockList = new ArrayList<>();
	// final String nodeID = clusterManager.getNodeID();
	// batchRemoveAllMatching_(p, ar -> {
	// if (!lockList.isEmpty()) {
	// log.info("(!lockList.isEmpty()), nodeID: {}, lockList.size: {}", nodeID, lockList.size());
	// releaseLock(lockList); // try again !
	// }
	// completionHandler.handle(ar.failed() ? Future.failedFuture(ar.cause()) : Future.succeededFuture());
	// }, lockList);
	// }
	//
	// @SuppressWarnings("rawtypes")
	// @Deprecated
	// private void batchRemoveAllMatching_(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler,
	// final List<Lock> lockList) {
	// List<Map.Entry<String, ClusterNodeInfo>> deletedList = new ArrayList<>();
	// final String nodeID = clusterManager.getNodeID();
	//
	// int timeoutInSeconds = 3; // XXX
	// Map<String, Future<Lock>> acquireLocks = new HashMap<>(); // <nodeID, ...>
	// mmap.entries().forEach(entry -> {
	// ClusterNodeInfo value = entry.getValue();
	// String deleteNodeId = value.nodeId;
	// if (p.test(value)) { // XXX: "!members.contains(ci.nodeId)"
	// deletedList.add(entry);
	// if (!acquireLocks.containsKey(deleteNodeId)) {
	// Future<Lock> f = Future.future();
	// acquireLocks.put(deleteNodeId, f);
	// AsyncLocalLock.acquireLockWithTimeout(vertx, deleteNodeId, timeoutInSeconds, f);
	// }
	// }
	// });
	//
	// if (!deletedList.isEmpty()) {
	// RBatch batch = redisson.createBatch();
	// deletedList.forEach(entry -> {
	// String key = entry.getKey();
	// ClusterNodeInfo value = entry.getValue();
	// mmap.removeAsync(key, value).whenComplete((ok, e) -> {
	// if (e != null) {
	// log.warn("prepare removeAsync failed, nodeID: {}, key: {}, value: {}, error: {}", nodeID, key, value,
	// e.toString());
	// }
	// });
	// });
	//
	// CompositeFuture.join(new ArrayList<Future>(acquireLocks.values())).setHandler(ar -> {
	// if (ar.failed()) {
	// log.warn("nodeID: {}, deletedList.size: {}, acquireLocks.size: {}, lockList.size: {}, error: {}", nodeID,
	// deletedList.size(), acquireLocks.size(), lockList.size(), ar.cause().toString());
	// }
	// acquireLocks.entrySet().forEach(entry -> {
	// Future<Lock> f = entry.getValue();
	// if (f.succeeded()) {
	// lockList.add(f.result());
	// }
	// });
	//
	// if (acquireLocks.size() != lockList.size()) {
	// log.warn("(acquireLocks.size() != lockList.size()), nodeID: {}, acquireLocks.size: {}, lockList.size: {}",
	// nodeID, acquireLocks.size(), lockList.size());
	// }
	//
	// batch.atomic().skipResult().executeAsync().whenCompleteAsync((result, e) -> {
	// if (e != null) {
	// log.warn("nodeID: {}, error: {}", nodeID, e.toString());
	// releaseLock(lockList);
	// completionHandler.handle(Future.failedFuture(e));
	// } else { // XXX: skipResult() ==> result.class=<null>, result=null
	// releaseLock(lockList);
	// completionHandler.handle(Future.succeededFuture());
	// }
	// });
	//
	// });
	// }
	// }
	//
	// @Deprecated
	// private void releaseLock(List<io.vertx.core.shareddata.Lock> lockList) {
	// lockList.forEach(lock -> {
	// AsyncLocalLock.releaseLock(lock);
	// });
	// lockList.clear();
	// }
}
