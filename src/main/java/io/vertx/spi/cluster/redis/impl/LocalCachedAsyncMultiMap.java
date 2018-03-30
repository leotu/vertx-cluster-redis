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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.redisson.api.RFuture;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.Factory.LocalCached;

/**
 *
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class LocalCachedAsyncMultiMap<K, V> implements AsyncMultiMap<K, V>, LocalCached {
	private static final Logger log = LoggerFactory.getLogger(LocalCachedAsyncMultiMap.class);

	private final AsyncMultiMap<K, V> delegate;
	private final Vertx vertx;
	private final ClusterManager clusterManager;
	protected final String topicName;

	protected final ConcurrentMap<K, ChoosableIterable<V>> choosableSetLocalCached = new ConcurrentHashMap<>();

	protected RTopic<K> subsTopic;
	protected int topicListenerId;
	protected long timerId;
	protected LocalDateTime clearAllTimestamp;

	public LocalCachedAsyncMultiMap(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson,
			AsyncMultiMap<K, V> delegate, int timeoutInSecoinds, String topicName) {
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.delegate = delegate;
		this.topicName = topicName;
		this.subsTopic = redisson.getTopic(this.topicName);
		this.topicListenerId = this.subsTopic.addListener(new MessageListener<K>() {
			@Override
			public void onMessage(String channel, K key) {
				if (key == null) {
					discard();
				} else {
					ChoosableIterable<V> previous = choosableSetLocalCached.remove(key);
					if (previous != null) {
						log.debug("topicListenerId: {}, key: {}, previous: {}", topicListenerId, key, previous);
					}
				}
			}
		});
		this.timerId = vertx.setPeriodic(1000 * timeoutInSecoinds, id -> {
			if (clearAllTimestamp == null) {
				discard();
			} else {
				LocalDateTime now = LocalDateTime.now();
				LocalDateTime checkTime = clearAllTimestamp.plusSeconds(timeoutInSecoinds / 2);
				if (now.isAfter(checkTime)) {
					discard();
				}
			}
		});
	}

	public void close() {
		discard();
		if (timerId > 0) {
			vertx.cancelTimer(timerId);
			timerId = 0;
		}
		if (topicListenerId > 0) {
			subsTopic.removeListener(topicListenerId);
			topicListenerId = 0;
		}
	}

	@Override
	public void discard() {
		clearAllTimestamp = LocalDateTime.now();
		if (!choosableSetLocalCached.isEmpty()) {
			choosableSetLocalCached.clear();
		}
	}

	@Override
	public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
		delegate.add(k, v, ar -> {
			if (ar.succeeded()) {
				publishLog(subsTopic.publishAsync(k), k, "add");
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});

	}

	@Override
	public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
		ChoosableIterable<V> valuesLocalCached = choosableSetLocalCached.get(k);
		if (valuesLocalCached != null && !valuesLocalCached.isEmpty()) {
			resultHandler.handle(Future.succeededFuture(valuesLocalCached));
			return;
		}

		delegate.get(k, ar -> {
			if (ar.succeeded()) {
				ChoosableIterable<V> result = ar.result();
				choosableSetLocalCached.put(k, result);
				resultHandler.handle(Future.succeededFuture(result));
			} else {
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
		delegate.remove(k, v, ar -> {
			publishLog(subsTopic.publishAsync(k), k, "remove");
			if (ar.succeeded()) {
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
		delegate.removeAllForValue(v, ar -> {
			publishLog(subsTopic.publishAsync(null), null, "removeAllForValue");
			if (ar.succeeded()) {
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
		delegate.removeAllMatching(p, ar -> {
			publishLog(subsTopic.publishAsync(null), null, "removeAllMatching");
			if (ar.succeeded()) {
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	private void publishLog(RFuture<Long> fu, K k, String method) {
		fu.whenComplete((numOfClientsRreceived, error) -> {
			if (error == null) {
				int numOfNodes = clusterManager.getNodes().size();
				if (numOfClientsRreceived != numOfNodes) {
					log.debug("{}({}) numOfClientsRreceived: {}, numOfNodes: {}", method, k, numOfClientsRreceived, numOfNodes);
				}
			} else {
				log.warn("{}({}), error: {}", method, k, error.toString());
			}
		});
	}

	@Override
	public String toString() {
		return delegate.toString() + "{topicName=" + topicName + "}";
	}

}
