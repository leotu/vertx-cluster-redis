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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.redisson.api.RBatch;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;

/**
 * batch.atomic return's value must using Codec to Object. (always return String type)
 * 
 * @see org.redisson.RedissonSetMultimapValues
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class RedisAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {
	private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMap.class);
	// private static boolean debug = false;

	protected final ConcurrentMap<K, AtomicReference<RedisChoosableSet<V>>> choosableSetPtr = new ConcurrentHashMap<>();
	protected final RedissonClient redisson;
	protected final Vertx vertx;
	protected final RSetMultimap<K, V> mmap;
	protected final String name;

	public RedisAsyncMultiMap(Vertx vertx, RedissonClient redisson, String name, Codec codec) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.vertx = vertx;
		this.redisson = redisson;
		this.name = name;
		this.mmap = createMultimap(this.redisson, this.name, codec);
	}

	/**
	 * Here you can customize(override method) a "Codec"
	 * 
	 * @see org.redisson.codec.JsonJacksonCodec
	 */
	protected RSetMultimap<K, V> createMultimap(RedissonClient redisson, String name, Codec codec) {
		if (codec == null) {
			return redisson.getSetMultimap(name);
			// return redisson.getSetMultimapCache(name);
		} else {
			return redisson.getSetMultimap(name, codec);
		}
	}

	@Override
	public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
		Context context = vertx.getOrCreateContext();
		mmap.putAsync(k, v).whenComplete((added, e) -> context.runOnContext(vd -> //
		completionHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture())) //
		);
	}

	/**
	 * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus#sendOrPub(SendContextImpl<T>)
	 */
	@Override
	public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
		Context context = vertx.getOrCreateContext();
		mmap.getAllAsync(k).whenComplete((v, e) -> { // v class is java.util.LinkedHashSet
			if (e != null) {
				context.runOnContext(vd -> resultHandler.handle(Future.failedFuture(e)));
			} else {
				AtomicReference<RedisChoosableSet<V>> currentRef = getCurrentRef(k);
				RedisChoosableSet<V> newSet = new RedisChoosableSet<>(v, currentRef);
				// XXX
				if (!newSet.isNewSet()) {
					context.runOnContext(vd -> resultHandler.handle(Future.succeededFuture(currentRef.get())));
				} else {
					newSet.moveToCurrent();
					context.runOnContext(vd -> resultHandler.handle(Future.succeededFuture(newSet)));
				}
			}
		});

	}

	@Override
	public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
		Context context = vertx.getOrCreateContext();
		mmap.removeAsync(k, v).whenComplete((removed, e) -> context.runOnContext(vd -> //
		completionHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(removed))) //
		);
	}

	@Override
	public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
		removeAllMatching(value -> value == v || value.equals(v), completionHandler);
	}

	/**
	 * Remove values which satisfies the given predicate in all keys.
	 */
	@Override
	public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
		Context context = vertx.getOrCreateContext();
		batchRemoveAllMatching(p, ar -> {
			if (ar.failed()) {
				context.runOnContext(vd -> completionHandler.handle(Future.failedFuture(ar.cause())));
			} else {
				context.runOnContext(vd -> completionHandler.handle(Future.succeededFuture(ar.result())));
			}
		});
	}

	@SuppressWarnings({ "rawtypes" })
	private void batchRemoveAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
		mmap.readAllKeySetAsync().whenComplete((keys, e) -> {
			if (e != null) {
				log.warn("error: {}", e.toString());
				completionHandler.handle(Future.failedFuture(e));
			} else {
				if (keys.isEmpty()) {
					completionHandler.handle(Future.succeededFuture());
					return;
				}
				Map<K, Future> keyFutures = new HashMap<>(keys.size());
				keys.forEach(key -> {
					keyFutures.put(key, Future.future());
				});
				for (K key : keys) {
					Future keyFuture = keyFutures.get(key);
					mmap.getAllAsync(key).whenComplete((values, e2) -> {
						if (e2 != null) {
							keyFuture.fail(e2);
						} else {
							if (values.isEmpty()) {
								keyFuture.complete();
							} else {
								// List<Future> valueFutures = new ArrayList<>();
								// values.forEach(value -> {
								// if (p.test(value)) { // XXX
								// log.debug("add remove key={}, value={}", key, value);
								// Future<Boolean> valueFuture = Future.future();
								// valueFutures.add(valueFuture);
								// mmap.removeAsync(key, value).whenComplete((removed, e3) -> {
								// if (e3 != null) {
								// valueFuture.fail(e3);
								// } else {
								// valueFuture.complete(removed);
								// }
								// });
								// } else {
								// log.debug("skip remove key={}, value={}", key, value);
								// }
								// });
								// CompositeFuture.join(valueFutures).setHandler(keyFuture); // XXX: join or all ?
								// ========================

								List<V> deletedList = new ArrayList<>();
								values.forEach(value -> {
									if (p.test(value)) { // XXX
										deletedList.add(value);
									}
								});

								if (deletedList.isEmpty()) {
									keyFuture.complete();
								} else {
									RBatch batch = redisson.createBatch();
									deletedList.forEach(value -> {
										mmap.removeAsync(key, value);
									});
									batch.atomic().skipResult().executeAsync().whenCompleteAsync((result, e3) -> {
										if (e != null) {
											log.warn("key: {}, error: {}", key, e3.toString());
											keyFuture.fail(e3);
										} else { // XXX: skipResult() ==> result.class=<null>, result=null
											keyFuture.complete();
										}
									});
								}
							}
						}
					});
				}
				//
				CompositeFuture.join(new ArrayList<>(keyFutures.values())).setHandler(
						ar -> completionHandler.handle(ar.failed() ? Future.failedFuture(ar.cause()) : Future.succeededFuture()));
			}
		});
	}

	private AtomicReference<RedisChoosableSet<V>> getCurrentRef(K k) {
		AtomicReference<RedisChoosableSet<V>> currentRef = choosableSetPtr.get(k);
		if (currentRef == null) {
			currentRef = new AtomicReference<>();
			AtomicReference<RedisChoosableSet<V>> previous = choosableSetPtr.putIfAbsent(k, currentRef);
			if (previous != null) {
				currentRef = previous;
			}
		}
		return currentRef;
	}

	@Override
	public String toString() {
		return super.toString() + "{name=" + name + "}";
	}

}
