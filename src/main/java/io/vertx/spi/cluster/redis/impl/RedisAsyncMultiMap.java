
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;

/**
 * 
 * @see org.redisson.codec.JsonJacksonCodec
 * @see org.redisson.RedissonSetMultimapValues
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {
	private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMap.class);

	static private boolean debug = false;

	protected ConcurrentMap<K, AtomicReference<V>> choosableSetPtr = new ConcurrentHashMap<>();
	protected final RedissonClient redisson;
	protected final RSetMultimap<K, V> mmap;
	protected final Codec codec;
	protected final String name;

	public RedisAsyncMultiMap(Vertx vertx, RedissonClient redisson, String name) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.redisson = redisson;
		this.name = name;
		this.mmap = redisson.getSetMultimap(name);
		this.codec = mmap.getCodec();
	}

	// public RedisAsyncMultiMap(Vertx vertx, RedissonClient redisson, String name, RSetMultimap<K, V> mmap) {
	// Objects.requireNonNull(redisson, "redisson");
	// Objects.requireNonNull(mmap, "mmap");
	// this.redisson = redisson;
	// this.name = name;
	// this.mmap = mmap;
	// this.codec = mmap.getCodec();
	// }

	@Override
	public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
		mmap.putAsync(k, v).whenComplete(
				(added, e) -> completionHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture()));
	}

	@Override
	public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
		mmap.getAllAsync(k).whenComplete((v, e) -> { // java.util.LinkedHashSet
			if (e != null) {
				resultHandler.handle(Future.failedFuture(e));
			} else {
				RedisChoosableSet<V> values = new RedisChoosableSet<>(v != null ? v.size() : 0, getCurrentPointer(k));
				values.addAll(v);
				values.moveToCurrent();
				resultHandler.handle(Future.succeededFuture(values));
			}
		});
	}

	@Override
	public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
		mmap.removeAsync(k, v).whenComplete((removed, e) -> completionHandler
				.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(removed)));
	}

	@Override
	public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
		removeAllMatching(value -> value == v || value.equals(v), completionHandler);
	}

	/**
	 * Remove values which satisfies the given predicate in all keys.
	 */
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
		mmap.readAllKeySetAsync().whenComplete((keys, e) -> {
			if (e != null) {
				log.warn("error={}", e.toString());
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
								// CompositeFuture.all(valueFutures).setHandler(keyFuture);
								// ========================

								List<V> deletedList = new ArrayList<>();
								values.forEach(value -> {
									if (p.test(value)) { // XXX
										deletedList.add(value);
										if (debug) {
											log.debug("add remove key={}, value.class={}, value={}", key,
													value.getClass().getName(), value);
										}
									} else {
										if (debug) {
											log.debug("skip remove key={} value.class={}, value={}", key,
													value.getClass().getName(), value);
										}
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
											log.warn("key={}, error={}", key, e3.toString());
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
				CompositeFuture.all(new ArrayList<>(keyFutures.values())).setHandler(ar -> completionHandler
						.handle(ar.failed() ? Future.failedFuture(ar.cause()) : Future.succeededFuture()));
			}
		});
	}

	// @SuppressWarnings("unchecked")
	// protected V mapValueDecoded(Codec codec, String value) {
	// return (V) value;
	// }

	private AtomicReference<V> getCurrentPointer(K k) {
		AtomicReference<V> current = choosableSetPtr.get(k);
		if (current == null) {
			current = new AtomicReference<>();
			AtomicReference<V> previous = choosableSetPtr.putIfAbsent(k, current);
			if (previous != null) {
				current = previous;
			}
		}
		return current;
	}
}
