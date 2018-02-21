
package io.vertx.spi.cluster.redis.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;

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
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {
	// private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMap.class);

	protected ConcurrentMap<K, AtomicReference<V>> choosableSetPtr = new ConcurrentHashMap<>();
	protected final RedissonClient redisson;
	protected final RSetMultimap<K, V> mmap;

	public RedisAsyncMultiMap(Vertx vertx, RedissonClient redisson, String name) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.redisson = redisson;
		this.mmap = redisson.getSetMultimap(name);
	}
	
	public RedisAsyncMultiMap(Vertx vertx, RedissonClient redisson, RSetMultimap<K, V> mmap) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(mmap, "mmap");
		this.redisson = redisson;
		this.mmap = mmap;
	}

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

				// if (values.isEmpty()) {
				// log.debug("values.size={}, current={}", values.size(), values.getCurrent());
				// }
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
		// removeAllMatching(value -> value.hashCode() == v.hashCode(), completionHandler);
		removeAllMatching(value -> value.equals(v), completionHandler);
	}

	/**
	 * Remove values which satisfies the given predicate in all keys.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
		mmap.readAllKeySetAsync().whenComplete((keys, e) -> {
			if (e != null) {
				completionHandler.handle(Future.failedFuture(e));
			} else {
				List<Future> keyFutures = new ArrayList<>(keys.size());
				for (int i = 0; i < keys.size(); i++) {
					keyFutures.add(Future.future());
				}
				AtomicInteger idx = new AtomicInteger(0);
				for (K key : keys) {
					Future keyFuture = keyFutures.get(idx.getAndIncrement());
					mmap.getAllAsync(key).whenComplete((values, e2) -> {
						if (e2 != null) {
							keyFuture.fail(e2);
						} else {
							if (values.isEmpty()) {
								// log.debug("values.isEmpty(), key={}", key);
								keyFuture.complete();
							} else {
								List<Future> valueFutures = new ArrayList<>();
								values.forEach(value -> {
									if (p.test(value)) { // XXX
										Future<Boolean> valueFuture = Future.future();
										valueFutures.add(valueFuture);
										mmap.removeAsync(key, value).whenComplete((removed, e3) -> {
											if (e3 != null) {
												valueFuture.fail(e3);
											} else {
												valueFuture.complete(removed);
											}
										});
									}
								});
								CompositeFuture.all(valueFutures).setHandler(keyFuture);
							}
						}
					});
				}
				//
				CompositeFuture.all(keyFutures).setHandler(ar -> completionHandler
						.handle(ar.failed() ? Future.failedFuture(ar.cause()) : Future.succeededFuture()));
			}
		});
	}

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
