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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;

/**
 *
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisAsyncMap<K, V> implements AsyncMap<K, V> {
	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(RedisAsyncMap.class);

	protected final Vertx vertx;
	protected final RMapCache<K, V> map;

	public RedisAsyncMap(Vertx vertx, RedissonClient redisson, String name) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.vertx = vertx;
		this.map = redisson.getMapCache(name);
	}

	@Override
	public void get(K k, Handler<AsyncResult<V>> resultHandler) {
		map.getAsync(k)
				.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
	}

	@Override
	public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
		map.fastPutAsync(k, v).whenComplete(
				(added, e) -> completionHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture()));
	}

	@Override
	public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
		map.fastPutAsync(k, v, ttl, TimeUnit.MILLISECONDS).whenComplete(
				(added, e) -> completionHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture()));
	}

	@Override
	public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
		map.putIfAbsentAsync(k, v).whenComplete((previousValue, e) -> completionHandler
				.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(previousValue)));
	}

	@Override
	public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
		map.putIfAbsentAsync(k, v, ttl, TimeUnit.MILLISECONDS).whenComplete((previousValue, e) -> completionHandler
				.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(previousValue)));
	}

	@Override
	public void remove(K k, Handler<AsyncResult<V>> resultHandler) {
		map.removeAsync(k).whenComplete((previousValue, e) -> resultHandler
				.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(previousValue)));
	}

	@Override
	public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
		map.removeAsync(k, v).whenComplete(
				(removed, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(removed)));
	}

	@Override
	public void replace(K k, V v, Handler<AsyncResult<V>> resultHandler) {
		map.replaceAsync(k, v).whenComplete((previousValue, e) -> resultHandler
				.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(previousValue)));
	}

	@Override
	public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
		map.replaceAsync(k, oldValue, newValue).whenComplete(
				(replaced, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(replaced)));
	}

	@Override
	public void clear(Handler<AsyncResult<Void>> resultHandler) {
		map.deleteAsync().whenComplete(
				(deleted, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture()));
	}

	@Override
	public void size(Handler<AsyncResult<Integer>> resultHandler) {
		map.sizeAsync()
				.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
	}

	@Override
	public void keys(Handler<AsyncResult<Set<K>>> resultHandler) {
		map.readAllKeySetAsync()
				.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
	}

	@Override
	public void values(Handler<AsyncResult<List<V>>> resultHandler) {
		map.readAllValuesAsync().whenComplete((v, e) -> {
			resultHandler.handle(e != null ? Future.failedFuture(e)
					: Future.succeededFuture((v instanceof List) ? (List<V>) v : new ArrayList<>(v)));
		});
	}

	@Override
	public void entries(Handler<AsyncResult<Map<K, V>>> resultHandler) {
		map.readAllMapAsync()
				.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
	}

}
