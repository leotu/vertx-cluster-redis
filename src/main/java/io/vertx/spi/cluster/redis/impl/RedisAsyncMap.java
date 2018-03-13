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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.redisson.Redisson;
import org.redisson.RedissonMapCache;
import org.redisson.RedissonObject;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.client.protocol.convertor.LongReplayConvertor;
import org.redisson.command.CommandAsyncExecutor;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.spi.cluster.redis.ExpirableAsync;
import io.vertx.spi.cluster.redis.NonPublicAPI.Reflection;

/**
 *
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class RedisAsyncMap<K, V> implements AsyncMap<K, V>, ExpirableAsync<K> {
	// private static final Logger log = LoggerFactory.getLogger(RedisAsyncMap.class);

	protected final RedisStrictCommand<Long> ZSCORE_LONG = new RedisStrictCommand<Long>("ZSCORE",
			new LongReplayConvertor()); // RedisCommands.ZSCORE

	protected final Vertx vertx;
	protected final RedissonClient redisson;
	protected final RMapCache<K, V> map;
	protected final String name;

	public RedisAsyncMap(Vertx vertx, RedissonClient redisson, String name) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.vertx = vertx;
		this.redisson = redisson;
		this.name = name;
		this.map = createRMapCache(this.redisson, this.name);
	}

	/**
	 * Here you can customize(override method) a "Codec"
	 * 
	 * @see org.redisson.codec.JsonJacksonCodec
	 * @see org.redisson.codec.FstCodec
	 */
	protected RMapCache<K, V> createRMapCache(RedissonClient redisson, String name) {
		return redisson.getMapCache(name, new RedisMapCodec());
		// return redisson.getMapCache(name);
	}

	@Override
	public void get(K k, Handler<AsyncResult<V>> resultHandler) {
		map.getAsync(k).whenComplete((v, e) -> {
			resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v));
		});
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

	/**
	 * @return previous value always null
	 */
	@Override
	public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
		map.putIfAbsentAsync(k, v).whenComplete((previousValue, e) -> completionHandler
				.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(previousValue)));
	}

	/**
	 * @return previous value always null
	 */
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

	/**
	 * @return previous (old) value
	 */
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

	/**
	 * https://redis.io/commands/zadd
	 * 
	 * @return TTL in milliseconds
	 * @see org.redisson.RedissonMapCache#getTimeoutSetNameByKey
	 * @see org.redisson.RedissonObject#encodeMapKey
	 */
	@Override
	public void refreshIfPresent(K k, long timeToLive, TimeUnit timeUnit, Handler<AsyncResult<Long>> resultHandler) {
		final String key = Reflection.callMethod(map, RedissonMapCache.class, "getTimeoutSetNameByKey",
				new Class<?>[] { Object.class }, new Object[] { name });

		final ByteBuf encodeMapKey = Reflection.callMethod(map, RedissonObject.class, "encodeMapKey",
				new Class<?>[] { Object.class }, new Object[] { k });
		final String field = encodeMapKey.toString(CharsetUtil.UTF_8);

		final Long ttl = timeUnit.toMillis(timeToLive);
		long currentTime = System.currentTimeMillis() + ttl;

		// final ByteBuf encodeMapValue = Reflection.callMethod(map, RedissonObject.class, "encodeMapValue",
		// new Class<?>[] { Object.class }, new Object[] { field });

		final Redisson redissonImpl = ((Redisson) redisson);
		final CommandAsyncExecutor commandExecutor = redissonImpl.getCommandExecutor();

		// XX: Only update elements that already exist. Never add elements.
		String zaddOptions = "XX";
		commandExecutor.writeAsync(key, LongCodec.INSTANCE, RedisCommands.ZADD_INT, key, zaddOptions, currentTime, field)
				.whenCompleteAsync((value, e) -> {
					if (e == null) { // java.lang.Boolean / java.lang.Long
						Long numOfAdded = (Long) value; // num.longValue() == 0
						// Boolean added = (Boolean) value;
						resultHandler.handle(Future.succeededFuture(numOfAdded)); // (Boolean) value)
					} else {
						resultHandler.handle(Future.failedFuture(e));
					}
				});

	}

	/**
	 * @return TTL in milliseconds
	 * @see org.redisson.RedissonMapCache#getTimeoutSetNameByKey
	 * @see org.redisson.RedissonObject#encodeMapKey
	 */
	@Override
	public void getTTL(K k, Handler<AsyncResult<Long>> resultHandler) {
		// final String key = "redisson__timeout__set:{" + name + "}"; // XXX
		final String key = Reflection.callMethod(map, RedissonMapCache.class, "getTimeoutSetNameByKey",
				new Class<?>[] { Object.class }, new Object[] { name });

		// final String field = "\"" + k + "\""; // XXX
		final ByteBuf encodeMapKey = Reflection.callMethod(map, RedissonObject.class, "encodeMapKey",
				new Class<?>[] { Object.class }, new Object[] { k });
		final String field = encodeMapKey.toString(CharsetUtil.UTF_8);

		final Redisson redissonImpl = ((Redisson) redisson);
		final CommandAsyncExecutor commandExecutor = redissonImpl.getCommandExecutor();

		commandExecutor.readAsync(key, LongCodec.INSTANCE, ZSCORE_LONG, key, field).whenCompleteAsync((value, e) -> {
			if (e == null) {
				if (value == null) {
					resultHandler.handle(Future.succeededFuture(0L));
				} else {
					Long val = (Long) value; //
					// log.debug("### val: {}", val);
					if (val.longValue() == 0) {
						resultHandler.handle(Future.succeededFuture(0L));
					} else {
						long nowMillis = System.currentTimeMillis();
						long valMillis = val;
						long ttlMillis = valMillis - nowMillis;

						LocalDateTime now = new Date(nowMillis).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime(); // LocalDateTime.now();
						LocalDateTime valTime = new Date(val).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
						if (now.isAfter(valTime)) {
							resultHandler.handle(Future.succeededFuture(0L));
						} else {
							long nowMilli = now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
							long valMilli = valTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
							long ttl = valMilli - nowMilli;
							if (ttl != ttlMillis) { // debugging
								resultHandler.handle(Future
										.failedFuture(new Exception("(ttl != ttlMillis), ttl: " + ttl + ", ttlMillis: " + ttlMillis)));
							}
							resultHandler.handle(Future.succeededFuture(ttl <= 0 ? 0 : ttl));
						}
					}
				}
			} else {
				resultHandler.handle(Future.failedFuture(e));
			}
		});
	}

}
