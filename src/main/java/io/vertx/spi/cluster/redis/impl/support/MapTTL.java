/*
 * Copyright (c) 2019 The original author or authors
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
package io.vertx.spi.cluster.redis.impl.support;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Objects;
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
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.spi.cluster.redis.ExpirableAsync;
import io.vertx.spi.cluster.redis.impl.support.NonPublicAPI.Reflection;

/**
 *
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class MapTTL<K, V> implements ExpirableAsync<K> {
	private static final Logger log = LoggerFactory.getLogger(MapTTL.class);

	private final RedisStrictCommand<Long> ZSCORE_LONG = new RedisStrictCommand<Long>("ZSCORE",
			new LongReplayConvertor()); // RedisCommands.ZSCORE

	private final Vertx vertx;
	private final RedissonClient redisson;
	private final String name;
	private RMapCache<K, V> map;

	public MapTTL(Vertx vertx, RedissonClient redisson, String name) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(name, "name");
		this.vertx = vertx;
		this.redisson = redisson;
		this.name = name;
	}

	protected void setMap(RMapCache<K, V> map) {
		this.map = map;
	}

	/**
	 * https://redis.io/commands/zadd
	 * 
	 * @return TTL in milliseconds
	 * @see org.redisson.RedissonMapCache#getTimeoutSetNameByKey
	 * @see org.redisson.RedissonObject#encodeMapKey
	 */
	@Override
	public void refreshTTLIfPresent(K k, long timeToLive, TimeUnit timeUnit, Handler<AsyncResult<Long>> resultHandler) {
		Context context = vertx.getOrCreateContext();
		final String key = Reflection.invokeMethod(map, RedissonMapCache.class, "getTimeoutSetNameByKey",
				new Class<?>[] { Object.class }, new Object[] { name });

		final ByteBuf encodeMapKey = Reflection.invokeMethod(map, RedissonObject.class, "encodeMapKey",
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
				.whenCompleteAsync((value, err) -> {
					if (err == null) { // java.lang.Boolean / java.lang.Long
						log.debug("value.class: {}, value: {}", value == null ? "<null>" : value.getClass().getName(), value);
						Long numOfAdded = (Long) value; // num.longValue() == 0
						// Boolean added = (Boolean) value;
						context.runOnContext(vd -> resultHandler.handle(Future.succeededFuture(numOfAdded))); // (Boolean) value)
					} else {
						context.runOnContext(vd -> resultHandler.handle(Future.failedFuture(err)));
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
		Context context = vertx.getOrCreateContext();
		action(k, ar -> {
			if (ar.failed()) {
				context.runOnContext(vd -> resultHandler.handle(Future.failedFuture(ar.cause())));
			} else {
				context.runOnContext(vd -> resultHandler.handle(Future.succeededFuture(ar.result())));
			}
		});
	}

	private void action(K k, Handler<AsyncResult<Long>> resultHandler) {
		// final String key = "redisson__timeout__set:{" + name + "}"; // XXX
		final String key = Reflection.invokeMethod(map, RedissonMapCache.class, "getTimeoutSetNameByKey",
				new Class<?>[] { Object.class }, new Object[] { name });

		// final String field = "\"" + k + "\""; // XXX
		final ByteBuf encodeMapKey = Reflection.invokeMethod(map, RedissonObject.class, "encodeMapKey",
				new Class<?>[] { Object.class }, new Object[] { k });
		final String field = encodeMapKey.toString(CharsetUtil.UTF_8);
		log.debug("k: {}, key: {}, field: {}", k, key, field);
		final Redisson redissonImpl = ((Redisson) redisson);
		final CommandAsyncExecutor commandExecutor = redissonImpl.getCommandExecutor();

		commandExecutor.readAsync(key, LongCodec.INSTANCE, ZSCORE_LONG, key, field).whenCompleteAsync((value, err) -> {
			if (err == null) {
				log.debug("value.class: {}, value: {}", value == null ? "<null>" : value.getClass().getName(), value);
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
						ZoneId zone = ZoneId.systemDefault();
						LocalDateTime now = new Date(nowMillis).toInstant().atZone(zone).toLocalDateTime(); // LocalDateTime.now();
						LocalDateTime valTime = new Date(val).toInstant().atZone(zone).toLocalDateTime();
						if (now.isAfter(valTime)) {
							resultHandler.handle(Future.succeededFuture(0L));
						} else {
							long nowMilli = now.atZone(zone).toInstant().toEpochMilli();
							long valMilli = valTime.atZone(zone).toInstant().toEpochMilli();
							long ttl = valMilli - nowMilli;
							if (ttl != ttlMillis) { // debugging
								resultHandler.handle(Future
										.failedFuture(new Exception("(ttl != ttlMillis), ttl: " + ttl + ", ttlMillis: " + ttlMillis)));
							}
							log.debug("ttl: {}", ttl);
							resultHandler.handle(Future.succeededFuture(ttl <= 0 ? 0 : ttl));
						}
					}
				}
			} else {
				resultHandler.handle(Future.failedFuture(err));
			}
		});
	}
}
