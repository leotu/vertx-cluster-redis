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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.test.core.AsyncTestBase;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SuppressWarnings("deprecation")
public class RetriableReceiverTest extends AsyncTestBase {
	static {
		System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
		LoggerFactory.initialise();
	}
	private static final Logger log = LoggerFactory.getLogger(RetriableReceiverTest.class);

	static protected RedissonClient createRedissonClient() {
		log.debug("...");
		Config config = new Config();
		config.useSingleServer() //
				.setAddress("redis://127.0.0.1:6379") //
				.setDatabase(2) //
				.setPassword("mypwd") //
				.setTcpNoDelay(true) //
				.setKeepAlive(true) //
				.setConnectionPoolSize(128) //
				.setTimeout(1000 * 5) //
				.setConnectionMinimumIdleSize(5);
		return Redisson.create(config);
	}

	static protected void closeRedissonClient(RedissonClient redisson) {
		redisson.shutdown(3, 15, TimeUnit.SECONDS);
		log.debug("after shutdown");
	}

	protected int clusterPort() {
		return 18080;
	}

	@Test
	public void test1EventBusP2P() throws Exception {
		log.debug("BEGIN...");

		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = clusterPort();

		log.debug("clusterHost1: {}, clusterPort1: {}", clusterHost1, clusterPort1);

		RedissonClient redisson1 = createRedissonClient();

		RedisClusterManager mgr1 = new RedisClusterManager(redisson1, clusterHost1 + "_" + clusterPort1);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);
		options1.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();

		AtomicInteger counter = new AtomicInteger(0);
		String address = "Retriable";

		// Receiver
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());

			res.result().eventBus().<String>consumer(address, message -> {
				assertNotNull(message);
				counter.incrementAndGet();
				// if (counter.getAndIncrement() % 100 == 0) {
				log.debug("{}, received message, clusterPort1: {}", counter, clusterPort1);
				// }
				assertTrue(message.body().startsWith("hello"));
				message.reply("ok:" + clusterPort1);
			});

			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);

		sleep("Ready for clusters initialize");

		log.debug("await...");
		await(10, TimeUnit.MINUTES); // XXX

		log.debug("close...");
		Future<Void> f1 = Future.future();
		vertx1.get().close(f1);

		//
		log.debug("finish...");
		CountDownLatch finish = new CountDownLatch(1);
		f1.setHandler(ar -> {
			finish.countDown();
		});

		finish.await(1, TimeUnit.MINUTES);
		sleep("END Before return");

		closeRedissonClient(redisson1);
	}

	private void sleep(String msg) {
		log.debug("Sleep: {}", msg);
		try {
			Thread.sleep(1000 * 3);
		} catch (InterruptedException e) {
			log.warn(e.toString());
		}
	}

	@SuppressWarnings("unused")
	private void sleep(String msg, long millis) {
		// log.debug("Sleep: {}", msg);
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			log.warn(e.toString());
		}
	}
}
