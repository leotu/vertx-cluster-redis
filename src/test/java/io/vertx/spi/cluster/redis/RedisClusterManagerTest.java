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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
//import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.test.core.AsyncTestBase;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RedisClusterManagerTest extends AsyncTestBase {
	private static final Logger log = LoggerFactory.getLogger(RedisClusterManagerTest.class);
//	private static final Logger log;
//	static {
//		System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
//		log = LoggerFactory.getLogger(RedisClusterManagerTest.class);
//	}

	private static RedissonClient redisson;

	@BeforeClass
	static public void beforeClass() {
		log.debug("...");
		Config config = new Config();
		config.useSingleServer() //
				.setAddress("redis://127.0.0.1:6379") //
				.setDatabase(1) //
				.setPassword("mypwd") //
				.setConnectionMinimumIdleSize(5);
		redisson = Redisson.create(config);
	}

	@AfterClass
	static public void afterClass() {
	    redisson.shutdown(10, 15, TimeUnit.SECONDS);
	}

	@Test
	public void test1EventBusP2P() throws Exception {
		log.debug("BEGIN...");

		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		RedisClusterManager mgr1 = new RedisClusterManager(redisson, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson, clusterHost2 + "_" + clusterPort2);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());

			res.result().eventBus().consumer("news", message -> {
				log.debug("1) message.body: {}", message.body());
				assertNotNull(message);
				assertTrue(message.body().equals("hello"));
				testComplete(); // XXX
			});

			vertx1.set(res.result());
		});
		
		assertWaitUntil(() -> vertx1.get() != null);

		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			vertx2.set(res.result());
			res.result().eventBus().send("news", "hello"); // send
		});
		
		assertWaitUntil(() -> vertx2.get() != null);
		
		log.debug("await...");
		await(); // XXX

		log.debug("close...");
		Future<Void> f1 = Future.future();
		Future<Void> f2 = Future.future();
		vertx1.get().close(f1);
		vertx2.get().close(f2);

		//
		log.debug("finish...");
		CountDownLatch finish = new CountDownLatch(1);
		CompositeFuture.all(f1, f2).setHandler(ar -> {
			finish.countDown();
		});

		finish.await(1, TimeUnit.MINUTES);
		log.debug("END.");
		awaitAsyncInit();
	}

	private void awaitAsyncInit() {
		try {
			Thread.sleep(1000 * 3);
		} catch (InterruptedException e) {
			log.warn(e.toString());
		}
		log.debug("After awaitAsyncInit");
	}
	
	//@Test
	public void testEventBusPubSub() throws Exception {
		log.debug("BEGIN...");
		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		String clusterHost3 = IpUtil.getLocalRealIP();
		int clusterPort3 = 8083;

		String clusterHost4 = IpUtil.getLocalRealIP();
		int clusterPort4 = 8084;

		RedisClusterManager mgr1 = new RedisClusterManager(redisson, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson, clusterHost2 + "_" + clusterPort2);
		RedisClusterManager mgr3 = new RedisClusterManager(redisson, clusterHost3 + "_" + clusterPort3);
		RedisClusterManager mgr4 = new RedisClusterManager(redisson, clusterHost4 + "_" + clusterPort4);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);

		VertxOptions options3 = new VertxOptions().setClusterManager(mgr3);
		options3.getEventBusOptions().setClustered(true).setHost(clusterHost3).setPort(clusterPort3);

		VertxOptions options4 = new VertxOptions().setClusterManager(mgr4);
		options4.getEventBusOptions().setClustered(true).setHost(clusterHost4).setPort(clusterPort4);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();
		AtomicReference<Vertx> vertx3 = new AtomicReference<>();
		AtomicReference<Vertx> vertx4 = new AtomicReference<>();

		AtomicInteger counter = new AtomicInteger();

		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());
			res.result().eventBus().consumer("news", message -> {
				log.debug("1) message.body: {}", message.body());
				assertNotNull(message);
				assertTrue(message.body().equals("hello"));
				counter.incrementAndGet();  // XXX
			});
			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);

		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			res.result().eventBus().consumer("news", message -> {
				log.debug("2) message.body: {}", message.body());
				assertNotNull(message);
				assertTrue(message.body().equals("hello"));
				counter.incrementAndGet();  // XXX
			});
			vertx2.set(res.result());
		});

		assertWaitUntil(() -> vertx2.get() != null);

		Vertx.clusteredVertx(options3, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr3.getNodeID());
			res.result().eventBus().consumer("news", message -> {
				log.debug("3) message.body: {}", message.body());
				assertNotNull(message);
				assertTrue(message.body().equals("hello"));
				counter.incrementAndGet();  // XXX
			});
			vertx3.set(res.result());
		});

		assertWaitUntil(() -> vertx3.get() != null);

		// Producer
		Vertx.clusteredVertx(options4, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr4.getNodeID());
			vertx4.set(res.result());
			res.result().eventBus().publish("news", "hello"); // publish
		});
		
		assertWaitUntil(() -> vertx4.get() != null);

		assertWaitUntil(() -> counter.get() == 3);

		log.debug("close...");
		Future<Void> f1 = Future.future();
		Future<Void> f2 = Future.future();
		Future<Void> f3 = Future.future();
		Future<Void> f4 = Future.future();

		vertx1.get().close(f1);
		vertx2.get().close(f2);
		vertx3.get().close(f3);
		vertx4.get().close(f4);

		log.debug("finish...");
		CountDownLatch finish = new CountDownLatch(1);
		CompositeFuture.all(f1, f2, f3, f4).setHandler(ar -> {
			log.debug("all closed: {}", ar.succeeded());
			finish.countDown();
		});

		finish.await(1, TimeUnit.MINUTES);
		log.debug("END.");
		awaitAsyncInit();
	}

	//@Test
	public void testEventBusWithReply() throws Exception {
		log.debug("BEGIN...");

		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		RedisClusterManager mgr1 = new RedisClusterManager(redisson, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson, clusterHost2 + "_" + clusterPort2);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());
			res.result().eventBus().consumer("news", message -> {
				log.debug("1) message.body: {}", message.body());
				assertNotNull(message);
				assertTrue(message.body().equals("ping"));
				message.reply("pong");
			});
			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);

		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			vertx2.set(res.result());
			res.result().eventBus().send("news", "ping", ar -> {
				if (ar.succeeded()) {
					log.debug("2) result.body: {}", ar.result().body());
					assertTrue(ar.result().body().equals("pong"));
					testComplete(); // XXX
				}
			});
		});

		log.debug("await...");
		await(); // XXX

		log.debug("close...");
		Future<Void> f1 = Future.future();
		Future<Void> f2 = Future.future();
		vertx1.get().close(f1);
		vertx2.get().close(f2);

		log.debug("finish...");
		CountDownLatch finish = new CountDownLatch(1);
		CompositeFuture.all(f1, f2).setHandler(ar -> {
			log.debug("all closed: {}", ar.succeeded());
			finish.countDown();
		});

		finish.await(1, TimeUnit.MINUTES);
		log.debug("END.");
		awaitAsyncInit();
	}

	@Test
	public void testSharedData() throws Exception {
		log.debug("BEGIN...");

		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		RedisClusterManager mgr1 = new RedisClusterManager(redisson, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson, clusterHost2 + "_" + clusterPort2);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());
			res.result().sharedData().getClusterWideMap("mymap1", ar -> {
				ar.result().put("news", "hello", v -> {
					log.debug("1) put succeeded: {}", v.succeeded());
				});
			});
			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);

		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			vertx2.set(res.result());
			res.result().sharedData().getClusterWideMap("mymap1", ar -> {
				ar.result().get("news", r -> {
					log.debug("2) get value: {}", r.result());
					assertEquals("hello", r.result());
					testComplete(); // XXX
				});
			});
		});

		log.debug("await...");
		await(); // XXX

		log.debug("close...");
		Future<Void> f1 = Future.future();
		Future<Void> f2 = Future.future();
		vertx1.get().close(f1);
		vertx2.get().close(f2);

		log.debug("finish...");
		CountDownLatch finish = new CountDownLatch(1);
		CompositeFuture.all(f1, f2).setHandler(ar -> {
			log.debug("all closed: {}", ar.succeeded());
			finish.countDown();
		});

		finish.await(1, TimeUnit.MINUTES);
		log.debug("END.");
		awaitAsyncInit();
	}
}
