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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.shareddata.AsyncMap;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
//import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.test.core.AsyncTestBase;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StressTest extends AsyncTestBase {
	private static final Logger log = LoggerFactory.getLogger(StressTest.class);
//	private static final Logger log;
//	static {
//		System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
//		log = LoggerFactory.getLogger(RedisClusterManagerTest.class);
//	}

	static protected RedissonClient createRedissonClient() {
		log.debug("...");
		Config config = new Config();
		config.useSingleServer() //
				.setAddress("redis://127.0.0.1:6379") //
				.setDatabase(1) //
				.setPassword("mypwd") //
				.setTcpNoDelay(true) //
				.setDnsMonitoring(false) //
				.setKeepAlive(true) //
				.setConnectionPoolSize(128) //
				.setConnectionMinimumIdleSize(5);
		return Redisson.create(config);
	}

	static protected void closeRedissonClient(RedissonClient redisson) {
		redisson.shutdown(3, 15, TimeUnit.SECONDS);
		log.debug("after shutdown");
	}

	@Test
	public void test1EventBusP2P() throws Exception {
		log.debug("BEGIN...");

		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		RedissonClient redisson1 = createRedissonClient();
		RedissonClient redisson2 = createRedissonClient();

		RedisClusterManager mgr1 = new RedisClusterManager(redisson1, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson2, clusterHost2 + "_" + clusterPort2);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		int maxCount = 57_000; // raise error: 60_000, 58_000; ok: 57_000
		AtomicInteger counter = new AtomicInteger(maxCount);

		// Receiver
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());

			res.result().eventBus().<String>consumer("news", message -> {
				assertNotNull(message);
				if (counter.get() % 1000 == 0) {
					log.debug("{}, received message", counter);
				}
				assertTrue(message.body().startsWith("hello"));

				if (counter.decrementAndGet() == 0) {
					log.info("Test received completed");
					testComplete(); // XXX
				}
			});

			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);

		// Producer
		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			vertx2.set(res.result());
		});

		assertWaitUntil(() -> vertx2.get() != null);

		sleep("Ready for clusters initialize");

		Vertx vertx = vertx2.get();
		log.debug("send...");
		vertx.executeBlocking(future -> {
			for (int i = 0; i < maxCount; i++) {
				if (i % 1000 == 0) {
					log.debug("{}, send message", i);
				}
				vertx.eventBus().send("news", "hello:" + i); // send
			}
		}, ar -> {
			if (ar.failed()) {
				log.warn(ar.cause().toString());
				fail(ar.cause());
			}
		});

//		Failed to send message 
//		org.redisson.client.RedisTimeoutException: Unable to send command: (SMEMBERS) with params: [{__vertx.subs}:3x2DX/aAu2PthjXwF19xhg] after 3 retry attempts
//			at org.redisson.command.CommandAsyncService$8.run(CommandAsyncService.java:562)
//			at io.netty.util.HashedWheelTimer$HashedWheelTimeout.expire(HashedWheelTimer.java:682)
//			at io.netty.util.HashedWheelTimer$HashedWheelBucket.expireTimeouts(HashedWheelTimer.java:757)
//			at io.netty.util.HashedWheelTimer$Worker.run(HashedWheelTimer.java:485)
//			at java.lang.Thread.run(Thread.java:748)

		log.debug("await...");
		await(3, TimeUnit.MINUTES); // XXX

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
		sleep("END Before return");

		closeRedissonClient(redisson1);
		closeRedissonClient(redisson2);
	}

//    @Test
	public void test2EventBusPubSub() throws Exception {
		log.debug("BEGIN...");
		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		String clusterHost3 = IpUtil.getLocalRealIP();
		int clusterPort3 = 8083;

		String clusterHost4 = IpUtil.getLocalRealIP();
		int clusterPort4 = 8084;

		RedissonClient redisson1 = createRedissonClient();
		RedissonClient redisson2 = createRedissonClient();
		RedissonClient redisson3 = createRedissonClient();
		RedissonClient redisson4 = createRedissonClient();

		RedisClusterManager mgr1 = new RedisClusterManager(redisson1, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson2, clusterHost2 + "_" + clusterPort2);
		RedisClusterManager mgr3 = new RedisClusterManager(redisson3, clusterHost3 + "_" + clusterPort3);
		RedisClusterManager mgr4 = new RedisClusterManager(redisson4, clusterHost4 + "_" + clusterPort4);

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

		int maxCount = 47_000; // raise error: 50_000, 49_000; ok: 47_000
		AtomicInteger counter = new AtomicInteger(maxCount * 3); // 3 receivers

		// Receiver
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());

			AtomicInteger localCounter = new AtomicInteger(0);
			res.result().eventBus().<String>consumer("news", message -> {
				assertNotNull(message);
				if (localCounter.getAndIncrement() % 200 == 0) {
					log.debug("{}, 1) received message", localCounter);
				}
				assertTrue(message.body().startsWith("hello"));

				if (counter.decrementAndGet() == 0) {
					log.info("By (1), Test received completed");
					testComplete(); // XXX
				}
			});
			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);

		// Receiver
		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			AtomicInteger localCounter = new AtomicInteger(0);
			res.result().eventBus().<String>consumer("news", message -> {
				assertNotNull(message);
				if (localCounter.getAndIncrement() % 200 == 0) {
					log.debug("{}, 2) received message", localCounter);
				}
				assertTrue(message.body().startsWith("hello"));

				if (counter.decrementAndGet() == 0) {
					log.info("By (2), Test received completed");
					testComplete(); // XXX
				}
			});
			vertx2.set(res.result());
		});

		assertWaitUntil(() -> vertx2.get() != null);

		// Receiver
		Vertx.clusteredVertx(options3, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr3.getNodeID());
			AtomicInteger localCounter = new AtomicInteger(0);
			res.result().eventBus().<String>consumer("news", message -> {
				assertNotNull(message);
				if (localCounter.getAndIncrement() % 200 == 0) {
					log.debug("{}, 3) received message", localCounter);
				}
				assertTrue(message.body().startsWith("hello"));

				if (counter.decrementAndGet() == 0) {
					log.info("By (3), Test received completed");
					testComplete(); // XXX
				}
			});
			vertx3.set(res.result());
		});

		assertWaitUntil(() -> vertx3.get() != null);

		// Producer
		Vertx.clusteredVertx(options4, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr4.getNodeID());
			vertx4.set(res.result());
		});

		assertWaitUntil(() -> vertx4.get() != null);

		sleep("Ready for clusters initialize");

		Vertx vertx = vertx4.get();
		log.debug("publish...");
		vertx.executeBlocking(future -> {
			for (int i = 0; i < maxCount; i++) {
				if (i % 200 == 0) {
					log.debug("{}, publish message", i);
				}
				vertx.eventBus().publish("news", "hello:" + i); // publish
			}
		}, ar -> {
			if (ar.failed()) {
				log.warn(ar.cause().toString());
				fail(ar.cause());
			}
		});

//		Failed to send message 
//		org.redisson.client.RedisTimeoutException: Unable to send command: (SMEMBERS) with params: [{__vertx.subs}:3x2DX/aAu2PthjXwF19xhg] after 3 retry attempts
//			at org.redisson.command.CommandAsyncService$8.run(CommandAsyncService.java:534)
//			at io.netty.util.HashedWheelTimer$HashedWheelTimeout.expire(HashedWheelTimer.java:682)
//			at io.netty.util.HashedWheelTimer$HashedWheelBucket.expireTimeouts(HashedWheelTimer.java:757)
//			at io.netty.util.HashedWheelTimer$Worker.run(HashedWheelTimer.java:485)
//			at java.lang.Thread.run(Thread.java:748)

		log.debug("await...");
		await(3, TimeUnit.MINUTES); // XXX

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
		sleep("END Before return");

		closeRedissonClient(redisson1);
		closeRedissonClient(redisson2);
		closeRedissonClient(redisson3);
		closeRedissonClient(redisson4);
	}

//	@Test
	public void test3EventBusWithReply() throws Exception {
		log.debug("BEGIN...");

		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		RedissonClient redisson1 = createRedissonClient();
		RedissonClient redisson2 = createRedissonClient();

		RedisClusterManager mgr1 = new RedisClusterManager(redisson1, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson2, clusterHost2 + "_" + clusterPort2);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		int maxCount = 55_000; // raise error: 60_000, 55_000, 54_480; ok: 54_450
		AtomicInteger counter = new AtomicInteger(maxCount);

		// Receiver
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());
			res.result().eventBus().<String>consumer("news", message -> {
				if (counter.get() % 1000 == 0) {
					log.debug("{}, received message", counter);
				}
				assertTrue(message.body().startsWith("ping"));

				message.reply("pong");
				if (counter.decrementAndGet() == 0) {
					log.info("Test received completed");
					testComplete(); // XXX
				}
			});
			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);

		// Producer
		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			vertx2.set(res.result());
		});

		sleep("Ready for clusters initialize");

		AtomicInteger replyCountdown = new AtomicInteger(maxCount);
		Vertx vertx = vertx2.get();
		log.debug("send/reply...");
		vertx.executeBlocking(future -> {
			for (int i = 0; i < maxCount; i++) {
				if (i % 1000 == 0) {
					log.debug("{}, send message", i);
				}
				vertx.eventBus().<String>send("news", "ping:" + i, ar -> {
					if (replyCountdown.get() % 1000 == 0) {
						log.debug("{}, reply message", counter);
					}
					if (replyCountdown.decrementAndGet() == 0) {
						log.info("Reply count down completed");
					}

					if (ar.succeeded()) {
						assertTrue(ar.result().body().startsWith("pong"));
					} else {
						log.warn("reply failed: {}", ar.cause().toString());
					}
				});
			}
		}, ar -> {
			if (ar.failed()) {
				log.warn(ar.cause().toString());
				fail(ar.cause());
			}
		});

		// reply failed: (TIMEOUT,-1) Timed out after waiting 30000(ms) for a reply.
		// address: __vertx.reply.d563a225-a3e8-4296-a8f4-c3b8c3bd1c48, repliedAddress:
		// news

		log.debug("await...");
		await(3, TimeUnit.MINUTES); // XXX

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
		sleep("END Before return");

		closeRedissonClient(redisson1);
		closeRedissonClient(redisson2);
	}

//	@Test
	public void test4SharedData() throws Exception {
		log.debug("BEGIN...");

		String clusterHost1 = IpUtil.getLocalRealIP();
		int clusterPort1 = 8081;

		String clusterHost2 = IpUtil.getLocalRealIP();
		int clusterPort2 = 8082;

		RedissonClient redisson1 = createRedissonClient();
		RedissonClient redisson2 = createRedissonClient();

		RedisClusterManager mgr1 = new RedisClusterManager(redisson1, clusterHost1 + "_" + clusterPort1);
		RedisClusterManager mgr2 = new RedisClusterManager(redisson2, clusterHost2 + "_" + clusterPort2);

		VertxOptions options1 = new VertxOptions().setClusterManager(mgr1);
		options1.getEventBusOptions().setClustered(true).setHost(clusterHost1).setPort(clusterPort1);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		// Put
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());
			vertx1.set(res.result());
		});

		assertWaitUntil(() -> vertx1.get() != null);
		Vertx putVertx = vertx1.get();

		// Get
		Vertx.clusteredVertx(options2, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr2.getNodeID());
			vertx2.set(res.result());
		});

		assertWaitUntil(() -> vertx2.get() != null);
		Vertx getVertx = vertx2.get();

		sleep("Ready for clusters initialize");

		int maxCount = 15_000; // raise error: 20_000; ok: 15_000
		String mapName = "mymap1";
		AtomicInteger putCounter = new AtomicInteger(maxCount);
		AtomicInteger getCounter = new AtomicInteger(maxCount);
		AtomicInteger getValCounter = new AtomicInteger();

		CountDownLatch completedLatch = new CountDownLatch(2);

		// Put
		log.debug("Put...");
		putVertx.sharedData().<String, String>getClusterWideMap(mapName, ar -> {
			assertTrue(ar.succeeded());
			AsyncMap<String, String> map = ar.result();
			map.clear(arv -> {
				log.debug("Put...clear: {}", arv.succeeded());
				putVertx.executeBlocking(future -> {
					log.debug("Put execute...");
					for (int i = 0; i < maxCount; i++) {
//						if (i % 1000 == 0) {
//							log.debug("{}, put", i);
//							sleep("sleep put:" + i, 1);
//						}
						final String key = "news-" + i;
						map.put(key, "hello-" + i, v -> {
							if (v.failed()) {
								log.warn("put key: {} failed: {}", key, v.cause().toString());
								log.error("", v.cause());
							}
							if (putCounter.get() % 500 == 0) {
								log.debug("{}, put", putCounter.get());
							}
							if (putCounter.decrementAndGet() == 0) {
								log.info("Put completed");
								completedLatch.countDown();
							}
						});
					}
				}, ar2 -> {
					if (ar2.failed()) {
						log.warn(ar2.cause().toString());
						fail(ar2.cause());
					}
				});
			});
			log.debug("PUT: {}", map);
		});

//		put key: news-19638 failed: org.redisson.client.RedisTimeoutException: Unable to send command: (EVAL) with params: [local insertable = false; local v = redis.call('hget', KEYS[1], ARGV[2]); if v == false then inserta..., 8, mymap1, redisson__timeout__set:{mymap1}, redisson__idle__set:{mymap1}, redisson_map_cache_created:{mymap1}, redisson_map_cache_updated:{mymap1}, redisson__map_cache__last_access__set:{mymap1}, redisson_map_cache_removed:{mymap1}, {mymap1}:redisson_options, ...] after 3 retry attempts

		// Get
		log.debug("Get...");
		getVertx.sharedData().<String, String>getClusterWideMap(mapName, ar -> {
			assertTrue(ar.succeeded());
			AsyncMap<String, String> map = ar.result();
			getVertx.executeBlocking(future -> {
				log.debug("Get execute...");
				for (int i = 0; i < maxCount; i++) {
//					if (i % 1000 == 0) {
//						log.debug("{}, get", i);
//						sleep("sleep get:" + i, 1000);
//					}
					final String key = "news-" + i;
					map.get(key, v -> {
						if (v.failed()) {
							log.warn("get key: {} failed: {}", key, v.cause().toString());
						} else if (v.result() != null) {
							getValCounter.incrementAndGet();
						}
						if (getCounter.get() % 500 == 0) {
							log.debug("{}, get", getCounter.get());
						}
						if (getCounter.decrementAndGet() == 0) {
							log.info("Get completed");
							completedLatch.countDown();
						}
					});
				}
			}, ar2 -> {
				if (ar2.failed()) {
					log.warn(ar2.cause().toString());
					fail(ar2.cause());
				}
			});
			log.debug("GET: {}", map);
		});

		log.debug("await...");
		completedLatch.await(10, TimeUnit.MINUTES);

		log.info("maxCount: {}, getValCounter: {}", maxCount, getValCounter);

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
		sleep("END Before return");

		closeRedissonClient(redisson1);
		closeRedissonClient(redisson2);
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
