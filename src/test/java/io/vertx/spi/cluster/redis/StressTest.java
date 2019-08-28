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

import java.util.UUID;
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

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.test.core.AsyncTestBase;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SuppressWarnings("deprecation")
public class StressTest extends AsyncTestBase {
	static {
		System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
		LoggerFactory.initialise();
	}
	private static final Logger log = LoggerFactory.getLogger(StressTest.class);

	static protected RedissonClient createRedissonClient() {
		log.debug("...");
		Config config = new Config();
		config.useSingleServer() //
				.setAddress("redis://127.0.0.1:6379") //
				.setDatabase(1) //
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
		options1.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);
		options2.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		int maxCount = 57_000; // raise error: 60_000; ok: 57_000
		AtomicInteger counter = new AtomicInteger(maxCount);
		String address = UUID.randomUUID().toString();

		// Receiver
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());

			res.result().eventBus().<String>consumer(address, message -> {
				assertNotNull(message);
				if (counter.get() % 500 == 0) {
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
		new Thread(() -> {
			for (int i = 0; i < maxCount; i++) {
				if (i % 200 == 0) {
					log.debug("{}, send message", i);
					sleep("send:" + i, 10);
				}
				vertx.eventBus().send(address, "hello:" + i); // send
			}
		}).start();

		log.debug("await...");
		await(5, TimeUnit.MINUTES); // XXX

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
		options1.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);
		options2.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		VertxOptions options3 = new VertxOptions().setClusterManager(mgr3);
		options3.getEventBusOptions().setClustered(true).setHost(clusterHost3).setPort(clusterPort3);
		options3.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		VertxOptions options4 = new VertxOptions().setClusterManager(mgr4);
		options4.getEventBusOptions().setClustered(true).setHost(clusterHost4).setPort(clusterPort4);
		options4.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();
		AtomicReference<Vertx> vertx3 = new AtomicReference<>();
		AtomicReference<Vertx> vertx4 = new AtomicReference<>();

		int maxCount = 51_000; // raise error: 52_000; ok: 51_000
		AtomicInteger counter = new AtomicInteger(maxCount * 3); // 3 receivers
		String address = UUID.randomUUID().toString();

		// Receiver
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());

			AtomicInteger localCounter = new AtomicInteger(0);
			res.result().eventBus().<String>consumer(address, message -> {
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
			res.result().eventBus().<String>consumer(address, message -> {
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
			res.result().eventBus().<String>consumer(address, message -> {
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
		new Thread(() -> {
			for (int i = 0; i < maxCount; i++) {
				if (i % 200 == 0) {
					log.debug("{}, publish message", i);
					sleep("send:" + i, 10);
				}
				vertx.eventBus().publish(address, "hello:" + i); // publish
			}
		}).start();

		log.debug("await...");
		await(5, TimeUnit.MINUTES); // XXX

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
		options1.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);
		options2.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		AtomicReference<Vertx> vertx1 = new AtomicReference<>();
		AtomicReference<Vertx> vertx2 = new AtomicReference<>();

		int maxCount = 55_000; // raise error: 60_000, 55_000, 54_480; ok: 54_450
		AtomicInteger counter = new AtomicInteger(maxCount);

		String address = UUID.randomUUID().toString();

		// Receiver
		Vertx.clusteredVertx(options1, res -> {
			assertTrue(res.succeeded());
			assertNotNull(mgr1.getNodeID());
			res.result().eventBus().<String>consumer(address, message -> {
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
		new Thread(() -> {
			for (int i = 0; i < maxCount; i++) {
				if (i % 200 == 0) {
					log.debug("{}, send message", i);
					sleep("send:" + i, 10);
				}
				vertx.eventBus().<String>send(address, "ping:" + i, ar -> {
					if (replyCountdown.get() % 1000 == 0) {
						log.debug("{}, reply message", replyCountdown);
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
		}).start();

		log.debug("await...");
		await(5, TimeUnit.MINUTES); // XXX

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
		options1.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

		VertxOptions options2 = new VertxOptions().setClusterManager(mgr2);
		options2.getEventBusOptions().setClustered(true).setHost(clusterHost2).setPort(clusterPort2);
		options2.setInternalBlockingPoolSize(VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE * 2)
				.setWorkerPoolSize(VertxOptions.DEFAULT_WORKER_POOL_SIZE * 2);

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

		String prefix = UUID.randomUUID().toString();

		// Put
		log.debug("Put...");
		putVertx.sharedData().<String, String>getClusterWideMap(mapName, ar -> {
			assertTrue(ar.succeeded());
			AsyncMap<String, String> map = ar.result();
			map.clear(arv -> {
				log.debug("Put...clear: {}", arv.succeeded());
				new Thread(() -> {
					log.debug("Put execute...");
					for (int i = 0; i < maxCount; i++) {
						if (i % 1000 == 0) {
							log.debug("{}, put", i);
							sleep("sleep put:" + i, 10);
						}
						final String key = prefix + "-" + i;
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
				}).start();
			});
			log.debug("PUT: {}", map);
		});

		// Get
		log.debug("Get...");
		getVertx.sharedData().<String, String>getClusterWideMap(mapName, ar -> {
			assertTrue(ar.succeeded());
			AsyncMap<String, String> map = ar.result();
			new Thread(() -> {
				log.debug("Get execute...");
				for (int i = 0; i < maxCount; i++) {
					if (i % 1000 == 0) {
						log.debug("{}, get", i);
						sleep("sleep get:" + i, 10);
					}
					final String key = prefix + "-" + i;
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
			}).start();
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

	private void sleep(String msg, long millis) {
		// log.debug("Sleep: {}", msg);
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			log.warn(e.toString());
		}
	}
}
