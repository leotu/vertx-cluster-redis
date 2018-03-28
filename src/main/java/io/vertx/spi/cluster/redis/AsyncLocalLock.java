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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.shareddata.impl.AsynchronousLock;

/**
 * Asynchronous Local Lock Utility
 * 
 * @see io.vertx.core.shareddata.impl.SharedDataImpl
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class AsyncLocalLock {
	private static final Logger log = LoggerFactory.getLogger(AsyncLocalLock.class);

	static private final ConcurrentMap<String, AsynchronousLock> localLocks = new ConcurrentHashMap<>();

	/**
	 * ignore any error
	 */
	static public void executeBlocking(Vertx vertx, String key, int timeoutInSeconds, Runnable executor) {
		acquireLockWithTimeout(vertx, key, timeoutInSeconds, lock -> {
			vertx.executeBlocking(future -> {
				try {
					executor.run();
					future.complete();
				} catch (Throwable ex) {
					future.fail(ex);
				}
			}, ar -> {
				if (ar.failed()) {
					log.info("key: {}, error: {}", key, ar.cause().toString());
				}
				releaseLock(lock);
			});
		}, e -> {
			log.info("key: {} ignore lock failed: {}", key, e.toString());
			vertx.executeBlocking(future -> {
				try {
					executor.run();
					future.complete();
				} catch (Throwable ex) {
					future.fail(ex);
				}
			}, ar -> {
				if (ar.failed()) {
					log.info("key: {}, ignore error: {}", key, ar.cause().toString());
				}
			});
		});
	}

	static public void executeBlocking(Vertx vertx, String key, int timeoutInSeconds, Runnable executor,
			Consumer<Throwable> error) {
		acquireLockWithTimeout(vertx, key, timeoutInSeconds, lock -> {
			vertx.executeBlocking(future -> {
				try {
					executor.run();
					future.complete();
				} catch (Throwable ex) {
					future.fail(ex);
				}
			}, ar -> {
				try {
					error.accept(ar.cause());
				} finally {
					releaseLock(lock);
				}
			});
		}, e -> error.accept(e));
	}

	/**
	 * ignore any error
	 */
	static public void execute(Vertx vertx, String key, int timeoutInSeconds, Runnable executor) {
		acquireLockWithTimeout(vertx, key, timeoutInSeconds, lock -> {
			try {
				executor.run();
			} catch (Throwable ignore) {
				log.info("key: {}, ignore error: {}", key, ignore.toString());
			} finally {
				releaseLock(lock);
			}
		}, e -> {
			log.info("key: {} ignore lock failed: {}", key, e.toString());
			try {
				executor.run();
			} catch (Throwable ignore) {
				log.info("key: {}, ignore error: {}", key, ignore.toString());
			}
		});
	}

	static public void execute(Vertx vertx, String key, int timeoutInSeconds, Runnable executor,
			Consumer<Throwable> error) {
		acquireLockWithTimeout(vertx, key, timeoutInSeconds, lock -> {
			try {
				executor.run();
			} catch (Throwable ex) {
				error.accept(ex);
			} finally {
				releaseLock(lock);
			}
		}, e -> error.accept(e));
	}

	/**
	 * @see io.vertx.core.shareddata.impl.SharedDataImpl#getLockWithTimeout
	 */
	static public void acquireLockWithTimeout(Vertx vertx, String key, int timeoutInSeconds,
			Handler<io.vertx.core.shareddata.Lock> resultHandler, Handler<Throwable> errorHandler) {
		acquireLockWithTimeout(vertx, key, timeoutInSeconds, ar -> {
			if (ar.failed()) {
				errorHandler.handle(ar.cause());
			} else {
				resultHandler.handle(ar.result());
			}
		});
	}

	/**
	 * @see io.vertx.core.shareddata.impl.AsynchronousLock#release
	 */
	static public void releaseLock(io.vertx.core.shareddata.Lock lock) {
		lock.release();
	}

	/**
	 * @see io.vertx.core.shareddata.impl.SharedDataImpl#getLocalLock
	 */
	static public void acquireLockWithTimeout(Vertx vertx, String key, int timeoutInSeconds,
			Handler<AsyncResult<Lock>> resultHandler) {
		AsynchronousLock lock = localLocks.computeIfAbsent(key, n -> new AsynchronousLock(vertx));
		lock.acquire(timeoutInSeconds * 1000, resultHandler);
	}
}