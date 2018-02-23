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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import org.redisson.api.RedissonClient;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.impl.HAManager;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.shareddata.impl.AsynchronousLock;

/**
 * Non Public API Utility
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class NonPublicAPI {
	private static final Logger log = LoggerFactory.getLogger(NonPublicAPI.class);

	public static final String HA_CLUSTER_MAP_NAME;
	public static final String EB_SERVER_ID_HA_KEY;
	public static final String EB_SUBS_MAP_NAME;

	static {
		HA_CLUSTER_MAP_NAME = Reflection.getStaticFinalField(HAManager.class, "CLUSTER_MAP_NAME");
		EB_SERVER_ID_HA_KEY = Reflection.getStaticFinalField(ClusteredEventBus.class, "SERVER_ID_HA_KEY");
		EB_SUBS_MAP_NAME = Reflection.getStaticFinalField(ClusteredEventBus.class, "SUBS_MAP_NAME");
	}

	/**
	 * FIX: Non Vert.x thread
	 */
	public static void runOnContext(Vertx vertx, Handler<Void> action) {
		if (Vertx.currentContext() != null) { // FIXME
			action.handle(null);
		} else {
			vertx.getOrCreateContext().runOnContext(action);
		}
	}

	public static boolean isInactive(Vertx vertx, RedissonClient redisson) {
		final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
		if (eventBus != null) {
			final HAManager haManager = ClusteredEventBusAPI.getHAManager(vertx);
			final VertxInternal vertxInternal = (VertxInternal) vertx;
			if (haManager != null) {
				final boolean haManagerStopped = Reflection.getField(haManager, HAManager.class, "stopped");
				return vertxInternal.isKilled() || redisson.isShutdown() || redisson.isShuttingDown() || haManager.isKilled()
						|| haManagerStopped;
			} else {
				return vertxInternal.isKilled() || redisson.isShutdown() || redisson.isShuttingDown();
			}
		} else {
			return redisson.isShutdown() || redisson.isShuttingDown();
		}
	}

	/**
	 * 
	 * @see HAManager#addDataToAHAInfo
	 * @see HAManager#addHaInfoIfLost
	 */
	public static boolean addHaInfoIfLost(Vertx vertx, String nodeId) {
		final JsonObject haInfo = ClusteredEventBusAPI.getHaInfo(vertx);
		if (haInfo == null) {
			log.debug("(haInfo == null)");
			return false;
		}
		final Map<String, String> clusterMap = HAManagerAPI.getClusterMap(vertx);
		if (clusterMap == null) {
			log.debug("(clusterMap == null)");
			return false;
		}
		clusterMap.put(nodeId, haInfo.encode());
		return true;
	}

	protected static class HAManagerAPI {
		public static Map<String, String> getClusterMap(Vertx vertx) {
			final HAManager haManager = ClusteredEventBusAPI.getHAManager(vertx);
			return haManager == null ? null : Reflection.getFinalField(haManager, HAManager.class, "clusterMap");
		}
	}

	protected static class ClusteredEventBusAPI {

		public static HAManager getHAManager(Vertx vertx) {
			final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
			if (eventBus == null) {
				log.debug("(eventBus == null)");
				return null;
			}
			HAManager haManager = Reflection.getFinalField(eventBus, ClusteredEventBus.class, "haManager");
			if (haManager == null) {
				log.debug("(haManager == null)");
			}
			return haManager;
		}

		/**
		 * Local ConcurrentHashSet
		 */
		public static Set<String> getOwnSubs(Vertx vertx) {
			final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
			return eventBus == null ? null : Reflection.getField(eventBus, ClusteredEventBus.class, "ownSubs");
		}

		public static ClusterNodeInfo getNodeInfo(Vertx vertx) {
			final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
			return eventBus == null ? null : Reflection.getField(eventBus, ClusteredEventBus.class, "nodeInfo");
		}

		public static JsonObject getHaInfo(Vertx vertx) {
			final HAManager haManager = getHAManager(vertx);
			return haManager == null ? null : Reflection.getFinalField(haManager, HAManager.class, "haInfo");
		}
	}

	protected static class Reflection {

		private static <T> T getStaticFinalField(Class<?> clsObj, String staticFieldName) {
			return getFinalField(null, clsObj, staticFieldName);
		}

		/**
		 * 
		 * @param reflectObj null for static field
		 */
		@SuppressWarnings("unchecked")
		private static <T> T getFinalField(Object reflectObj, Class<?> clsObj, String fieldName) {
			Objects.requireNonNull(clsObj, "clsObj");
			Objects.requireNonNull(fieldName, "fieldName");
			try {
				Field field = clsObj.getDeclaredField(fieldName);
				boolean keepStatus = field.isAccessible();
				if (!keepStatus) {
					field.setAccessible(true);
				}
				try {
					Field modifiersField = Field.class.getDeclaredField("modifiers");
					modifiersField.setAccessible(true);
					modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
					//
					return (T) field.get(reflectObj);
				} finally {
					field.setAccessible(keepStatus);
				}
			} catch (Exception e) {
				Throwable t = e.getCause() != null && e instanceof InvocationTargetException ? e.getCause() : e;
				throw new RuntimeException(fieldName, t);
			}
		}

		/**
		 * 
		 * @param reflectObj null for static field
		 */
		@SuppressWarnings("unchecked")
		private static <T> T getField(Object reflectObj, Class<?> clsObj, String fieldName) {
			Objects.requireNonNull(clsObj, "clsObj");
			Objects.requireNonNull(fieldName, "fieldName");
			try {
				Field field = clsObj.getDeclaredField(fieldName);
				boolean keepStatus = field.isAccessible();
				if (!keepStatus) {
					field.setAccessible(true);
				}
				try {
					return (T) field.get(reflectObj);
				} finally {
					field.setAccessible(keepStatus);
				}
			} catch (Exception e) {
				Throwable t = e.getCause() != null && e instanceof InvocationTargetException ? e.getCause() : e;
				throw new RuntimeException(fieldName, t);
			}
		}

		/**
		 *
		 * @param reflectObj null for static method
		 */
		@SuppressWarnings({ "unchecked", "unused" })
		static private <T> T callMethod(Object reflectObj, Class<?> clsObj, String methodName, Class<?>[] argsTypes,
				Object[] argsValues) {
			Objects.requireNonNull(clsObj, "clsObj");
			Objects.requireNonNull(methodName, "methodName");
			try {
				Method method = clsObj.getDeclaredMethod(methodName, argsTypes);
				boolean keepStatus = method.isAccessible();
				if (!keepStatus) {
					method.setAccessible(true);
				}
				try {
					return (T) method.invoke(reflectObj, argsValues);
				} finally {
					method.setAccessible(keepStatus);
				}
			} catch (Exception e) {
				Throwable t = e.getCause() != null && e instanceof InvocationTargetException ? e.getCause() : e;
				throw new RuntimeException(methodName, t);
			}
		}
	}

	/**
	 * Local Lock
	 * 
	 * @see io.vertx.core.shareddata.impl.SharedDataImpl
	 */
	public static class AsyncLocalLock {
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
				} catch (Throwable ex) {
					log.info("key: {}, ignore error: {}", key, ex.toString());
				} finally {
					releaseLock(lock);
				}
			}, e -> {
				log.info("key: {} ignore lock failed: {}", key, e.toString());
				try {
					executor.run();
				} catch (Throwable ex) {
					log.info("key: {}, ignore error: {}", key, ex.toString());
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
		static private void acquireLockWithTimeout(Vertx vertx, String key, int timeoutInSeconds,
				Handler<io.vertx.core.shareddata.Lock> resultHandler, Handler<Throwable> errorHandler) {
			getLockWithTimeout(vertx, key, timeoutInSeconds * 1000, ar -> {
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
		static private void releaseLock(io.vertx.core.shareddata.Lock lock) {
			lock.release();
		}

		/**
		 * @see io.vertx.core.shareddata.impl.SharedDataImpl#getLocalLock
		 */
		static private void getLockWithTimeout(Vertx vertx, String key, int timeoutInSeconds,
				Handler<AsyncResult<Lock>> resultHandler) {
			AsynchronousLock lock = localLocks.computeIfAbsent(key, n -> new AsynchronousLock(vertx));
			lock.acquire(timeoutInSeconds * 1000, resultHandler);
		}
	}
}
