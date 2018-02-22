package io.vertx.spi.cluster.redis.impl;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Objects;

import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.impl.HAManager;
import io.vertx.core.impl.VertxInternal;

/**
 * Non Public API Utility
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class NonPublicSupportAPI {
	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(NonPublicSupportAPI.class);

	public static final String HA_CLUSTER_MAP_NAME;

	public static final String EB_SERVER_ID_HA_KEY;
	public static final String EB_SUBS_MAP_NAME;
	static {
		HA_CLUSTER_MAP_NAME = getStaticFinalField(HAManager.class, "CLUSTER_MAP_NAME");
		// log.debug("HA_CLUSTER_MAP_NAME={}", HA_CLUSTER_MAP_NAME);

		EB_SERVER_ID_HA_KEY = getStaticFinalField(ClusteredEventBus.class, "SERVER_ID_HA_KEY");
		// log.debug("EB_SERVER_ID_HA_KEY={}", EB_SERVER_ID_HA_KEY);

		EB_SUBS_MAP_NAME = getStaticFinalField(ClusteredEventBus.class, "SUBS_MAP_NAME");
		// log.debug("EB_SUBS_MAP_NAME={}", EB_SUBS_MAP_NAME);
	}

	public static boolean isInactive(Vertx vertx, RedissonClient redisson) {
		final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
		if (eventBus != null) {
			final HAManager haManager = getHAManager(vertx);
			final VertxInternal vertxInternal = (VertxInternal) vertx;
			if (haManager != null) {
				final boolean haManagerStopped = getField(haManager, HAManager.class, "stopped");
				return vertxInternal.isKilled() || redisson.isShutdown() || redisson.isShuttingDown()
						|| haManager.isKilled() || haManagerStopped;
			} else {
				return vertxInternal.isKilled() || redisson.isShutdown() || redisson.isShuttingDown();
			}
		} else {
			return redisson.isShutdown() || redisson.isShuttingDown();
		}
	}

	// /**
	// * @see HAManager#addDataToAHAInfo
	// */
	// public static boolean addDataToAHAInfo(Vertx vertx, String nodeID) {
	// final HAManager haManager = getHAManager(vertx);
	// if (haManager == null) {
	// return false;
	// }
	// final JsonObject haInfo = getHaInfo(vertx);
	// final Map<String, String> clusterMap = getFinalField(haManager, HAManager.class, "clusterMap");
	// clusterMap.put(nodeID, haInfo.encode());
	// return true;
	// }

	// /**
	// * @see HAManager#addHaInfoIfLost
	// */
	// public static boolean addHaInfoIfLost(Vertx vertx, String nodeID) {
	// final HAManager haManager = getHAManager(vertx);
	// if (haManager == null) {
	// return false;
	// }
	// // callMethod(haManager, HAManager.class, "addHaInfoIfLost", new Class<?>[0], new Object[0]);
	// final JsonObject haInfo = getHaInfo(vertx);
	// final Map<String, String> clusterMap = getFinalField(haManager, HAManager.class, "clusterMap");
	// clusterMap.put(nodeID, haInfo.encode());
	// return true;
	// }

	private static HAManager getHAManager(Vertx vertx) {
		final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
		if (eventBus == null) {
			return null;
		}
		return getFinalField(eventBus, ClusteredEventBus.class, "haManager");
	}

	// private Set<String> getOwnSubs(Vertx vertx) {
	// final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
	// if (eventBus == null) {
	// return null;
	// }
	// return getField(eventBus, ClusteredEventBus.class, "ownSubs");
	// }

	// /**
	// * @param vertx
	// */
	// public Map<String, ClusterNodeInfo> getOwnSubsWithClusterNodeInfo(Vertx vertx) {
	// final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
	// if (eventBus == null) {
	// return null;
	// }
	// ClusterNodeInfo nodeInfo = getClusterNodeInfo(vertx);
	// Set<String> ownSubs = getOwnSubs(vertx);
	// ConcurrentMap<String, ClusterNodeInfo> rejoinSyncAddresses = new ConcurrentHashMap<>();
	// ownSubs.forEach(address -> rejoinSyncAddresses.put(address, nodeInfo));
	// return rejoinSyncAddresses;
	// }

	// private ClusterNodeInfo getClusterNodeInfo(Vertx vertx) {
	// final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
	// if (eventBus == null) {
	// return null;
	// }
	// return getField(eventBus, ClusteredEventBus.class, "nodeInfo");
	// }
	//
	// public static JsonObject getHaInfo(Vertx vertx) {
	// final HAManager haManager = getHAManager(vertx);
	// if (haManager == null) {
	// log.debug("(haManager == null)");
	// return null;
	// }
	// final JsonObject haInfo = getFinalField(haManager, HAManager.class, "haInfo");
	// // log.debug("haInfo={}", haInfo);
	// return haInfo;
	// }

	private static <T> T getStaticFinalField(Class<?> clsObj, String staticFieldName) {
		return getFinalField(null, clsObj, staticFieldName);
	}

	/**
	 * 
	 * @param reflectObj
	 *            null for static field
	 */
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
				Object fieldObj = field.get(reflectObj);
				@SuppressWarnings("unchecked")
				T t = (T) fieldObj;
				return t;
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
	 * @param reflectObj
	 *            null for static field
	 */
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
				Object fieldObj = field.get(reflectObj);
				@SuppressWarnings("unchecked")
				T t = (T) fieldObj;
				return t;
			} finally {
				field.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			Throwable t = e.getCause() != null && e instanceof InvocationTargetException ? e.getCause() : e;
			throw new RuntimeException(fieldName, t);
		}
	}

	// /**
	// *
	// * @param reflectObj
	// * null for static method
	// */
	// static public <T> T callMethod(Object reflectObj, Class<?> clsObj, String methodName, Class<?>[] argsTypes,
	// Object[] argsValues) {
	// Objects.requireNonNull(clsObj, "clsObj");
	// Objects.requireNonNull(methodName, "methodName");
	// try {
	// Method method = clsObj.getDeclaredMethod(methodName, argsTypes);
	// boolean keepStatus = method.isAccessible();
	// if (!keepStatus) {
	// method.setAccessible(true);
	// }
	// try {
	// @SuppressWarnings("unchecked")
	// T obj = (T) method.invoke(reflectObj, argsValues);
	// return obj;
	// } finally {
	// method.setAccessible(keepStatus);
	// }
	// } catch (Exception e) {
	// Throwable t = e.getCause() != null && e instanceof InvocationTargetException ? e.getCause() : e;
	// throw new RuntimeException(methodName, t);
	// }
	// }
}
