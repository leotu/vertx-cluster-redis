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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.MessageImpl;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.impl.HAManager;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.ClusterManager;

/**
 * Non Public API Utility
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class NonPublicAPI {
	// private static final Logger log = LoggerFactory.getLogger(NonPublicAPI.class);

//	public static final String HA_CLUSTER_MAP_NAME; // "__vertx.haInfo"
//	public static final String EB_SERVER_ID_HA_KEY; // "server_id"
//	public static final String EB_SUBS_MAP_NAME; // "__vertx.subs"
//
//	static {
//		HA_CLUSTER_MAP_NAME = Reflection.getStaticFinalField(VertxImpl.class, "CLUSTER_MAP_NAME");
//		EB_SERVER_ID_HA_KEY = Reflection.getStaticFinalField(ClusteredEventBus.class, "SERVER_ID_HA_KEY");
//		EB_SUBS_MAP_NAME = Reflection.getStaticFinalField(ClusteredEventBus.class, "SUBS_MAP_NAME");
//	}

	/**
	 * 
	 * @see HAManager#addDataToAHAInfo
	 * @see HAManager#addHaInfoIfLost
	 */
	public static void addHaInfoIfLost(HAManager haManager, String nodeId) {
		final JsonObject haInfo = ClusteredEventBusAPI.haInfo(haManager);
		final Map<String, String> clusterMap = HAManagerAPI.clusterMap(haManager);
		clusterMap.put(nodeId, haInfo.encode());
	}

	protected static class HAManagerAPI {
		public static Map<String, String> clusterMap(HAManager haManager) {
			return Reflection.getFinalField(haManager, HAManager.class, "clusterMap");
		}
	}

	/**
	 * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
	 */
	public static class ClusteredEventBusAPI {

		public static ClusteredEventBus eventBus(Vertx vertx) {
			return (ClusteredEventBus) vertx.eventBus();
		}

		public static HAManager haManager(ClusteredEventBus eventBus) {
			return Reflection.getFinalField(eventBus, ClusteredEventBus.class, "haManager");
		}

		public static ClusterManager clusterManager(ClusteredEventBus eventBus) {
			return Reflection.getFinalField(eventBus, ClusteredEventBus.class, "clusterManager");
		}

		/**
		 * self
		 */
		public static ServerID serverID(ClusteredEventBus eventBus) {
			return Reflection.getFinalField(eventBus, ClusteredEventBus.class, "serverID");
		}

		/**
		 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder
		 */
		public static class ConnectionHolderAPI {

			/**
			 * ConnectionHolder's pending
			 */
			public static Queue<ClusteredMessage<?, ?>> pending(Object connHolder) {
				if (!connHolder.getClass().getName().equals("io.vertx.core.eventbus.impl.clustered.ConnectionHolder")) {
					throw new IllegalArgumentException(
							"Only support type: io.vertx.core.eventbus.impl.clustered.ConnectionHolder, but parameter's type is: "
									+ connHolder.getClass().getName());
				}
				return Reflection.getField(connHolder, connHolder.getClass(), "pending");
			}

			/**
			 * ConnectionHolder's serverID
			 */
			public static ServerID serverID(Object connHolder) {
				if (!connHolder.getClass().getName().equals("io.vertx.core.eventbus.impl.clustered.ConnectionHolder")) {
					throw new IllegalArgumentException(
							"Only support type: io.vertx.core.eventbus.impl.clustered.ConnectionHolder, but parameter's type is: "
									+ connHolder.getClass().getName());
				}
				return Reflection.getField(connHolder, connHolder.getClass(), "serverID");
			}
		}

		/**
		 * Local ConcurrentHashSet
		 */
		public static Set<String> ownSubs(ClusteredEventBus eventBus) {
			return Reflection.getField(eventBus, ClusteredEventBus.class, "ownSubs");
		}

		/**
		 * @see ClusteredEventBus#sendRemote
		 */
		public static void sendRemote(ClusteredEventBus eventBus, ServerID serverID, ClusteredMessage<?, ?> message) {
			Reflection.invokeMethod(eventBus, ClusteredEventBus.class, "sendRemote",
					new Class[] { ServerID.class, MessageImpl.class }, new Object[] { serverID, message });
		}

		public static ClusterNodeInfo nodeInfo(ClusteredEventBus eventBus) {
			return Reflection.getField(eventBus, ClusteredEventBus.class, "nodeInfo");
		}

		/**
		 * ? is ConnectionHolder type
		 * 
		 * @see ClusteredEventBus#connections
		 */
		public static ConcurrentMap<ServerID, ?> connections(ClusteredEventBus eventBus) {
			return Reflection.invokeMethod(eventBus, ClusteredEventBus.class, "connections");
		}

		public static void setConnections(ClusteredEventBus eventBus, ConcurrentMap<ServerID, ?> connections) {
			Reflection.setFinalField(eventBus, ClusteredEventBus.class, "connections", connections);
		}

		public static JsonObject haInfo(HAManager haManager) {
			return Reflection.getFinalField(haManager, HAManager.class, "haInfo");
		}
	}

	public static class Reflection {

		public static <T> T getStaticFinalField(Class<?> clsObj, String staticFieldName) {
			return getFinalField(null, clsObj, staticFieldName);
		}

		/**
		 * 
		 * @param reflectObj null for static field
		 */
		@SuppressWarnings("unchecked")
		public static <T> T getFinalField(Object reflectObj, Class<?> clsObj, String fieldName) {
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
		 * @param reflectObj null for static field
		 */
		static public void setFinalField(Object reflectObj, Class<?> clsObj, String fieldName, Object newValue) {
			Objects.requireNonNull(clsObj, "clsObj");
			Objects.requireNonNull(fieldName, "fieldName");
			try {
				Field field = clsObj.getDeclaredField(fieldName);
				boolean keepStatus = field.isAccessible();
				if (!keepStatus) {
					field.setAccessible(true);
				}
				Field modifiersField = Field.class.getDeclaredField("modifiers");
				modifiersField.setAccessible(true);
				modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
				try {
					field.set(reflectObj, newValue);
				} finally {
					field.setAccessible(keepStatus);
					modifiersField.setInt(field, field.getModifiers() & Modifier.FINAL);
				}
			} catch (Exception e) {
				Throwable t = e.getCause() != null && e instanceof InvocationTargetException ? e.getCause() : e;
				throw new RuntimeException(fieldName, t);
			}
		}
		
		/**
		 * If the underlying field is a static field, the reflectObj argument is ignored; it may be null.
		 *
		 * @param reflectObj
		 *            may be null.
		 */
		static public void setField(Object reflectObj, Class<?> clsObj, String fieldName, Object newValue) {
			if (clsObj == null) {
				throw new IllegalArgumentException("(clsObj == null)");
			}
			if (fieldName == null || fieldName.length() == 0) {
				throw new IllegalArgumentException("(fieldName == null || fieldName.length() == 0), fieldName=["
						+ fieldName + "]");
			}
			try {
				Field field = clsObj.getDeclaredField(fieldName);
				boolean keepStatus = field.isAccessible();
				if (!keepStatus) {
					field.setAccessible(true);
				}
				try {
					field.set(reflectObj, newValue);
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
		public static <T> T getField(Object reflectObj, Class<?> clsObj, String fieldName) {
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
		public static <T> T invokeMethod(Object reflectObj, Class<?> clsObj, String methodName) {
			return invokeMethod(reflectObj, clsObj, methodName, new Class<?>[0], new Object[0]);
		}

		/**
		 *
		 * @param reflectObj null for static method
		 */
		@SuppressWarnings({ "unchecked" })
		public static <T> T invokeMethod(Object reflectObj, Class<?> clsObj, String methodName, Class<?>[] argsTypes,
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

		/**
		 * @param reflectObj may be null.
		 */
		static public <T> T invokeMethod(Object reflectObj, Method method, Object[] argsValues) {
			if (method == null) {
				throw new IllegalArgumentException("(method == null)");
			}
			if (method.getParameterTypes().length != argsValues.length) {
				throw new IllegalArgumentException(
						"(method.getParameterTypes().length != argsValues.length), method.parameterTypes.length="
								+ method.getParameterTypes().length + ", argsValues.length=" + argsValues.length);
			}
			try {
				boolean keepStatus = method.isAccessible();
				if (!keepStatus) {
					method.setAccessible(true);
				}
				try {
					@SuppressWarnings("unchecked")
					T obj = (T) method.invoke(reflectObj, argsValues);
					return obj;
				} finally {
					method.setAccessible(keepStatus);
				}
			} catch (Exception e) {
				Throwable t = e.getCause() != null && e instanceof InvocationTargetException ? e.getCause() : e;
				throw new RuntimeException(method.getName(), t);
			}
		}

		static public void listMethod(Class<?> clsObj, Logger log) {
			if (clsObj == null) {
				throw new IllegalArgumentException("(clsObj == null)");
			}
			Method[] methods = clsObj.getDeclaredMethods(); // getMethods();
			for (int i = 0; i < methods.length; i++) {
				log.debug("methods[" + i + "], getName=[" + methods[i].getName() + "], toString=[" + methods[i].toString()
						+ "], isAccessible=[" + methods[i].isAccessible() + "]");
			}
		}

		static public Method[] getMethod(Class<?> clsObj, String methodName) {
			Method[] methods = clsObj.getDeclaredMethods();
			List<Method> findMethods = new ArrayList<>();
			for (Method method : methods) {
				if (method.getName().equals(methodName)) {
					findMethods.add(method);
				}
			}
			return findMethods.toArray(new Method[0]);
		}
	}

}
