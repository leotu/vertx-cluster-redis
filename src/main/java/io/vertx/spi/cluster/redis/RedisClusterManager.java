package io.vertx.spi.cluster.redis;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.impl.HAManager;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMap;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMap;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMapEventbus;
import io.vertx.spi.cluster.redis.impl.RedisMap;
import io.vertx.spi.cluster.redis.impl.RedisMapEventbus;

/**
 * https://github.com/redisson/redisson/wiki/7.-%E5%88%86%E5%B8%83%E5%BC%8F%E9%9B%86%E5%90%88#72-%E5%A4%9A%E5%80%BC%E6%98%A0%E5%B0%84multimap
 * 
 * @see io.vertx.core.impl.VertxFactoryImpl#clusteredVertx
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisClusterManager implements ClusterManager {
	private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

	private Vertx vertx;
	private final RedissonClient redisson;
	private final boolean customClient;
	private String nodeID;

	private volatile boolean active;
	private NodeListener nodeListener;

	private RedisMapEventbus haInfo;
	private RedisAsyncMultiMapEventbus subs;

	public static final String CLUSTER_MAP_NAME = "__vertx.haInfo"; // HAManager.class
	public static final String SUBS_MAP_NAME = "__vertx.subs"; // ClusteredEventBus.class

	public RedisClusterManager(RedissonClient redisson, String nodeID) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(nodeID, "nodeID");
		this.redisson = redisson;
		this.nodeID = nodeID;
		this.customClient = true;
		log.debug("nodeID={}", nodeID);
	}

	public RedisClusterManager(JsonObject config) {
		Objects.requireNonNull(config, "config");
		log.debug("config={}", config);

		String redisHost = config.getString("redisHost");
		Integer redisPort = config.getInteger("redisPort");
		Integer database = config.getInteger("database");
		Config redissonConfig = new Config();
		redissonConfig.useSingleServer() //
				.setAddress("redis://" + redisHost + ":" + redisPort) //
				.setDatabase(database);
		this.redisson = Redisson.create(redissonConfig);
		this.nodeID = redisHost + "_" + redisPort;
		this.nodeID = UUID.nameUUIDFromBytes(nodeID.getBytes(StandardCharsets.UTF_8)).toString();
		this.customClient = false;
		log.debug("nodeID={}", nodeID);
	}

	/**
	 * (1)
	 */
	@Override
	public void setVertx(Vertx vertx) {
		log.debug("nodeID={}, vertx={}", nodeID, vertx);
		this.vertx = vertx;
	}

	/**
	 * (5)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
		log.debug("nodeID={}, name={}", nodeID, name);
		vertx.executeBlocking(future -> {
			if (name.equals(SUBS_MAP_NAME)) {
				subs = new RedisAsyncMultiMapEventbus(vertx, this, redisson, name);
				future.complete((AsyncMultiMap<K, V>) subs);
			} else {
				future.complete(new RedisAsyncMultiMap<K, V>(vertx, redisson, name));
			}
		}, resultHandler);
	}

	@Override
	public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
		log.debug("nodeID={}, name={}", nodeID, name);
		vertx.executeBlocking(future -> {
			future.complete(new RedisAsyncMap<K, V>(vertx, redisson, name));
		}, resultHandler);
	}

	/**
	 * (3)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<K, V> getSyncMap(String name) {
		log.debug("nodeID={}, name={}", nodeID, name);
		if (name.equals(CLUSTER_MAP_NAME)) {
			haInfo = new RedisMapEventbus(vertx, this, redisson, name);
			return (Map<K, V>) haInfo;
		} else {
			return new RedisMap<K, V>(vertx, redisson, name);
		}
	}

	// protected RedisMapEventbus getHaInfo() {
	// return haInfo;
	// }
	//
	// protected RedisAsyncMultiMapEventbus getSubs() {
	// return subs;
	// }

	@Override
	public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
		log.debug("nodeID={}, name={}, timeout={}", nodeID, name, timeout);
		try {
			RLock lock = redisson.getLock(name); // getFairLock ?
			lock.tryLockAsync(timeout, TimeUnit.MILLISECONDS).whenComplete((v, e) -> resultHandler
					.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(new RedisLock(lock))));
		} catch (Exception e) {
			log.error("nodeID=" + nodeID + ", name=" + name + ", timeout=" + timeout, e);
			resultHandler.handle(Future.failedFuture(e));
		}
	}

	@Override
	public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
		log.debug("nodeID={}, name={}", nodeID, name);
		try {
			RAtomicLong counter = redisson.getAtomicLong(name);
			resultHandler.handle(Future.succeededFuture(new RedisCounter(counter)));
		} catch (Exception e) {
			log.error("nodeID=" + nodeID + ", name=" + name, e);
			resultHandler.handle(Future.failedFuture(e));
		}
	}

	@Override
	public String getNodeID() {
		return nodeID;
	}

	@Override
	public List<String> getNodes() {
		List<String> nodes = haInfo.keySet().stream().map(e -> e.toString()).collect(Collectors.toList());
		log.debug("nodeID={}, nodes.size={}", nodeID, nodes.size());
		return nodes;
	}

	public boolean isInactive() {
		final VertxInternal vertxInternal = (VertxInternal) vertx;
		final ClusteredEventBus eventBus = (ClusteredEventBus) vertx.eventBus();
		if (eventBus != null) {
			final HAManager haManager = getFinalField(eventBus, ClusteredEventBus.class, "haManager");
			if (haManager != null) {
				final boolean haManagerStopped = getField(haManager, HAManager.class, "stopped");
				return vertxInternal.isKilled() || !isActive() || redisson.isShutdown() || redisson.isShuttingDown()
						|| haManager.isKilled() || haManagerStopped;
			} else {
				return vertxInternal.isKilled() || !isActive() || redisson.isShutdown() || redisson.isShuttingDown();
			}
		} else {
			return !isActive() || redisson.isShutdown() || redisson.isShuttingDown();
		}
	}

	/**
	 * (4)
	 */
	@Override
	public void nodeListener(NodeListener nodeListener) {
		log.debug("nodeID={}, nodeListener={}", nodeID, nodeListener);
		this.nodeListener = new NodeListener() {
			@Override
			synchronized public void nodeAdded(String nodeID) {
				if (nodeListener != null && !isInactive()) {
					nodeListener.nodeAdded(nodeID);
				} else {
					log.debug("skip call nodeAdded(...)");
				}
			}

			@Override
			synchronized public void nodeLeft(String nodeID) {
				if (nodeListener != null && !isInactive()) {
					nodeListener.nodeLeft(nodeID);
				} else {
					log.debug("skip call nodeLeft(...)");
				}
			}
		};
		this.haInfo.attachListener(this.nodeListener);
	}

	/**
	 * (2)
	 */
	@Override
	public void join(Handler<AsyncResult<Void>> resultHandler) {
		log.debug("nodeID={}", nodeID);
		// Thread.dumpStack();
		vertx.executeBlocking(future -> {
			if (active) {
				future.fail(new Exception("already active"));
			} else {
				active = true;
				future.complete();
			}
		}, resultHandler);
	}

	/**
	 * (6)
	 */
	@Override
	public void leave(Handler<AsyncResult<Void>> resultHandler) {
		log.debug("nodeID={}", nodeID);
		// Thread.dumpStack();
		vertx.executeBlocking(future -> {
			synchronized (RedisClusterManager.this) {
				if (!active) {
					future.fail(new Exception("already inactive"));
				} else {
					active = false;
					try {
						haInfo.stop();
						if (!customClient) {
							redisson.shutdown(5, 15, TimeUnit.SECONDS);
						}
						nodeListener = null;
						future.complete();
					} catch (Exception e) {
						future.fail(e);
					}
				}
			}
		}, resultHandler);
	}

	@Override
	public boolean isActive() {
		// log.debug("nodeID={}, active={}", nodeID, active);
		return active;
	}

	/**
	 * Lock implement
	 */
	private class RedisCounter implements Counter {
		private final RAtomicLong counter;

		public RedisCounter(RAtomicLong counter) {
			this.counter = counter;
		}

		@Override
		public void get(Handler<AsyncResult<Long>> resultHandler) {
			counter.getAsync().whenComplete(
					(v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
			counter.incrementAndGetAsync().whenComplete(
					(v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
			counter.getAndIncrementAsync().whenComplete(
					(v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
			counter.decrementAndGetAsync().whenComplete(
					(v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
			counter.addAndGetAsync(value).whenComplete(
					(v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
			counter.getAndAddAsync(value).whenComplete(
					(v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
			counter.compareAndSetAsync(expected, value).whenComplete(
					(v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

	}

	/**
	 * Lock implement
	 */
	private class RedisLock implements Lock {
		private final RLock lock;

		public RedisLock(RLock lock) {
			this.lock = lock;
		}

		@Override
		public void release() {
			lock.unlock();
		}

	}

	private <T> T getFinalField(Object reflectObj, Class<?> clsObj, String fieldName) {
		Objects.requireNonNull(reflectObj, "reflectObj");
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
			e.printStackTrace();
			throw new RuntimeException(fieldName, e);
		}
	}

	private <T> T getField(Object reflectObj, Class<?> clsObj, String fieldName) {
		Objects.requireNonNull(reflectObj, "reflectObj");
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
			e.printStackTrace();
			throw new RuntimeException(fieldName, e);
		}
	}
}
