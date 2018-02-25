package io.vertx.spi.cluster.redis;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.json.JsonObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.NonPublicAPI.ClusteredEventBusAPI;
import io.vertx.spi.cluster.redis.NonPublicAPI.Reflection;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMap;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMap;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMapSubs;
import io.vertx.spi.cluster.redis.impl.RedisMap;
import io.vertx.spi.cluster.redis.impl.RedisMapHaInfo;

/**
 * 
 * @see io.vertx.core.impl.VertxFactoryImpl#clusteredVertx
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisClusterManager implements ClusterManager {
	private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

	static private boolean debug = false;

	private Vertx vertx;
	private final RedissonClient redisson;
	@SuppressWarnings("unused")
	private final boolean customRedissonClient;
	private String nodeId;
	private ClusteredEventBus eventBus;

	private volatile boolean active;
	private NodeListener nodeListener;

	private RedisMapHaInfo haInfo;
	private RedisAsyncMultiMapSubs subs;

	public static final String CLUSTER_MAP_NAME = NonPublicAPI.HA_CLUSTER_MAP_NAME;
	public static final String SUBS_MAP_NAME = NonPublicAPI.EB_SUBS_MAP_NAME;

	public RedisClusterManager(RedissonClient redisson, String nodeId) {
		Objects.requireNonNull(redisson, "redisson");
		Objects.requireNonNull(nodeId, "nodeId");
		this.redisson = redisson;
		this.nodeId = nodeId;
		this.customRedissonClient = true;
	}

	public RedisClusterManager(JsonObject config) {
		Objects.requireNonNull(config, "config");
		// log.debug("config={}", config);

		String redisHost = config.getString("redisHost");
		Integer redisPort = config.getInteger("redisPort");
		Integer database = config.getInteger("database");
		Config redissonConfig = new Config();
		redissonConfig.useSingleServer() //
				.setAddress("redis://" + redisHost + ":" + redisPort) //
				.setDatabase(database);
		this.redisson = Redisson.create(redissonConfig);
		this.nodeId = redisHost + "_" + redisPort;
		this.nodeId = UUID.nameUUIDFromBytes(nodeId.getBytes(StandardCharsets.UTF_8)).toString();
		this.customRedissonClient = false;
	}

	private void readyEventBus(ClusteredEventBus eventBus, RedisAsyncMultiMapSubs subs) {
		// log.debug("...");
		this.eventBus = eventBus;

		@SuppressWarnings("unchecked")
		ConcurrentMap<ServerID, Object> oldOne = (ConcurrentMap<ServerID, Object>) ClusteredEventBusAPI
				.getConnections(this.eventBus);
		@SuppressWarnings("serial")
		ConcurrentMap<ServerID, Object> newOne = new ConcurrentHashMap<ServerID, Object>() {
			PendingMessageProcessor pendingProcessor = new PendingMessageProcessor(vertx, eventBus, subs);

			/**
			 * @param key is ServerID type
			 * @param value is ConnectionHolder type
			 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder#close
			 */
			@Override
			public boolean remove(Object serverID, Object connHolder) {
				boolean ok = super.remove(serverID, connHolder);
				if (ok) {
					Queue<ClusteredMessage<?, ?>> pending = Reflection.getField(connHolder, connHolder.getClass(), "pending");
					if (pending != null && !pending.isEmpty()) {
						pendingProcessor.accept((ServerID) serverID, pending);
					}
				} else {
					if (debug) {
						log.debug("removed ok: {}, serverID: {}, connHolder: {}", ok, serverID, connHolder);
					}
				}
				return ok;
			}
		};
		ClusteredEventBusAPI.setConnections(this.eventBus, newOne); // reset to new Instance
		if (!oldOne.isEmpty()) {
			if (debug) {
				log.debug("(!oldOne.isEmpty()), oldOne.size: {}", oldOne.size());
			}
			newOne.putAll(oldOne);
		}
	}

	public ClusteredEventBus getEventBus() {
		return eventBus;
	}

	/**
	 * (1)
	 */
	@Override
	public void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}

	/**
	 * (5), eventBus been created !
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
		vertx.executeBlocking(future -> {
			if (name.equals(SUBS_MAP_NAME)) {
				subs = new RedisAsyncMultiMapSubs(vertx, this, redisson, name);
				readyEventBus(ClusteredEventBusAPI.getEventBus(vertx), subs);
				future.complete((AsyncMultiMap<K, V>) subs);
			} else {
				future.complete(new RedisAsyncMultiMap<K, V>(vertx, redisson, name));
			}

		}, resultHandler);
	}

	@Override
	public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
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
		if (name.equals(CLUSTER_MAP_NAME)) {
			haInfo = new RedisMapHaInfo(vertx, this, redisson, name);
			return (Map<K, V>) haInfo;
		} else {
			return new RedisMap<K, V>(vertx, redisson, name);
		}
	}

	@Override
	public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
		try {
			RLock lock = redisson.getLock(name); // getFairLock ?
			lock.tryLockAsync(timeout, TimeUnit.MILLISECONDS).whenComplete((v, e) -> resultHandler
					.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(new RedisLock(lock))));
		} catch (Exception e) {
			log.warn("nodeId: " + nodeId + ", name: " + name + ", timeout: " + timeout, e);
			resultHandler.handle(Future.failedFuture(e));
		}
	}

	@Override
	public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
		try {
			RAtomicLong counter = redisson.getAtomicLong(name);
			resultHandler.handle(Future.succeededFuture(new RedisCounter(counter)));
		} catch (Exception e) {
			log.error("nodeId: " + nodeId + ", name: " + name, e);
			resultHandler.handle(Future.failedFuture(e));
		}
	}

	@Override
	public String getNodeID() {
		return nodeId;
	}

	/**
	 * @see io.vertx.core.impl.HAManager#addHaInfoIfLost
	 */
	@Override
	public List<String> getNodes() {
		return haInfo.keySet().stream().map(e -> e.toString()).collect(Collectors.toList());
	}

	/**
	 * (4)
	 * 
	 * @see io.vertx.core.impl.HAManager#nodeAdded
	 * @see io.vertx.core.impl.HAManager#nodeLeft
	 */
	@Override
	public void nodeListener(NodeListener nodeListener) {
		this.nodeListener = new NodeListener() {
			@Override
			synchronized public void nodeAdded(String nodeId) {
				// if (isActive()) {
				nodeListener.nodeAdded(nodeId);
				// } else {
				// log.warn("Inactive, skip execute nodeAdded({})", nodeId);
				// }
			}

			/**
			 * The method won't delete it's own subs
			 */
			@Override
			synchronized public void nodeLeft(String nodeId) {
				// if (isActive()) {
				nodeListener.nodeLeft(nodeId);
				// } else {
				// log.warn("Inactive, skip execute nodeLeft({})", nodeId);
				// }
			}
		};
		this.haInfo.attachListener(this.nodeListener);
	}

	/**
	 * (2)
	 */
	@Override
	public void join(Handler<AsyncResult<Void>> resultHandler) {
		if (debug) {
			log.debug("join nodeId={}", nodeId);
		}
		if (active) {
			throw new IllegalStateException("Already activated");
		}
		vertx.executeBlocking(future -> {
			if (active) {
				future.fail(new IllegalStateException("Already activated"));
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
		if (debug) {
			log.debug("leave nodeId={}", nodeId);
		}
		if (!active) {
			throw new IllegalStateException("Already inactive");
		}
		vertx.executeBlocking(future -> {
			synchronized (RedisClusterManager.this) {
				if (!active) {
					future.fail(new IllegalStateException("Already inactive"));
				} else {
					active = false;
					try {
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
		return active;
	}

	// public boolean isInactive() {
	// return nodeListener == null || !isActive() || NonPublicAPI.isInactive(vertx, redisson);
	// }

	// public void close() {
	// subs.close();
	// haInfo.close();
	// if (!customClient) {
	// redisson.shutdown(5, 15, TimeUnit.SECONDS);
	// }
	// }

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
			counter.getAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
			counter.incrementAndGetAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
			counter.getAndIncrementAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
			counter.decrementAndGetAsync()
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
			counter.addAndGetAsync(value)
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
			counter.getAndAddAsync(value)
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
		}

		@Override
		public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
			counter.compareAndSetAsync(expected, value)
					.whenComplete((v, e) -> resultHandler.handle(e != null ? Future.failedFuture(e) : Future.succeededFuture(v)));
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

}
