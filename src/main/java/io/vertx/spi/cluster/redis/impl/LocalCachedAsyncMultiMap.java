package io.vertx.spi.cluster.redis.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.redisson.api.RFuture;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.NonPublicAPI.LocalCached;

public class LocalCachedAsyncMultiMap<K, V> implements AsyncMultiMap<K, V>, LocalCached {
	private static final Logger log = LoggerFactory.getLogger(LocalCachedAsyncMultiMap.class);

	static private boolean debug = false;

	private final AsyncMultiMap<K, V> delegate;
	private final Vertx vertx;
	private final ClusterManager clusterManager;

	protected ConcurrentMap<K, ChoosableIterable<V>> choosableSetLocalCached = new ConcurrentHashMap<>();

	protected RTopic<K> subsTopic;
	protected int topicListenerId;

	protected long timerId;

	public LocalCachedAsyncMultiMap(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson,
			AsyncMultiMap<K, V> delegate, int timeoutInSecoinds, String topicName) {
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.delegate = delegate;
		this.subsTopic = redisson.getTopic(topicName);
		this.topicListenerId = this.subsTopic.addListener(new MessageListener<K>() {
			@Override
			public void onMessage(String channel, K key) {
				if (key == null) {
					if (debug) {
						log.debug("*** [{}]: clear all, size: {}", topicListenerId, choosableSetLocalCached.size());
					}
					clearAll();
				} else {
					ChoosableIterable<V> previous = choosableSetLocalCached.remove(key);
					if (debug) {
						log.debug("*** [{}], key: {}, previous: {}", topicListenerId, key, previous);
					}
				}
			}
		});
		this.timerId = vertx.setPeriodic(1000 * timeoutInSecoinds, id -> {
			if (debug) {
				log.debug("timerId: {}, clear all, size: {}", timerId, choosableSetLocalCached.size());
			}
			choosableSetLocalCached.clear();
		});

		if (debug) {
			log.debug("topicName: {}, timeoutInSecoinds: {}", topicName, timeoutInSecoinds);
		}
	}

	public void close() {
		clearAll();
		if (timerId > 0) {
			if (debug) {
				log.debug("cancelTimer: {}", timerId);
			}
			vertx.cancelTimer(timerId);
			timerId = 0;
		}
		if (topicListenerId > 0) {
			if (debug) {
				log.debug("remove topicListenerId: {}", topicListenerId);
			}
			subsTopic.removeListener(topicListenerId);
			topicListenerId = 0;
		}
	}

	@Override
	public void clearAll() {
		choosableSetLocalCached.clear();
	}

	@Override
	public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
		delegate.add(k, v, ar -> {
			if (ar.succeeded()) {
				publishLog(subsTopic.publishAsync(k), k, "add");
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});

	}

	@Override
	public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
		ChoosableIterable<V> valuesLocalCached = choosableSetLocalCached.get(k);
		if (valuesLocalCached != null && !valuesLocalCached.isEmpty()) {
			resultHandler.handle(Future.succeededFuture(valuesLocalCached));
			return;
		}

		delegate.get(k, ar -> {
			if (ar.succeeded()) {
				ChoosableIterable<V> result = ar.result();
				choosableSetLocalCached.put(k, result);
				if (debug) {
					log.debug("put: {}, size: {}", k, choosableSetLocalCached.size());
				}
				resultHandler.handle(Future.succeededFuture(result));
			} else {
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
		delegate.remove(k, v, ar -> {
			publishLog(subsTopic.publishAsync(k), k, "remove");
			if (ar.succeeded()) {
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
		delegate.removeAllForValue(v, ar -> {
			publishLog(subsTopic.publishAsync(null), null, "removeAllForValue");
			if (ar.succeeded()) {
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
		delegate.removeAllMatching(p, ar -> {
			publishLog(subsTopic.publishAsync(null), null, "removeAllMatching");
			if (ar.succeeded()) {
				completionHandler.handle(Future.succeededFuture(ar.result()));
			} else {
				completionHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	private void publishLog(RFuture<Long> fu, K k, String method) {
		fu.whenComplete((numOfClientsRreceived, error) -> {
			if (error == null) {
				int numOfNodes = clusterManager.getNodes().size();
				if (numOfClientsRreceived != numOfNodes && debug) {
					log.info("{}({}) numOfClientsRreceived: {}, numOfNodes: {}", method, k, numOfClientsRreceived, numOfNodes);
				} else if (debug) {
					log.debug("{}({}) numOfClientsRreceived: {}, numOfNodes: {}", method, k, numOfClientsRreceived, numOfNodes);
				}
			} else {
				log.warn("{}({}), error: {}", method, k, error.toString());
			}
		});
	}

}
