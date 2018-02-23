
package io.vertx.spi.cluster.redis.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * SUBS_MAP_NAME = "__vertx.subs"
 * <p/>
 * When last node disconnected will still keep it's subs address. (Don't remove last node subs, "__vertx.subs" are not
 * empty !)
 * 
 * 
 * @see io.vertx.core.net.impl.ServerID
 * @see org.redisson.codec.JsonJacksonCodec
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisAsyncMultiMapSubs extends RedisAsyncMultiMap<String, ClusterNodeInfo> {
	private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMapSubs.class);

	static private boolean debug = false;

	public RedisAsyncMultiMapSubs(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson,
			String name) {
		super(vertx, redisson, name);
	}

	@Override
	public void add(String k, ClusterNodeInfo v, Handler<AsyncResult<Void>> completionHandler) {
		super.add(k, v, completionHandler);
	}

	@Override
	public void get(String k, Handler<AsyncResult<ChoosableIterable<ClusterNodeInfo>>> resultHandler) {
		super.get(k, resultHandler);
	}

	@Override
	public void remove(String k, ClusterNodeInfo v, Handler<AsyncResult<Boolean>> completionHandler) {
		super.remove(k, v, completionHandler);
	}

	@Override
	public void removeAllForValue(ClusterNodeInfo v, Handler<AsyncResult<Void>> completionHandler) {
		removeAllMatching(value -> value == v || value.equals(v), completionHandler);
	}

	/**
	 * Remove values which satisfies the given predicate in all keys.
	 * 
	 * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus#setClusterViewChangedHandler
	 */
	@Override
	public void removeAllMatching(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler) {
		batchRemoveAllMatching(p, completionHandler);
	}

	private void batchRemoveAllMatching(Predicate<ClusterNodeInfo> p, Handler<AsyncResult<Void>> completionHandler) {
		List<Map.Entry<String, ClusterNodeInfo>> deletedList = new ArrayList<>();
		mmap.entries().forEach(entry -> {
			String key = entry.getKey();
			ClusterNodeInfo value = entry.getValue();
			if (p.test(value)) { // XXX: "!members.contains(ci.nodeId)"
				deletedList.add(entry);
				if (debug) {
					log.debug("add remove key={}, value.class={}, value={}", key, value.getClass().getName(), value);
				}
			} else {
				if (debug) {
					log.debug("skip remove key={} value.class={}, value={}", key, value.getClass().getName(), value);
				}
			}
		});

		if (!deletedList.isEmpty()) {
			RBatch batch = redisson.createBatch();
			deletedList.forEach(entry -> {
				mmap.removeAsync(entry.getKey(), entry.getValue());
			});

			batch.atomic().skipResult().executeAsync().whenCompleteAsync((result, e) -> {
				if (e != null) {
					log.warn("error: {}", e.toString());
					completionHandler.handle(Future.failedFuture(e));
				} else { // XXX: skipResult() ==> result.class=<null>, result=null
					completionHandler.handle(Future.succeededFuture());
				}
			});
		}
	}
}
