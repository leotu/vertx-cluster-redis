
package io.vertx.spi.cluster.redis.impl;

import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.spi.cluster.redis.RedisClusterManager;

/**
 * 
 * @see io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo
 * @see io.vertx.core.net.impl.ServerID
 * @see org.redisson.codec.JsonJacksonCodec
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class RedisAsyncMultiMapEventbus extends RedisAsyncMultiMap<String, ClusterNodeInfo> {
	private static final Logger log = LoggerFactory.getLogger(RedisAsyncMultiMapEventbus.class);

	@SuppressWarnings("unused")
	private final RedisClusterManager clusterManager;

	public RedisAsyncMultiMapEventbus(Vertx vertx, RedisClusterManager clusterManager, RedissonClient redisson,
			String name) {
		super(vertx, redisson, name);
		this.clusterManager = clusterManager;
		log.debug("name={}", name);
	}
}
