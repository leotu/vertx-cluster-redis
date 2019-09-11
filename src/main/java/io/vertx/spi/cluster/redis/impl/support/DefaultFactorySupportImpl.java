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


import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.FactorySupport;

/**
 * 
 * @see org.redisson.api.RLocalCachedMap
 * @see org.redisson.Redisson#getLocalCachedMap
 * @see org.redisson.api.LocalCachedMapOptions
 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder
 * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class FactorySupportImpl implements FactorySupport {

	@Override
	public PendingMessageProcessor createPendingMessageProcessor(Vertx vertx, ClusterManager clusterManager,
			AsyncMultiMap<String, ClusterNodeInfo> subs) {
		return new PendingMessageProcessorImpl(vertx, clusterManager, subs);
	}

}
