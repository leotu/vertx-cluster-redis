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

import java.util.concurrent.TimeUnit;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Non Standard Vertx's API
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public interface ExpirableAsync<K> {

	/**
	 * Remaining time to live
	 * 
	 * @return TTL in milliseconds
	 */
	void getTTL(K k, Handler<AsyncResult<Long>> resultHandler);

	/**
	 * Refresh TTL if present. Only update elements that already exist. Never add elements.
	 * 
	 * @return The number of elements added to the sorted sets, not including elements already existing for which the
	 *         score was updated
	 */
	void refreshTTLIfPresent(K k, long timeToLive, TimeUnit timeUnit, Handler<AsyncResult<Long>> resultHandler);

}
