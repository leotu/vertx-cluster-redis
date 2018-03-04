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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.SLF4JLogDelegateFactory;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class MiscTest {
	private static final Logger log;
	static {
		System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
		log = LoggerFactory.getLogger(MiscTest.class);
	}

	@Test
	public void testConcurrentMap() throws Exception {
		final ConcurrentMap<String, String> cmap = new ConcurrentHashMap<>();

		cmap.computeIfAbsent("ABC", key -> {
			log.debug("key={}", key);
			return "123";
		});
		log.debug("cmap.size={}, cmap={}", cmap.size(), cmap);

		cmap.computeIfAbsent("DEF", key -> {
			log.debug("key={}", key);
			return null;
		});
		log.debug("cmap.size={}, cmap={}", cmap.size(), cmap);

	}
}
