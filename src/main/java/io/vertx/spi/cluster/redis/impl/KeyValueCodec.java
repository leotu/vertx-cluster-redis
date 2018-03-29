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
package io.vertx.spi.cluster.redis.impl;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/**
 *
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class KeyValueCodec implements Codec {
	private final Encoder valueEncoder;
	private final Decoder<Object> valueDecoder;

	private final Encoder mapKeyEncoder;
	private final Decoder<Object> mapKeyDecoder;

	private final Encoder mapValueEncoder;
	private final Decoder<Object> mapValueDecoder;

	public KeyValueCodec(Encoder valueEncoder, Decoder<Object> valueDecoder, Encoder mapKeyEncoder,
			Decoder<Object> mapKeyDecoder, Encoder mapValueEncoder, Decoder<Object> mapValueDecoder) {
		this.valueEncoder = valueEncoder;
		this.valueDecoder = valueDecoder;
		this.mapKeyEncoder = mapKeyEncoder;
		this.mapKeyDecoder = mapKeyDecoder;
		this.mapValueEncoder = mapValueEncoder;
		this.mapValueDecoder = mapValueDecoder;
	}

	@Override
	public Decoder<Object> getMapValueDecoder() {
		return mapValueDecoder;
	}

	@Override
	public Encoder getMapValueEncoder() {
		return mapValueEncoder;
	}

	@Override
	public Decoder<Object> getMapKeyDecoder() {
		return mapKeyDecoder;
	}

	@Override
	public Encoder getMapKeyEncoder() {
		return mapKeyEncoder;
	}

	@Override
	public Decoder<Object> getValueDecoder() {
		return valueDecoder;
	}

	@Override
	public Encoder getValueEncoder() {
		return valueEncoder;
	}

}
