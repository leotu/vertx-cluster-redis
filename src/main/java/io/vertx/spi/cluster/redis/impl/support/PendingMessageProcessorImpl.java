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

import java.lang.reflect.InvocationTargetException;

//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryContext;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.impl.MessageImpl;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.FactorySupport.PendingMessageProcessor;

/**
 * Retryable to choose another server ID
 * 
 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class PendingMessageProcessorImpl implements PendingMessageProcessor {
	private static final Logger log = LoggerFactory.getLogger(PendingMessageProcessorImpl.class);

	private static final String PING_ADDRESS = "__vertx_ping";
	private static final String GENERATED_REPLY_ADDRESS_PREFIX = "__vertx.reply.";

	private final String retryHeaderKey = "__retry_outbound_interceptor__";
	private final String retryCounterHeaderKey = retryHeaderKey + "counter";

	private Vertx vertx;
	@SuppressWarnings("unused")
	private ClusterManager clusterManager;
	private ClusteredEventBus eventBus;
	@SuppressWarnings("unused")
	private AsyncMultiMap<String, ClusterNodeInfo> subs;

	public PendingMessageProcessorImpl(Vertx vertx, ClusterManager clusterManager,
			AsyncMultiMap<String, ClusterNodeInfo> subs) {
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.eventBus = (ClusteredEventBus) vertx.eventBus();
		this.subs = subs;
	}

	@Override
	public void run() {
		log.debug("...");
//		if (true) {
//			
//		}
		eventBus.addOutboundInterceptor(ctx -> { // sendInterceptors (sendReply)
			Message<?> message = ctx.message();
			if (ctx.send() && message instanceof ClusteredMessage) {
				// log.debug("###<< BEFORE: OutboundInterceptor ctx: {}", ctx.message().body());
				ClusteredMessage<?, ?> msg = (ClusteredMessage<?, ?>) message;
				boolean fromRetry = msg.headers().get(retryHeaderKey) != null;
				if (!msg.isFromWire() && !PING_ADDRESS.equals(msg.address())
						&& !msg.address().startsWith(GENERATED_REPLY_ADDRESS_PREFIX) //
						&& !(msg.writeHandler() instanceof PendingWriteHandler) && !fromRetry) {
					log.debug("address: {}, fromRetry: {}, fromWire: {}, replyAddress: {}", msg.address(), fromRetry,
							msg.isFromWire(), msg.replyAddress());
					PendingWriteHandler pendingWriteHandler = new PendingWriteHandler(vertx, ctx, msg);
					NonPublicAPI.Reflection.setField(msg, MessageImpl.class, "writeHandler", pendingWriteHandler);
				}
				// log.debug("###<< AFTER: OutboundInterceptor ctx: {}", ctx.message().body());
				ctx.next();
			} else {
				ctx.next();
			}
		});
	}

	/**
	 *
	 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder
	 */
	public class PendingWriteHandler implements Handler<AsyncResult<Void>> {

		private final Vertx vertx;
		private final DeliveryContext<?> ctx;
		private final ClusteredMessage<?, ?> msg;
		private final Handler<AsyncResult<Void>> wrapWriteHandler;

		public PendingWriteHandler(Vertx vertx, DeliveryContext<?> ctx, ClusteredMessage<?, ?> msg) {
			this.vertx = vertx;
			this.ctx = ctx;
			this.msg = msg;
			this.wrapWriteHandler = msg.writeHandler();
			// log.debug("wrapWriteHandler: {}", wrapWriteHandler);
		}

		@Override
		public void handle(AsyncResult<Void> ar) {
			boolean retriable = false;
			if (wrapWriteHandler == null) {
				if (ar.failed()) {
					if (isConnectionRefusedErr(ar.cause())) {
//						log.warn("<< isConnectionRefusedErr msg.address: " + msg.address() + ", result failed!",
//								ar.cause());
						retriable = true;
						action(vertx, msg);
					}
				}
			} else {
				if (ar.succeeded()) {
					wrapWriteHandler.handle(Future.succeededFuture(ar.result()));
				} else {
					if (isConnectionRefusedErr(ar.cause())) {
//						log.warn("<< isConnectionRefusedErr msg.address: {}, result failed: {}", msg.address(),
//								ar.cause().toString());
						wrapWriteHandler.handle(Future.failedFuture(ar.cause()));
						retriable = true;
						action(vertx, msg);
					}
					else {
						wrapWriteHandler.handle(Future.failedFuture(ar.cause()));
					}
				}
			}
		}
	}

	private void action(Vertx vertx, ClusteredMessage<?, ?> msg) {
		msg.headers().set(retryHeaderKey, msg.address());

		int fromRetryCounter = 0;
		try {
			fromRetryCounter = Integer.parseInt(msg.headers().get(retryCounterHeaderKey));
		} catch (NumberFormatException e) {
			// ignored
		}
		// log.debug("<< OutboundInterceptor msg.address: {}, fromRetryCounter: {}", msg.address(), fromRetryCounter);
		msg.headers().set(retryCounterHeaderKey, String.valueOf(fromRetryCounter + 1));
	}

	private boolean isConnectionRefusedErr(Throwable e) {
		boolean connectionRefusedErr = false;
		while (e != null) {
			if (e instanceof InvocationTargetException) {
				e = ((InvocationTargetException) e).getCause();
			}
			String errMsg = e.getMessage();
			if (errMsg != null && errMsg.startsWith("Connection refused:")) { // Connection refused: /192.168.99.1:18081
				connectionRefusedErr = true;
			}
			e = e.getCause();
		}
		return connectionRefusedErr;
	}

}
