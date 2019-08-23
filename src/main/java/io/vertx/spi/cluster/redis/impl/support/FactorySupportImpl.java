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
package io.vertx.spi.cluster.redis.impl.support;

<<<<<<< HEAD
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

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
	//private static final Logger log = LoggerFactory.getLogger(FactorySupportImpl.class);

	@Override
	public PendingMessageProcessor createPendingMessageProcessor(Vertx vertx, ClusterManager clusterManager,
			AsyncMultiMap<String, ClusterNodeInfo> subs) {
		return new PendingMessageProcessorImpl(vertx, clusterManager, subs);
=======
import java.lang.reflect.InvocationTargetException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.MessageImpl;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.FactorySupport;
import io.vertx.spi.cluster.redis.impl.support.NonPublicAPI.ClusteredEventBusAPI;
import io.vertx.spi.cluster.redis.impl.support.NonPublicAPI.ClusteredEventBusAPI.ConnectionHolderAPI;

/**
 * 
 * @see org.redisson.api.RLocalCachedMap
 * @see org.redisson.Redisson#getLocalCachedMap
 * @see org.redisson.api.LocalCachedMapOptions
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class FactorySupportImpl implements FactorySupport {
	private static final Logger log = LoggerFactory.getLogger(FactorySupportImpl.class);

//	public static final String CLUSTER_MAP_NAME = NonPublicAPI.HA_CLUSTER_MAP_NAME;
//	public static final String SUBS_MAP_NAME = NonPublicAPI.EB_SUBS_MAP_NAME;

	private static final String PING_ADDRESS = "__vertx_ping";
	private static final String GENERATED_REPLY_ADDRESS_PREFIX = "__vertx.reply.";

	@SuppressWarnings({ "serial", "unchecked" })
	@Override
	public PendingMessageProcessor createPendingMessageProcessor(Vertx vertx, ClusterManager clusterManager,
			AsyncMultiMap<String, ClusterNodeInfo> subs) {

		ClusteredEventBus eventBus = ClusteredEventBusAPI.eventBus(vertx);
		AtomicReference<PendingMessageProcessor> pendingProcessorRef = new AtomicReference<>();

		ConcurrentMap<ServerID, Object> newConnections = new ConcurrentHashMap<ServerID, Object>() {

			/**
			 * @param key   is ServerID type
			 * @param value is ConnectionHolder type
			 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder#close
			 */
			@Override
			public boolean remove(Object serverID, Object connHolder) {
				boolean wasRemoved = super.remove(serverID, connHolder);
				if (wasRemoved) {
					log.debug("was removed pendingProcessor serverID: {}", serverID);
					pendingProcessorRef.get().run((ServerID) serverID, connHolder);
				} else {
					log.debug("skip pendingProcessor serverID: {}, was removed nothing.", serverID);
				}

				Queue<ClusteredMessage<?, ?>> pending = ConnectionHolderAPI.pending(connHolder);
				if (pending != null) {
					log.debug("serverID: {},  pending.size: {}", serverID, pending.size());
				}
				return wasRemoved;
			}

//			/**
//			 * @param value is ConnectionHolder type
//			 * @see io.vertx.core.eventbus.impl.clustered.ClusteredEventBus#sendRemote
//			 */
//			// @Override
//			public Object putIfAbsent_(ServerID key, Object connHolder) {
//				if (connHolder instanceof ConnectionHolderExt) {
//					return super.putIfAbsent(key, connHolder);
//				} else {
//					ClusteredEventBus eventBus = Reflection.getField(connHolder, connHolder.getClass(), "eventBus");
//					NetClient client = Reflection.getField(connHolder, connHolder.getClass(), "client");
//					ServerID serverID = Reflection.getField(connHolder, connHolder.getClass(), "serverID");
//					Vertx vertx = Reflection.getField(connHolder, connHolder.getClass(), "vertx");
//					EventBusMetrics metrics = Reflection.getField(connHolder, connHolder.getClass(), "metrics");
//					Queue<ClusteredMessage> pending = Reflection.getField(connHolder, connHolder.getClass(), "pending");
//					NetSocket socket = Reflection.getField(connHolder, connHolder.getClass(), "socket");
//					boolean connected = Reflection.getField(connHolder, connHolder.getClass(), "connected");
//					long timeoutID = Reflection.getField(connHolder, connHolder.getClass(), "timeoutID");
//					long pingTimeoutID = Reflection.getField(connHolder, connHolder.getClass(), "pingTimeoutID");
//
//					EventBusOptions options = Reflection.getField(eventBus, eventBus.getClass(), "options");
//
//					ConnectionHolderExt holder = new ConnectionHolderExt(eventBus, serverID, options);
//
//					Reflection.setFinalField(holder, connHolder.getClass(), "eventBus", eventBus);
//					Reflection.setFinalField(holder, connHolder.getClass(), "client", client);
//					Reflection.setFinalField(holder, connHolder.getClass(), "serverID", serverID);
//					Reflection.setFinalField(holder, connHolder.getClass(), "vertx", vertx);
//					Reflection.setFinalField(holder, connHolder.getClass(), "metrics", metrics);
//					Reflection.setFinalField(holder, connHolder.getClass(), "pending", pending);
//					Reflection.setFinalField(holder, connHolder.getClass(), "socket", socket);
//					Reflection.setFinalField(holder, connHolder.getClass(), "connected", connected);
//					Reflection.setFinalField(holder, connHolder.getClass(), "timeoutID", timeoutID);
//					Reflection.setFinalField(holder, connHolder.getClass(), "pingTimeoutID", pingTimeoutID);
//
//					Object prevHolder = super.put(serverID, holder);
//					if (prevHolder == null) {
//						log.debug("put ConnectionHolder serverID: {}", serverID);
//					} else {
////						Queue<ClusteredMessage<?, ?>> pending = ConnectionHolderAPI.pending(prevHolder);
//						if (!(prevHolder instanceof ConnectionHolderExt)) {
//							super.put(serverID, holder);
//						}
//						if (pending != null) {
//							log.debug("serverID: {}, prevHolder pending.size: {}", serverID, pending.size());
//						}
//					}
//					log.debug("prevHolder: {}", prevHolder);
//					return prevHolder;
//				}
//			}
		};

		PendingMessageProcessor pendingProcessor = new PendingMessageProcessorImpl(vertx, clusterManager, eventBus,
				subs, newConnections);
		pendingProcessorRef.set(pendingProcessor);

		ConcurrentMap<ServerID, Object> oldOne = (ConcurrentMap<ServerID, Object>) ClusteredEventBusAPI
				.connections(eventBus);

		log.debug("Reset to new server id connection holder instance");
		ClusteredEventBusAPI.setConnections(eventBus, newConnections); // reset to create new Instance

		if (!oldOne.isEmpty()) {
			log.debug("existing old size: {}", oldOne.size());
		}
		newConnections.putAll(oldOne);

		return pendingProcessor;
	}

	public void eventBusInterceptor(Vertx vertx) {
		vertx.eventBus().addInboundInterceptor(ctx -> { // receiveInterceptors (deliverToHandler)
			if (ctx.message() instanceof ClusteredMessage) {
				ClusteredMessage<?, ?> msg = (ClusteredMessage<?, ?>) ctx.message();
				if (!PING_ADDRESS.equals(msg.address()) && !msg.address().startsWith(GENERATED_REPLY_ADDRESS_PREFIX)) {
					Handler<AsyncResult<Void>> writeHandler = msg.writeHandler();
//				log.debug("@@@>> BEFORE: InboundInterceptor msg.address: {}, writeHandler: {}", msg.address(),
//						writeHandler);
					Handler<AsyncResult<Void>> pendingWriteHandler;
					if (writeHandler == null) {
						pendingWriteHandler = (ar) -> {
							if (ar.succeeded()) {
								log.debug(
										"###>> BEFORE(without writeHandler): InboundInterceptor msg.address: {}, writeHandler result succeeded, body: {}",
										msg.address(), ar.result());
							} else {
								log.warn(
										"###>> BEFORE(without writeHandler): InboundInterceptor msg.address: {}, writeHandler result failed: {}",
										msg.address(), ar.cause().toString());
							}
						};

					} else {
						pendingWriteHandler = (ar) -> {
							if (ar.succeeded()) {
								log.debug(
										"###>> BEFORE(with writeHandler): InboundInterceptor msg.address: {}, writeHandler result succeeded, body: {}",
										msg.address(), ar.result());
								writeHandler.handle(Future.succeededFuture(ar.result()));
							} else {
								boolean connectionRefusedErr = false;
								Throwable e = ar.cause();
								while (e != null) {
									if (e instanceof InvocationTargetException) {
										e = ((InvocationTargetException) e).getCause();
									}
									String errMsg = e.getMessage();
									if (errMsg != null && errMsg.startsWith("Connection refused:")) {
										connectionRefusedErr = true;
									}
									e = e.getCause();
								}

								log.warn(
										"###>> BEFORE(with writeHandler): InboundInterceptor msg.address: {}, writeHandler result failed: {}, connectionRefusedErr: {}",
										msg.address(), ar.cause().toString(), connectionRefusedErr);
								writeHandler.handle(Future.failedFuture(ar.cause()));

							}
						};
					}
					msg.headers().add("__InboundInterceptor__", msg.address());
					NonPublicAPI.Reflection.setField(msg, MessageImpl.class, "writeHandler", pendingWriteHandler);

//				log.debug("@@@>> AFTER: InboundInterceptor msg.address: {}, writeHandler: {}", msg.address(),
//						 msg.writeHandler());
				}
			}

//		log.debug("###>> BEFORE: InboundInterceptor ctx: {}", ctx.message().body());
			ctx.next();
//		log.debug("###>> AFTER: InboundInterceptor ctx: {}", ctx.message().body());
		});

		vertx.eventBus().addOutboundInterceptor(ctx -> { // sendInterceptors (sendReply)
			if (ctx.message() instanceof ClusteredMessage) {
				ClusteredMessage<?,?> msg = (ClusteredMessage<?,?>) ctx.message();
				if (!PING_ADDRESS.equals(msg.address()) && !msg.address().startsWith(GENERATED_REPLY_ADDRESS_PREFIX)) {
					boolean fromRetry = msg.headers().get("__OutboundInterceptor__") != null;
					int fromRetryCounter = 0;
					try {
						fromRetryCounter = Integer.parseInt(msg.headers().get("__OutboundInterceptor__counter"));
					} catch (NumberFormatException e1) {
					}
					msg.headers().add("__OutboundInterceptor__", msg.address());
					msg.headers().add("__OutboundInterceptor__counter", String.valueOf(fromRetryCounter));

					Handler<AsyncResult<Void>> writeHandler = msg.writeHandler();
//				log.debug("@@@<< BEFORE: OutboundInterceptor msg.address: {}, writeHandler: {}", msg.address(),
//						writeHandler);
					Handler<AsyncResult<Void>> pendingWriteHandler;
					if (writeHandler == null) {
						pendingWriteHandler = (ar) -> {
							if (ar.succeeded()) {
								log.debug(
										"###<< BEFORE(without writeHandler): OutboundInterceptor msg.address: {}, writeHandler result succeeded, body: {}",
										msg.address(), ar.result());
							} else {
								// io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection
								// refused: /192.168.99.1:18081

								boolean connectionRefusedErr = false;
								Throwable e = ar.cause();
								while (e != null) {
									if (e instanceof InvocationTargetException) {
										e = ((InvocationTargetException) e).getCause();
									}
									String errMsg = e.getMessage();
									if (errMsg != null && errMsg.startsWith("Connection refused:")) {
										connectionRefusedErr = true;
									}
									e = e.getCause();
								}

								log.warn(
										"###<< BEFORE(without writeHandler): OutboundInterceptor msg.address: {}, writeHandler result failed: {}, connectionRefusedErr: {}, fromRetry: {}",
										msg.address(), ar.cause().toString(), connectionRefusedErr, fromRetry);

								log.warn("###<<===>> BEFORE(without writeHandler): OutboundInterceptor msg.address: "
										+ msg.address() + ", writeHandler result failed!", ar.cause());

							}
						};

					} else {
						pendingWriteHandler = (ar) -> {
							if (ar.succeeded()) {
								log.debug(
										"###<< BEFORE(with writeHandler): OutboundInterceptor msg.address: {}, writeHandler result succeeded, body: {}",
										msg.address(), ar.result());
								writeHandler.handle(Future.succeededFuture(ar.result()));
							} else {
								log.warn(
										"###<< BEFORE(with writeHandler): OutboundInterceptor msg.address: {}, writeHandler result failed: {}",
										msg.address(), ar.cause().toString());
								writeHandler.handle(Future.failedFuture(ar.cause()));
							}
						};
					}

					NonPublicAPI.Reflection.setField(msg, MessageImpl.class, "writeHandler", pendingWriteHandler);

//				log.debug("@@@<< AFTER: OutboundInterceptor msg.address: {}, writeHandler: {}", msg.address(),
//						 msg.writeHandler());
				}
			}
			// log.debug("###<< BEFORE: OutboundInterceptor ctx: {}", ctx.message().body());
			ctx.next();
			// log.debug("###<< AFTER: OutboundInterceptor ctx: {}", ctx.message().body());

//		io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: /192.168.99.1:18081
//		at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method)
//		at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717)
//		at io.netty.channel.socket.nio.NioSocketChannel.doFinishConnect(NioSocketChannel.java:327)
//		at io.netty.channel.nio.AbstractNioChannel$AbstractNioUnsafe.finishConnect(AbstractNioChannel.java:340)
//		at io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:665)
//		at io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:612)
//		at io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:529)
//		at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:491)
//		at io.netty.util.concurrent.SingleThreadEventExecutor$5.run(SingleThreadEventExecutor.java:905)
//		at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
//		at java.lang.Thread.run(Thread.java:748)
//	Caused by: java.net.ConnectException: Connection refused
		});
>>>>>>> branch 'master' of https://github.com/leotu/vertx-cluster-redis.git
	}

}
