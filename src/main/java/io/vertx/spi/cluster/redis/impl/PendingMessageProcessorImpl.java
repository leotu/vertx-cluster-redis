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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.Factory.PendingMessageProcessor;
import io.vertx.spi.cluster.redis.impl.NonPublicAPI.ClusteredEventBusAPI;
import io.vertx.spi.cluster.redis.impl.NonPublicAPI.ClusteredEventBusAPI.ConnectionHolderAPI;

/**
 * Tryable to choose another server ID
 * 
 * @see io.vertx.core.eventbus.impl.clustered.ConnectionHolder#close
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class PendingMessageProcessorImpl implements PendingMessageProcessor {
	private static final Logger log = LoggerFactory.getLogger(PendingMessageProcessorImpl.class);

	private static boolean debug = false;

	private final Vertx vertx;
	private final ClusteredEventBus eventBus;

	@SuppressWarnings("unused")
	private final Context sendNoContext;
	private ServerID selfServerID; // self, local server

	private final AsyncMultiMap<String, ClusterNodeInfo> subs;
	private final ClusterManager clusterManager;
	private final ConcurrentMap<ServerID, Object> connections; // <ServerID, ConnectionHolder>
	private PendingRetryingStrategy strategy;

	public PendingMessageProcessorImpl(Vertx vertx, ClusterManager clusterManager, ClusteredEventBus eventBus,
			AsyncMultiMap<String, ClusterNodeInfo> subs, ConcurrentMap<ServerID, Object> connections) {
		this.vertx = vertx;
		this.clusterManager = clusterManager;
		this.eventBus = eventBus;
		this.subs = subs;
		this.sendNoContext = vertx.getOrCreateContext();
		this.connections = connections;
		this.strategy = new PendingRetryingStrategy(this.connections);
	}

	private void initIfNeeded() {
		if (this.selfServerID == null) {
			this.selfServerID = ClusteredEventBusAPI.serverID(this.eventBus);
			this.strategy.setSelfServerID(this.selfServerID);
		}
	}

	@Override
	public void run(Object failedServerID, Object connHolder) {
		initIfNeeded();

		Objects.requireNonNull(failedServerID, "failedServerID");
		Objects.requireNonNull(connHolder, "connHolder");

		Queue<ClusteredMessage<?, ?>> pending = ConnectionHolderAPI.pending(connHolder);
		ServerID holderServerID = ConnectionHolderAPI.serverID(connHolder);
		if (!failedServerID.equals(holderServerID)) {
			throw new RuntimeException("(!failedServerID.equals(holderServerID), serverID: " + failedServerID
					+ ", holderServerID: " + holderServerID);
		}
		if (pending != null && !pending.isEmpty()) {
			Future<Void> fu = process((ServerID) failedServerID, pending);
			fu.setHandler(ar -> {
				if (ar.failed()) {
					log.warn("failedServerID: {}, pendingProcessor error: {}", failedServerID, ar.cause().toString());
				}
			});
		} else if (debug) {
			log.debug("failedServerID: {}, pending.size: {}", failedServerID, pending == null ? "<null>" : pending.size());
		}
	}

	/**
	 * 
	 * @param serverID failedServerID
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })

	private Future<Void> process(ServerID failedServerID, Queue<ClusteredMessage<?, ?>> pending) {
		Objects.requireNonNull(failedServerID, "failedServerID");
		Objects.requireNonNull(pending, "pending");

		int pendingSize = pending.size();
		Queue<Future> runStatusFutures = new ArrayDeque<>(pendingSize);
		for (int i = 0; i < pendingSize; i++) {
			runStatusFutures.add(Future.future());
		}
		List<Future> completeFutures = new ArrayList<>(runStatusFutures);

		for (ClusteredMessage<?, ?> message = pending.poll(); message != null; message = pending.poll()) { // FIFO
			Future<Integer> runStatusFuture = runStatusFutures.poll();
			if (!clusterManager.isActive()) {
				if (debug) {
					log.debug("(!clusterManager.isActive())");
				}
				pending.clear();
				//
				runStatusFuture.complete(0);
				Future<Integer> f;
				while ((f = runStatusFutures.poll()) != null) {
					f.complete(0);
				}
				break;
			} else if (!discard(message)) {
				ClusteredMessage<?, ?> cmessage = message;
				resend(failedServerID, message).setHandler(ar -> {
					if (ar.failed()) {
						log.warn("Failed {} to retry {} message, address: {}, replyAddress:{}, isFromWire:{}, error: {}",
								cmessage.isSend() ? "send" : "publish", failedServerID, cmessage.address(), cmessage.replyAddress(),
								cmessage.isFromWire(), ar.cause().toString());
						runStatusFuture.fail(ar.cause());
					} else {
						if (!ar.result()) {
							if (debug) {
								log.debug(
										"failed {} to retry {} message, address: {}, replyAddress:{}, isFromWire:{}, no available serverID.",
										cmessage.isSend() ? "send" : "publish", failedServerID, cmessage.address(), cmessage.replyAddress(),
										cmessage.isFromWire());
							}
						}
						runStatusFuture.complete(ar.result() ? 1 : 0);
					}
				});
			} else {
				log.warn("Discard {} to retry {} message, address: {}, replyAddress:{}, isFromWire:{}",
						message.isSend() ? "send" : "publish", failedServerID, message.address(), message.replyAddress(),
						message.isFromWire());
				runStatusFuture.complete(-1);
			}
		} // while

		return complete(pendingSize, completeFutures, Future.future());
	}

	@SuppressWarnings("rawtypes")
	private Future complete(int pendingSize, List<Future> completeFutures, Future future) {
		CompositeFuture.join(completeFutures).setHandler(ar -> {
			// if (ar.failed()) { // All completed and at least one faileds
			// } else { // All succeeded
			// }
			int failedCounter = 0;
			int discardCounter = 0;
			int sureSendedCounter = 0;
			int notSendedCounter = 0;
			for (Future f : completeFutures) {
				if (f.failed()) {
					failedCounter++;
				} else {
					int status = (Integer) f.result();
					if (status == 1) {
						sureSendedCounter++;
					} else if (status == 0) {
						notSendedCounter++;
					} else {
						discardCounter++;
					}
				}
			}
			if (debug) {
				if (pendingSize != (failedCounter + discardCounter + sureSendedCounter + notSendedCounter)) {
					log.debug(
							"messages pendingSize: {}, discardCounter: {}, failedCounter: {}, sureSendedCounter: {}, notSendedCounter: {}",
							pendingSize, discardCounter, failedCounter, sureSendedCounter, notSendedCounter);
				}
			}
			future.complete();
		});
		return future;
	}

	private boolean discard(ClusteredMessage<?, ?> message) {
		if (!message.isSend()) { // skip Publish
			if (debug) {
				log.debug("(!message.isSend())");
			}
			return true;
		}
		if (message.isFromWire()) { // skip readFromWire
			if (debug) {
				log.debug("(message.isFromWire())");
			}
			return true;
		}
		return strategy.isDiscardMessage(message);
	}

	private Future<Boolean> resend(ServerID failedServerID, ClusteredMessage<?, ?> message) {
		String address = message.address();
		Future<Boolean> fu = Future.future();
		Handler<AsyncResult<ChoosableIterable<ClusterNodeInfo>>> resultHandler = asyncResult -> {
			if (asyncResult.succeeded()) {
				ChoosableIterable<ClusterNodeInfo> serverIDs = asyncResult.result();
				if (serverIDs != null && !serverIDs.isEmpty()) {
					resendToSubs(failedServerID, message, serverIDs).setHandler(ar -> {
						if (ar.failed()) {
							fu.fail(ar.cause());
						} else {
							fu.complete(true);
						}
					});
				} else {
					log.debug("No available serverID by address: {}, failed server id: {}, error: {}", address, failedServerID);
					fu.complete(false);
				}
			} else {
				// log.warn("Address: {}, failed to resend message, failed server id: {}, error: {}", address, failedServerID,
				// asyncResult.cause().toString());
				fu.fail(asyncResult.cause());
			}
		};
		subs.get(address, resultHandler);
		return fu;
	}

	/**
	 * vertx.executeBlocking(...)
	 */
	private Future<Void> resendToSubs(ServerID failedServerID, ClusteredMessage<?, ?> message,
			ChoosableIterable<ClusterNodeInfo> subs) {
		Future<Void> fu = Future.future();
		vertx.executeBlocking(future -> {
			Future<ServerID> selectOneFuture = strategy.next(failedServerID, message, subs);
			if (selectOneFuture.failed()) {
				future.fail(selectOneFuture.cause());
			} else {
				ServerID choosedServerID = selectOneFuture.result();
				ClusteredEventBusAPI.sendRemote(eventBus, choosedServerID, message);
				future.complete();
			}
		}, false, fu); // executed in parallel
		return fu;
	}

}
