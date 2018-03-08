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

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.spi.cluster.redis.NonPublicAPI.ClusteredEventBusAPI;
import io.vertx.spi.cluster.redis.NonPublicAPI.ClusteredEventBusAPI.ConnectionHolderAPI;
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMapSubs;

/**
 * Tryable to choose another server ID
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class PendingMessageProcessor {
	private static final Logger log = LoggerFactory.getLogger(PendingMessageProcessor.class);

	static private boolean debug = false;

	final static private String HA_ORIGINAL_SERVER_ID_KEY = "_HA_ORIGINAL_SERVER_ID";
	final static private String HA_RESEND_SERVER_ID_KEY = "_HA_RESEND_SERVER_ID";
	final static private String HA_RESEND_AGAIN_SERVER_ID_KEY = "_HA_RESEND_AGAIN_SERVER_ID";

	private final ClusteredEventBus eventBus;
	private final Context sendNoContext;
	private final ServerID clusterServerID; // self, local server
	private final RedisAsyncMultiMapSubs subs;
	private final RedisClusterManager clusterManager;
	private final ConcurrentMap<ServerID, Object> connections; // <ServerID, ConnectionHolder>

	public PendingMessageProcessor(Vertx vertx, RedisClusterManager clusterManager, ClusteredEventBus eventBus,
			RedisAsyncMultiMapSubs subs, ConcurrentMap<ServerID, Object> connections) {
		this.clusterManager = clusterManager;
		this.eventBus = eventBus;
		this.subs = subs;
		this.sendNoContext = vertx.getOrCreateContext();
		this.clusterServerID = ClusteredEventBusAPI.serverID(eventBus);
		this.connections = connections;
	}

	/**
	 * Async ?
	 */
	public void run(ServerID serverID, Queue<ClusteredMessage<?, ?>> pending) {
		Objects.requireNonNull(serverID, "serverID");
		Objects.requireNonNull(pending, "pending");
		ClusteredMessage<?, ?> message;
		while ((message = pending.poll()) != null) { // FIFO
			if (!clusterManager.isActive()) {
				if (debug) {
					log.debug("!isActive(), serverID: {}, pending.size: {}", serverID, pending.size());
				}
				pending.clear();
			} else if (!discard(message)) {
				resend(serverID, message);
			}
		}
	}

	private boolean discard(ClusteredMessage<?, ?> message) {
		if (!message.isSend()) { // skip Publish
			if (debug) {
				log.debug("discard(!message.isSend()): address: {}", message.address());
			}
			return true;
		}
		if (message.isFromWire()) { // skip readFromWire
			if (debug) {
				log.debug("discard(message.isFromWire()): address: {}", message.address());
			}
			return true;
		}

		String haOriginalServerId = message.headers().get(HA_ORIGINAL_SERVER_ID_KEY);
		String haResendServerId = message.headers().get(HA_RESEND_SERVER_ID_KEY);
		String haResendAgainServerId = message.headers().get(HA_RESEND_AGAIN_SERVER_ID_KEY);

		if (haResendAgainServerId != null) {
			if (debug) {
				log.debug(
						"discard(haResendAgainServerId != null): haResendAgainServerId: {}, haResendServerId: {}, haResendAgainServerId: {}, address: {}",
						haResendAgainServerId, haResendServerId, haResendAgainServerId, message.address());
			}
			return true; // had retry 2 times
		}
		if (haOriginalServerId != null && haResendServerId != null && haOriginalServerId.equals(haResendServerId)) {
			if (debug) {
				log.debug(
						"discard(haOriginalServerId.equals(haResendServerId)): haResendAgainServerId: {}, haResendServerId: {}, haResendAgainServerId: {}, address: {}",
						haResendAgainServerId, haResendServerId, haResendAgainServerId, message.address());
			}
			return true; // had retry original server
		}
		return false;
	}

	private void resend(ServerID failedServerID, ClusteredMessage<?, ?> message) {
		String address = message.address();
		Handler<AsyncResult<ChoosableIterable<ClusterNodeInfo>>> resultHandler = asyncResult -> {
			if (asyncResult.succeeded()) {
				ChoosableIterable<ClusterNodeInfo> serverIDs = asyncResult.result();
				if (serverIDs != null && !serverIDs.isEmpty()) {
					resendToSubs(failedServerID, message, serverIDs);
				}
			} else {
				log.warn("Failed to resend message, previous failed server id: " + failedServerID, asyncResult.cause());
			}
		};
		if (Vertx.currentContext() == null) {
			// Guarantees the order when there is no current context ?
			sendNoContext.runOnContext(v -> {
				subs.get(address, resultHandler);
			});
		} else {
			subs.get(address, resultHandler);
		}
	}

	/**
	 * Choose new one
	 * 
	 * @param originalServerID failed server
	 */
	private <T> void resendToSubs(ServerID failedServerID, ClusteredMessage<?, ?> message,
			ChoosableIterable<ClusterNodeInfo> subs) {
		ClusterNodeInfo ci = subs.choose();
		ServerID choosedServerID = null;
		ServerID pendingServerID = null;
		ServerID localServerID = null;
		while ((ci = subs.choose()) != null) {
			ServerID sid = ci.serverID;
			if (!sid.equals(failedServerID) && !sid.equals(clusterServerID)) {
				Object connHolder = connections.get(sid);
				if (connHolder == null) {
					choosedServerID = sid;
					break;
				} else {
					Queue<ClusteredMessage<?, ?>> pending = ConnectionHolderAPI.pending(connHolder);
					ServerID holderServerID = ConnectionHolderAPI.serverID(connHolder);
					if (!sid.equals(holderServerID)) {
						log.warn("(!sid.equals(holderServerID), sid: {}, holderServerID: {}", sid, holderServerID);
					}
					if (pending == null || pending.isEmpty()) {
						choosedServerID = sid;
						break;
					} else if (pending != null) {
						pendingServerID = sid;
						log.debug("skip pendingServerID: {}, pending.size: {}", pendingServerID, pending.size());
					}
				}
			} else if (sid.equals(clusterServerID)) {
				localServerID = sid;
			}
		}
		if (choosedServerID == null) {
			choosedServerID = pendingServerID != null ? pendingServerID : failedServerID;
			if (choosedServerID.equals(failedServerID) && localServerID != null) {
				// FIXME: change to localServerID ?
			}
			log.info("new one not found, return to failed or pending server: {}, address: {}", choosedServerID,
					message.address());
		} else {
			log.info("switch to new server: {}, previous failed server: {}, address: {}", choosedServerID, failedServerID,
					message.address());
		}

		String originalServerId = message.headers().get(HA_ORIGINAL_SERVER_ID_KEY);
		if (originalServerId == null) {
			message.headers().set(HA_ORIGINAL_SERVER_ID_KEY, failedServerID.toString());
			message.headers().set(HA_RESEND_SERVER_ID_KEY, choosedServerID.toString());
		} else {
			message.headers().set(HA_RESEND_SERVER_ID_KEY, failedServerID.toString());
			message.headers().set(HA_RESEND_AGAIN_SERVER_ID_KEY, choosedServerID.toString());
		}
		ClusteredEventBusAPI.sendRemote(eventBus, choosedServerID, message);
	}

}
