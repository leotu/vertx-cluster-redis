package io.vertx.spi.cluster.redis;

import java.util.Objects;
import java.util.Queue;
import java.util.function.BiConsumer;

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
import io.vertx.spi.cluster.redis.impl.RedisAsyncMultiMapSubs;

/**
 * 
 * @author leo.tu.taipei@gmail.com, leo@syncpo.com
 */
public class PendingMessageProcessor implements BiConsumer<ServerID, Queue<ClusteredMessage<?, ?>>> {
	private static final Logger log = LoggerFactory.getLogger(PendingMessageProcessor.class);

	static private boolean debug = false;

	final static private String HA_ORIGINAL_SERVER_ID_KEY = "_HA_ORIGINAL_SERVER_ID";
	final static private String HA_RESEND_SERVER_ID_KEY = "_HA_RESEND_SERVER_ID";

	private final ClusteredEventBus eventBus;
	private final Context sendNoContext;
	private final ServerID selfServerID; // self
	private final RedisAsyncMultiMapSubs subs;

	public PendingMessageProcessor(Vertx vertx, ClusteredEventBus eventBus, RedisAsyncMultiMapSubs subs) {
		this.eventBus = eventBus;
		this.subs = subs;
		this.sendNoContext = vertx.getOrCreateContext();
		this.selfServerID = ClusteredEventBusAPI.getServerID(eventBus);
	}

	/**
	 * Async ?
	 */
	@Override
	public void accept(ServerID serverID, Queue<ClusteredMessage<?, ?>> pending) {
		Objects.requireNonNull(serverID, "serverID");
		Objects.requireNonNull(pending, "pending");
		ClusteredMessage<?, ?> message;
		while ((message = pending.poll()) != null) { // FIFO
			if (message.isSend() && !message.isFromWire() && !discard(message)) { // skip Publish & readFromWire
				resend(serverID, message);
			} else {
				if (debug) {
					log.debug("discard: {}, publish mode: {}, read from wire: {}", discard(message), !message.isSend(),
							message.isFromWire());
				}
			}
		}
		// pending.forEach(message -> { // FIFO
		// if (message.isSend() && !message.isFromWire()) { // skip Publish & readFromWire
		// resend(serverID, message);
		// }
		// });
	}

	private boolean discard(ClusteredMessage<?, ?> message) {
		String haOriginalServerId = message.headers().get(HA_ORIGINAL_SERVER_ID_KEY);
		String haResendServerId = message.headers().get(HA_RESEND_SERVER_ID_KEY);
		if (haOriginalServerId != null || haResendServerId != null) {
			if (debug) {
				log.debug("discard & don't resend: haOriginalServerId: {}, haResendServerId: {}", haOriginalServerId,
						haResendServerId);
			}
			return true;
		}
		return false;
	}

	private void resend(ServerID excludedServerID, ClusteredMessage<?, ?> message) {
		String address = message.address();
		Handler<AsyncResult<ChoosableIterable<ClusterNodeInfo>>> resultHandler = asyncResult -> {
			if (asyncResult.succeeded()) {
				ChoosableIterable<ClusterNodeInfo> serverIDs = asyncResult.result();
				if (serverIDs != null && !serverIDs.isEmpty()) {
					resendToSubs(excludedServerID, message, serverIDs);
				}
			} else {
				log.warn("Failed to resend message, original failed server id: " + excludedServerID, asyncResult.cause());
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

	private <T> void resendToSubs(ServerID excludedServerID, ClusteredMessage<?, ?> message,
			ChoosableIterable<ClusterNodeInfo> subs) {
		// Choose one
		ClusterNodeInfo ci = subs.choose();
		ServerID sid = null;
		while ((ci = subs.choose()) != null) {
			sid = ci.serverID;
			if (!sid.equals(excludedServerID) && !sid.equals(selfServerID)) {
				break;
			}
		}
		if (sid == null) {
			if (debug) {
				log.debug("(sid == null), reset to original serverID(excluded): {}", excludedServerID);
			}
			sid = excludedServerID;
		} else {
			if (debug) {
				log.debug("new serverID: {}, original serverID(excluded): {}", sid, excludedServerID);
			}
		}
		message.headers().set(HA_ORIGINAL_SERVER_ID_KEY, excludedServerID.toString());
		message.headers().set(HA_RESEND_SERVER_ID_KEY, sid.toString());
		ClusteredEventBusAPI.sendRemote(eventBus, sid, message);
	}

}
