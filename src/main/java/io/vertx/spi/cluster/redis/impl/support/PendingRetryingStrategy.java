package io.vertx.spi.cluster.redis.impl.support;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;

//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.spi.cluster.redis.impl.support.NonPublicAPI.ClusteredEventBusAPI.ConnectionHolderAPI;

/**
 * Pending Message Retrying Strategy
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class PendingRetryingStrategy {
	private static final Logger log = LoggerFactory.getLogger(PendingRetryingStrategy.class);

	private static boolean debug = true;

	final static private String HA_RETRY_SERVER_ID_KEY = "__HA_RETRY_SERVER_ID";
	final static private String HA_RETRY_DISCARD_MESSAGE_KEY = "__HA_RETRY_DISCARD_MESSAGE";

	private ServerID selfServerID; // self, local server
	final private ConcurrentMap<ServerID, Object> connections; // <ServerID, ConnectionHolder>

	public PendingRetryingStrategy(ConcurrentMap<ServerID, Object> connections) {
		this.connections = connections;
	}

	public void setSelfServerID(ServerID selfServerID) {
		this.selfServerID = selfServerID;
	}

	/**
	 * Let it <code>vertx.executeBlocking(...)</code>
	 */
	@SuppressWarnings("deprecation")
	protected Future<ServerID> next(ServerID failedServerID, ClusteredMessage<?, ?> message,
			ChoosableIterable<ClusterNodeInfo> subs) {

		ClusterNodeInfo ci;
		ServerID choosedServerID = null;
		ServerID pendingServerID = null;
		ServerID localServerID = null;

		boolean repeatFailedServer = false;
		int repeatRetryServers = 0;
		boolean repeatLocalServer = false;
		boolean pendingServerServer = false;
		while ((ci = subs.choose()) != null) { // choose one
			ServerID next = ci.serverID;
			if (next.equals(failedServerID)) {
				if (debug) {
					log.debug("^^^ (next.equals(failedServerID)), next: {}", next);
				}
				if (repeatFailedServer) {
					break;
				}
				repeatFailedServer = true;
			} else if (next.equals(selfServerID)) {
				localServerID = next;
				if (debug) {
					log.debug("^^^ (next.equals(selfServerID)), next: {}", next);
				}
				if (repeatLocalServer) {
					break;
				}
				repeatLocalServer = true;
			} else if (inRetryServerList(message, next)) {
				if (debug) {
					log.debug("^^^ (inRetryServerList(message, next)), next: {}, retryServers: {}, repeatRetryServers:{}", next,
							getRetryServerList(message), repeatRetryServers);
				}
				if (repeatRetryServers > getRetryServerList(message).size()) {
					break;
				}
				repeatRetryServers++;
				continue;
			} else {
				Object connHolder = connections.get(next);
				if (connHolder == null) { // new open
					choosedServerID = next;
					break;
				} else { // exist
					Queue<ClusteredMessage<?, ?>> pending = ConnectionHolderAPI.pending(connHolder);
					if (pending == null || pending.isEmpty()) { // not pending node
						choosedServerID = next;
						break;
					} else {
						pendingServerID = next; // next is pending node
						if (debug) {
							log.debug("^^^ pendingServerID = next, next: {}", next);
						}
						if (pendingServerServer) {
							break;
						}
						pendingServerServer = true;
					}
				}
			}
		}

		int currentRetryTimes = retryTimes(message);
		if (choosedServerID == null) { // not found available server ID
			if (localServerID != null) {
				choosedServerID = localServerID;
				if (debug) {
					log.debug(
							"*** new one not found, switch to local server: {}, current retry times: {}, address: '{}', body: {}",
							choosedServerID, currentRetryTimes, message.address(), message.body());
				}
			} else if (pendingServerID != null) {
				choosedServerID = pendingServerID;
				if (debug) {
					log.debug(
							"*** new one not found, switch to pending server: {}, current retry times: {}, address: '{}', body: {}",
							choosedServerID, currentRetryTimes, message.address(), message.body());
				}
			} else {
				choosedServerID = failedServerID;
				if (debug) {
					log.debug(
							"*** new one not found, switch to failed server: {}, current retry times: {}, address: '{}', body: {}",
							choosedServerID, currentRetryTimes, message.address(), message.body());
				}
			}
		} else {
			if (debug) {
				log.debug(
						"*** new one found, switch to new server: {} and failed server: {}, current retry times: {}, address: '{}', body: {}",
						choosedServerID, failedServerID, currentRetryTimes, message.address(), message.body());
			}
		}

		//
		Future<ServerID> fu = Future.future();
		if (!inRetryServerList(message, choosedServerID)) {
			appendRetryServer(message, choosedServerID);
			fu.complete(choosedServerID);
		} else {
			markDiscardMessage(message, choosedServerID);
			fu.fail("No one can be choose, retry servers: " + getRetryServerList(message));
		}
		return fu;
	}

	// =====
	protected boolean isDiscardMessage(Message<?> message) {
		return message.headers().contains(HA_RETRY_DISCARD_MESSAGE_KEY);
	}

	private void markDiscardMessage(Message<?> message, ServerID choosedServerID) {
		message.headers().set(HA_RETRY_DISCARD_MESSAGE_KEY, choosedServerID.toString());
	}

	private void appendRetryServer(Message<?> message, ServerID choosedServerID) {
		MultiMap headers = message.headers();
		if (retryTimes(message) == 0) {
			headers.set(HA_RETRY_SERVER_ID_KEY, choosedServerID.toString());
		} else {
			headers.set(HA_RETRY_SERVER_ID_KEY, headers.get(HA_RETRY_SERVER_ID_KEY) + "," + choosedServerID.toString());
		}
		if (debug) {
			log.debug("appendRetryServer: {}", headers.get(HA_RETRY_SERVER_ID_KEY));
		}
	}

	private int retryTimes(Message<?> message) {
		return getRetryServerList(message).size();
	}

	private List<String> getRetryServerList(Message<?> message) {
		String serverList = message.headers().get(HA_RETRY_SERVER_ID_KEY);
		return Arrays.asList(serverList == null ? new String[0] : serverList.split(","));
	}

	private boolean inRetryServerList(Message<?> message, ServerID serverID) {
		return getRetryServerList(message).contains(serverID.toString());
	}
}
