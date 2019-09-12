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
package io.vertx.spi.cluster.redis.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.redis.Factory.ExpirableAsync;
import io.vertx.spi.cluster.redis.Factory.NodeAttachListener;

/**
 * 
 * @see io.vertx.core.impl.HAManager
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
class RedisMapHaInfo extends RedisMap<String, String> implements NodeAttachListener {
  private static final Logger log = LoggerFactory.getLogger(RedisMapHaInfo.class);

  private final ClusterManager clusterManager;

  private NodeListener nodeListener;
  private int removedListeneId;
  private int expiredListenerId;
  private int createdListenerId;
  private int updatedListenerId;

  private int timeToLiveSeconds = 10;
  private int freshIntervalInSeconds = 2;

  private final TTLAgent ttlAgent;
  private final ExpirableAsync<String> asyncTTL;

  private final Map<String, String> refreshKv = new ConcurrentHashMap<>(); // Collections.singletonMap();

  public RedisMapHaInfo(Vertx vertx, ClusterManager clusterManager, RedissonClient redisson, String name,
      ExpirableAsync<String> asyncTTL) {
    super(vertx, redisson, name, null);
    this.clusterManager = clusterManager;
    this.asyncTTL = asyncTTL;
    this.ttlAgent = new TTLAgent();
    this.ttlAgent.setAction(this::refreshAction);
    this.attachListener();
  }

  /**
   * @see org.redisson.client.codec.StringCodec
   */
  @Override
  protected RMapCache<String, String> createMap(RedissonClient redisson, String name, Codec codec) {
    return redisson.getMapCache(name, new StringCodec());
  }

  public RMapCache<String, String> getMapAsync() {
    return (RMapCache<String, String>) super.map;
  }

  @Override
  public void attachListener(NodeListener nodeListener) {
    this.nodeListener = nodeListener;
  }

  public void detachListener() {
    RMapCache<String, String> mapAsync = getMapAsync();
    if (removedListeneId != 0) {
      mapAsync.removeListener(removedListeneId);
      removedListeneId = 0;
    }
    if (expiredListenerId != 0) {
      mapAsync.removeListener(expiredListenerId);
      expiredListenerId = 0;
    }
    if (createdListenerId != 0) {
      mapAsync.removeListener(createdListenerId);
      createdListenerId = 0;
    }
    if (updatedListenerId != 0) {
      mapAsync.removeListener(updatedListenerId);
      updatedListenerId = 0;
    }
    ttlAgent.stop();
  }

  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    log.warn("UnsupportedOperationException: putAll");
    throw new UnsupportedOperationException("putAll");
  }

  @Override
  public void replaceAll(BiFunction<? super String, ? super String, ? extends String> function) {
    log.warn("UnsupportedOperationException: replaceAll");
    throw new UnsupportedOperationException("replaceAll");
  }

  /**
   * Included self node ID notify
   * 
   * @see io.vertx.core.impl.HAManager#nodeAdded
   * @see io.vertx.core.impl.HAManager#nodeLeft
   */
  private void attachListener() {
    String selfNodeId = clusterManager.getNodeID();
    RMapCache<String, String> mapAsync = getMapAsync();
    if (removedListeneId == 0) {
      removedListeneId = mapAsync.addListener(new EntryRemovedListener<String, String>() {
        @Override
        public void onRemoved(EntryEvent<String, String> event) {
          String nodeId = event.getKey();
          if (nodeListener != null && clusterManager.getNodeID().equals(nodeId)) {
            ttlAgent.stop();
          }
          if (nodeListener != null) {
            log.debug("removed nodeLeft nodeId: {}, selfNodeId: {}", nodeId, selfNodeId);
            nodeListener.nodeLeft(nodeId);
          }
        }
      });
    }

    if (expiredListenerId == 0) {
      expiredListenerId = mapAsync.addListener(new EntryExpiredListener<String, String>() {
        @Override
        public void onExpired(EntryEvent<String, String> event) {
          String nodeId = event.getKey();
          if (nodeListener != null && clusterManager.getNodeID().equals(nodeId)) {
            ttlAgent.stop();
          }
          if (nodeListener != null) {
            log.debug("expired nodeLeft nodeId: {}, selfNodeId: {}", nodeId, selfNodeId);
            nodeListener.nodeLeft(nodeId);
          }
        }
      });
    }

    if (createdListenerId == 0) {
      createdListenerId = mapAsync.addListener(new EntryCreatedListener<String, String>() {
        @Override
        public void onCreated(EntryEvent<String, String> event) {
          String nodeId = event.getKey();
          if (nodeListener != null) {
            log.debug("created nodeAdded nodeId: {}, selfNodeId: {}", nodeId, selfNodeId);
            nodeListener.nodeAdded(nodeId);
          }
          if (nodeListener != null && clusterManager.getNodeID().equals(nodeId)) {
            ttlAgent.start();
          }
        }
      });
    }

    if (updatedListenerId == 0) {
      updatedListenerId = mapAsync.addListener(new EntryUpdatedListener<String, String>() {
        @Override
        public void onUpdated(EntryEvent<String, String> event) {
          String nodeId = event.getKey();
          if (nodeListener != null && !clusterManager.getNodeID().equals(nodeId)) {
            log.debug("updated nodeAdded nodeId: {}, selfNodeId: {}", nodeId, selfNodeId);
            nodeListener.nodeAdded(nodeId);
          }
          if (nodeListener != null && clusterManager.getNodeID().equals(nodeId)) {
            ttlAgent.start();
          }
        }
      });
    }
    ttlAgent.start();
  }

  // ===
  @Override
  public String put(String key, String value) {
    refreshKv.put(key, value);
    if (refreshKv.size() != 1) {
      log.warn("(refreshKv.size() != 1), key: {}, value: {}, refreshKv.size: {}", key, value, refreshKv.size());
    }
    return getMapAsync().put(key, value, timeToLiveSeconds, TimeUnit.SECONDS);
  }

  @Override
  public String remove(Object key) {
    refreshKv.remove(key);
    if (!refreshKv.isEmpty()) {
      log.warn("(!refreshKv.isEmpty()), key: {}, refreshKv.size: {}", key, refreshKv.size());
    }
    return super.remove(key);
  }

  @Override
  public String replace(String key, String value) {
    refreshKv.replace(key, value);
    return super.replace(key, value);
  }

  @Override
  public void clear() {
    refreshKv.clear();
    super.clear();
  }

  // ===
  /**
   * 
   */
  private void refreshAction(long counter) {
    String selfNodeId = clusterManager.getNodeID();
    if (refreshKv.size() > 1) {
      log.warn("(refreshKv.size() > 1), selfNodeId: {}, refreshKv.size: {}, counter: {}", selfNodeId, refreshKv.size(),
          counter);
    }
    refreshKv.forEach((k, v) -> {
      asyncTTL.refreshTTLIfPresent(k, timeToLiveSeconds, TimeUnit.SECONDS, ar -> {
        if (ar.failed()) {
          log.warn("Refresh TTL failed: selfNodeId: {}, counter: {}, k: {}, v: {}, error: {}", selfNodeId, counter, k, v,
              ar.cause().toString());
        } else {
          if (counter % 5 == 0) { // Each 5 times, 5=10/2
            asyncTTL.getTTL(k, ar2 -> {
              if (ar2.failed()) {
                log.warn("Get TTL failed: selfNodeId: {}, counter: {}, k: {}, v: {}, error: {}", selfNodeId, counter, k, v,
                    ar2.cause().toString());
                replaceAction(selfNodeId, counter, k, v);
              } else {
                long ttlMillis = ar2.result();
                long checkMillis = (freshIntervalInSeconds + 1) * 1000;
                if (ttlMillis < checkMillis) {
                  log.debug(
                      "Get TTL (ttlMillis < checkMillis): selfNodeId: {}, counter: {}, k: {}, ttlMillis: {}, checkMillis: {}",
                      selfNodeId, counter,
                      k, ttlMillis, checkMillis);
                  refreshAction(counter + 1); // must: "+1", don't reenter ==> (counter % 5 == 0)
                }
              }
            });
          }
        }
      });
    });
  }

  private void replaceAction(String selfNodeId, long counter, String k, String v) {
    getMapAsync().fastPutAsync(k, v, timeToLiveSeconds, TimeUnit.SECONDS)
        .whenComplete((newKey, err) -> {
          if (err != null) {
            log.warn("fastPutAsync: selfNodeId: {}, counter: {}, k: {}, v: {}, error: {}", selfNodeId, counter, k,
                v, err.toString());
          } else {
            if (newKey) {
              log.debug("fastPutAsync: selfNodeId: {}, counter: {}, k: {}, v: {}, newKey: {}", selfNodeId, counter, k,
                  v, newKey);
            }
          }
        });
  }

  private class TTLAgent {
    private Consumer<Long> action;
    private long timeId = -1;
    private AtomicLong counter = new AtomicLong(0);

    private void setAction(Consumer<Long> action) {
      this.action = action;
    }

    private void start() {
      if (isStarted()) {
        log.debug("timeId: {}, selfNodeId: {}", timeId, clusterManager.getNodeID());
        return;
      }
      timeId = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(freshIntervalInSeconds), id -> {
        action.accept(counter.incrementAndGet());
      });
    }

    private boolean isStarted() {
      return timeId != -1;
    }

    private void stop() {
      if (!isStarted()) {
        log.debug("timeId: {}, selfNodeId: {}", timeId, clusterManager.getNodeID());
        return;
      }
      long id = timeId;
      timeId = -1;
      vertx.cancelTimer(id);
    }
  }
}
