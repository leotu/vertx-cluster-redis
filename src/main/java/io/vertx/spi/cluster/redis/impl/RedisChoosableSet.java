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

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ChoosableIterable;

/**
 * Implementor must be Thread-Safe
 * <p/>
 * <code>T<code> is ClusterNodeInfo
 * 
 * @see io.vertx.spi.cluster.hazelcast.impl.ChoosableSet
 * @see io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@SuppressWarnings("serial")
class RedisChoosableSet<T> implements ChoosableIterable<T>, Serializable {
	private static final Logger log = LoggerFactory.getLogger(RedisChoosableSet.class);

	private static boolean debug = false;

	private final Set<T> ids;
	private volatile Iterator<T> iter;
	private final AtomicReference<RedisChoosableSet<T>> currentRef;
	private T previous;

	public RedisChoosableSet(Collection<? extends T> values, AtomicReference<RedisChoosableSet<T>> currentRef) {
		Objects.requireNonNull(currentRef, "currentRef");
		this.currentRef = currentRef;
		this.ids = new ConcurrentHashSet<>(values != null ? values.size() : 0);
		if (values != null && !values.isEmpty()) {
			this.ids.addAll(values);
		}
	}

	public int size() {
		return ids.size();
	}

	public void add(T elem) {
		ids.add(elem);
	}

	public void remove(T elem) {
		ids.remove(elem);
	}

	public void merge(RedisChoosableSet<T> toMerge) {
		ids.addAll(toMerge.ids);
	}

	@Override
	public boolean isEmpty() {
		return ids.isEmpty();
	}

	public boolean contains(T elem) {
		return ids.contains(elem);
	}

	@Override
	public Iterator<T> iterator() {
		return ids.iterator();
	}

	private Iterator<T> initIteratorIfNeeded() {
		if (iter == null || !iter.hasNext()) {
			iter = ids.iterator(); // new one
		}
		return iter;
	}

	void moveToCurrent() {
		RedisChoosableSet<T> current = currentRef.get();
		if (current == null || ids.isEmpty()) {
			return;
		}
		if (current == this) {
			return;
		}
		initIteratorIfNeeded();
		boolean match = false;
		while (iter.hasNext()) {
			T next = iter.next();
			if (next == current.previous || next.equals(current.previous)) {
				match = true;
				previous = next;
				break;
			}
		}
		if (debug && !match) {
			log.debug("(moveToCurrent: !match), current: {}, previous: {}, ids.size: {}", current, previous, ids.size());
			previous = null;
		}
	}

	@Override
	public synchronized T choose() {
		if (ids.isEmpty()) {
			return null;
		}
		initIteratorIfNeeded();
		try {
			RedisChoosableSet<T> current = currentRef.get();
			T next = iter.next();
			if (ids.size() > 1 && (next == previous || next.equals(previous))) {
				if (debug) {
					log.debug("(ids.size() > 1 && (next == previous || next.equals(previous))), next: {}, ids.size: {}", next,
							ids.size());
				}
				initIteratorIfNeeded();
				next = iter.next();
			}
			previous = next;
			//
			if (currentRef.get() == null) {
				currentRef.compareAndSet(null, this);
			} else if (current != this) { // not same Object
				currentRef.compareAndSet(current, this); // not been modified
			}
			return next;
		} catch (NoSuchElementException e) {
			log.warn(e.toString());
			return null;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		RedisChoosableSet other = (RedisChoosableSet) obj;
		if (ids.size() != other.size()) {
			return false;
		}
		return ids.containsAll(other.ids) && other.ids.containsAll(ids);
	}

	@Override
	public String toString() {
		List<String> strs = ids.stream().map(e -> e.toString()).collect(Collectors.toList());
		return super.toString() + "{ids=" + strs + ", previous=" + previous + "}";
	}

}
