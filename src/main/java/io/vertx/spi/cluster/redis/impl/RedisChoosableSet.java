/*
 *  Copyright (c) 2011-2015 The original author or authors
 *  ------------------------------------------------------
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *       The Eclipse Public License is available at
 *       http://www.eclipse.org/legal/epl-v10.html
 *
 *       The Apache License v2.0 is available at
 *       http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.spi.cluster.redis.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.spi.cluster.ChoosableIterable;

/**
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
@SuppressWarnings("serial")
class RedisChoosableSet<T> implements ChoosableIterable<T>, Serializable {

	private final Set<T> ids;
	private volatile Iterator<T> iter;
	private final AtomicReference<T> current;

	public RedisChoosableSet(int initialSize, AtomicReference<T> current) {
		ids = new ConcurrentHashSet<>(initialSize);
		this.current = current;
	}

	public Set<T> getIds() {
		return ids;
	}

	public int size() {
		return ids.size();
	}

	public void addAll(Collection<? extends T> c) {
		ids.addAll(c);
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

	public void moveToCurrent() {
		T t = current.get();
		if (t == null || ids.isEmpty()) {
			return;
		}
		if (iter == null || !iter.hasNext()) {
			iter = ids.iterator();
		}
		current.set(null);
		while (iter.hasNext()) {
			T next = iter.next();
			if (next == t || next.equals(t)) {
				current.compareAndSet(null, next);
				break;
			}
		}
	}

	public T getCurrent() {
		return current.get();
	}

	public synchronized T choose() {
		if (!ids.isEmpty()) {
			if (iter == null || !iter.hasNext()) {
				iter = ids.iterator();
			}
			try {
				T t = iter.next();
				current.set(t);
				return t;
			} catch (NoSuchElementException e) {
				return null;
			}
		} else {
			return null;
		}
	}

}
