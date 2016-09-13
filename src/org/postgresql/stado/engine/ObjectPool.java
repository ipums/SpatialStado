/*****************************************************************************
 * Copyright (C) 2008 EnterpriseDB Corporation.
 * Copyright (C) 2011 Stado Global Development Group.
 * Copyright (c) 2016 Regents of the University of Minnesota
 *
 * This file is part of the Minnesota Population Center's Terra Populus project.
 * For copyright and licensing information, see the NOTICE and LICENSE files
 * in this project's top-level directory, and also online at:
 * https://github.com/mnpopcenter/stado
 *
 * This file is part of Stado.
 *
 * Stado is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stado is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stado.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can find Stado at http://www.stado.us
 *
 ****************************************************************************/
/*
 * 
 */
package org.postgresql.stado.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBServerException;


/**
 * Abstract class to pool objects, like Threads, JDBC connections, etc.
 * ObjectPool is assumed homogenous, that is when object is requested, no matter
 * which is returned. Class is thread-safe
 *
 *  
 */
public abstract class ObjectPool<T> {
    private static final XLogger logger = XLogger.getLogger(ObjectPool.class);

    public static final long DEFAULT_RELEASE_TIMEOUT = 600000L;// 10 min

    public static final long DEFAULT_GET_TIMEOUT = 60000L;// 1 min

    private int minSize;

    private int maxSize;

    private boolean destroyed;

    private long releaseTimeout = DEFAULT_RELEASE_TIMEOUT;

    private long getTimeout = DEFAULT_GET_TIMEOUT;

    private LinkedList<PoolEntry<T>> buffer;

    private HashSet<T> out;

    private Runnable cleanupAgent = null;

    /**
     *
     * @param minSize
     * @param maxSize
     */
    public ObjectPool(int minSize, int maxSize) {
        final String method = "ObjectPool";
        logger.entering(method, new Object[] { new Integer(minSize),
                new Integer(maxSize) });
        try {

            this.minSize = minSize;
            this.maxSize = maxSize;
            destroyed = false;
            buffer = new LinkedList<PoolEntry<T>>();
            out = new HashSet<T>();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void initBuffer() throws XDBServerException {
        final String method = "initBuffer";
        logger.entering(method);
        try {

            for (int i = buffer.size() + out.size(); i < minSize; i++) {
                try {
                    PoolEntry<T> poolEntry = new PoolEntry<T>();
                    poolEntry.timestamp = System.currentTimeMillis();
                    poolEntry.entry = createEntry();
                    buffer.addLast(poolEntry);
                } catch (XDBServerException e) {
                    logger.catching(e);
                    XDBServerException ex = new XDBServerException(
                            "Failed to initialize pool", e);
                    logger.throwing(ex);
                    throw ex;
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public synchronized T getObject() throws XDBServerException {
        final String method = "getObject";
        logger.entering(method);
        try {

            logger.debug(getClass().getName() + ".getObject(): Buffer has "
                    + buffer.size() + " objects, " + out.size()
                    + " is out there.");
            // long start = System.currentTimeMillis();
            // while (true)
            // {
            if (destroyed) {
                XDBServerException ex = new XDBServerException(
                        "Can not serve object - pool is destroyed");
                logger.throwing(ex);
                throw ex;
            }
            if (!buffer.isEmpty()) {
                PoolEntry<T> poolEntry = buffer.removeFirst();
                out.add(poolEntry.entry);
                packBuffer();
                return poolEntry.entry;
            }
            if (out.size() < maxSize) {
                T entry = createEntry();
                out.add(entry);
                return entry;
            }
            if (cleanupAgent != null) {
                new Thread(cleanupAgent).start();
            }

            /*
             * Always create an entry anyway. We have a problem
             * where objects may not always be being returned to the pool 
             * in a timely manner.
             * TODO: Fix this
             */
            T entry = createEntry();
            out.add(entry);
            return entry;
            /*
             * Commented out as part of retry wait loop until we can 
             * debug above issue.
             * try { long toWait =
             * start + getTimeout - System.currentTimeMillis(); if (toWait <= 0) {
             * XDBServerException ex = new XDBServerException( "Can not serve
             * object - timeout expired"); logger.throwing (ex); throw ex; }
             * wait(toWait); } catch (InterruptedException e) { }
             */
            // }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @return whether or not the object is "out"
     * @param entry
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public synchronized boolean isOut(Object entry) throws XDBServerException {
        final String method = "isOut";
        logger.entering(method, new Object[] { entry });
        try {
            boolean retValue = out.contains(entry);
            notifyAll();
            return retValue;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param entry
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public synchronized void releaseObject(T entry) throws XDBServerException {
        final String method = "releaseObject";
        logger.entering(method, new Object[] { entry });
        try {

            if (!out.remove(entry)) {
                XDBServerException ex = new XDBServerException(
                        "Attempt to release unknown object");
                logger.throwing(ex);
                throw ex;
            }
            if (destroyed) {
                destroyEntry(entry);
            } else {
                if (buffer.size() + out.size() <= maxSize) {
                    PoolEntry<T> poolEntry = new PoolEntry<T>();
                    poolEntry.timestamp = System.currentTimeMillis();
                    poolEntry.entry = entry;
                    buffer.addLast(poolEntry);
                    packBuffer();
                } else {
                    destroyEntry(entry);
                }
            }
            notifyAll();
            logger.debug(getClass().getName() + ".releaseObject(): Buffer has "
                    + buffer.size() + " objects, " + out.size()
                    + " is out there.");

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param entry
     * @param finalize
     * @throws org.postgresql.stado.exception.XDBServerException
     */


    public synchronized void destroyObject(T entry, boolean finalize)
            throws XDBServerException {
        final String method = "destroyObject";
        logger.entering(method, new Object[] { entry, new Boolean(finalize) });
        try {

            if (!out.remove(entry)) {
                XDBServerException ex = new XDBServerException(
                        "Attempt to release unknown object");
                logger.throwing(ex);
                throw ex;
            }
            if (finalize) {
                destroyEntry(entry);
            }
            if (buffer.size() + out.size() < minSize) {
                initBuffer();
            }
            notifyAll();
            logger.debug(getClass().getName() + ".releaseObject(): Buffer has "
                    + buffer.size() + " objects, " + out.size()
                    + " is out there.");

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     */
    public synchronized void destroy() {
        destroyed = true;
        while (!buffer.isEmpty()) {
            destroyEntry(buffer.removeFirst().entry);
        }
        for (T t : out) {
            destroyEntry(t);
        }
        out.clear();
        notifyAll();
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    protected abstract T createEntry() throws XDBServerException;

    /**
     *
     * @param entry
     */
    protected void destroyEntry(T entry) {
    }

    protected synchronized void packBuffer() {
        while (buffer.size() > 0 && buffer.size() + out.size() > minSize) {
            PoolEntry<T> poolEntry = buffer.getFirst();
            if (poolEntry.timestamp + releaseTimeout < System
                    .currentTimeMillis()) {
                destroyEntry(buffer.removeFirst().entry);
            } else {
                return;
            }
        }
    }

    /**
     * @return the timeout
     */
    public synchronized long getGetTimeout() {
        return getTimeout;
    }

    /**
     * @return the max size of the pool
     */
    public synchronized int getMaxSize() {
        return maxSize;
    }

    /**
     * @return the min size of the pool
     */
    public synchronized int getMinSize() {
        return minSize;
    }

    /**
     * @return the objects which have been taken from the pool
     */
    public synchronized Collection<T> getObjectsInUse() {
        return new ArrayList<T>(out);
    }

    /**
     * @return the release timeout
     */
    public synchronized long getReleaseTimeout() {
        return releaseTimeout;
    }

    /**
     * @param l the timeout
     */
    public synchronized void setGetTimeout(long l) {
        getTimeout = l;
    }

    /**
     * @param i the max size of the pool
     */
    public synchronized void setMaxSize(int i) {
        maxSize = i;
    }

    /**
     * @param i the min size of the pool
     */
    public synchronized void setMinSize(int i) {
        minSize = i;
    }

    /**
     * @param l the release timeout
     */
    public synchronized void setReleaseTimeout(long l) {
        releaseTimeout = l;
    }

    /**
     *
     * @param agent
     * @return
     */
    public synchronized Runnable setCleanupAgent(Runnable agent) {
        Runnable oldAgent = cleanupAgent;
        cleanupAgent = agent;
        return oldAgent;
    }

    protected class PoolEntry<E> {
        public long timestamp;

        public E entry;
    }
}
