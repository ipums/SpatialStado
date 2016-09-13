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
 * CoordinatorPools.java
 *
 *  
 */

package org.postgresql.stado.engine;

import java.util.HashMap;

import java.sql.Connection;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.*;


/**
 * 
 * 
 */
public class CoordinatorPools {
    private static final XLogger logger = XLogger
            .getLogger(CoordinatorPools.class);

    // private static final HashMap pools;
    private static final HashMap<String, JDBCPool> connections = new HashMap<String, JDBCPool>();
    
    //sray
    private static final HashMap<String, Integer> coordNodeIds = new HashMap<String, Integer>();
    
    /** Creates a new instance of CoordinatorPools */
    public CoordinatorPools() {

    }
    
    /**
     * Initializes pool
     * @param dbName 
     */
    public static void initPool(String dbName, int coordinatorNode) {
        synchronized (connections) {
            JDBCPool pool = connections.get(dbName);

            if (pool == null) {
                int minSize = Property.getInt(
                        "xdb.jdbc.coordinator.pool.initsize",
                        Props.XDB_DEFAULT_THREADS_POOL_INITSIZE);
                int maxSize = Property
                        .getInt(
                                "xdb.jdbc.coordinator.pool.maxsize",
                                Math
                                        .max(
                                                (int) Math
                                                        .round(Props.XDB_DEFAULT_THREADS_POOL_MAXSIZE * 0.8),
                                                minSize));
                Node node = MetaData.getMetaData().getNode(coordinatorNode);
                
                pool = new JDBCPool(node.getSJdbcDriver(), node
                        .getJdbcString(dbName), node.getJdbcUser(), node
                        .getJdbcPassword(), minSize, maxSize);
                // Force error if no connection available immediately,
                // as a result XDBSessionContext won't be stuck waiting for
                // connection
                // but allow concurrent XDBSessionContext to execute
                pool.setGetTimeout(-1);
                pool.setReleaseTimeout(Property.getInt("xdb.jdbc.pool.idle",
                        Props.XDB_DEFAULT_THREADS_POOL_IDLE));
                pool.initBuffer();
                connections.put(dbName, pool);
                coordNodeIds.put(dbName, coordinatorNode);//sray
            }
        }
    }

    /**
     * 
     * @param dbName 
     */

    public static void destoryPool(String dbName) {
        synchronized (connections) {
            JDBCPool pool = connections.remove(dbName);
            if (pool != null) {
                pool.destroy();
            }
        }
    }

    /**
     * get a connection for specified db
     * @param dbName 
     * @return 
     */
    public static Connection getConnection(String dbName) {
        final String method = "getConnection";
        logger.entering(method, new Object[] { dbName });
        try {

            JDBCPool pool = null;
            synchronized (connections) {
                pool = connections.get(dbName);
            }

            if (pool != null) {
                return pool.getConnection();
            } else {
                throw new XDBServerException("Coordinator Pool for database "
                        + dbName + " is not initialized");
            }

        } finally {
            logger.exiting(method);
        }
    }

    //sray
    public static int getCoordNodeId(String dbName) {
        final String method = "getCoordNodeId";
        logger.entering(method, new Object[] { dbName });
        try {

            Integer nodeId = null;
            synchronized (coordNodeIds) {
            	nodeId = coordNodeIds.get(dbName);
            }

            if (nodeId != null) {
                return nodeId;
            } else {
                throw new XDBServerException("Coordinator Pool for database "
                        + dbName + " is not initialized");
            }

        } finally {
            logger.exiting(method);
        }
    }
    
    
    /**
     * release a connection for db
     * @param dbName 
     * @param connection 
     * @throws java.lang.Exception 
     */
    public static void releaseConnection(String dbName, Connection connection)
            throws Exception {
        final String method = "releaseConnection";
        logger.entering(method, new Object[] { dbName, connection });
        try {

            JDBCPool pool = null;
            synchronized (connections) {
                pool = connections.get(dbName);
            }
            if (pool != null) {
                pool.releaseConnection(connection);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @param dbName
     * @param connection
     */
    public static void destroyConnection(String dbName, Connection connection) {
        final String method = "destroyConnection";
        logger.entering(method, new Object[] {});
        try {

            JDBCPool pool = null;
            synchronized (connections) {
                pool = connections.get(dbName);
            }
            if (pool != null) {
                pool.destroyConnection(connection);
            }

        } finally {
            logger.exiting(method);
        }
    }
}
