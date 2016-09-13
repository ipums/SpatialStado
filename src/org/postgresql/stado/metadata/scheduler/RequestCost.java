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
package org.postgresql.stado.metadata.scheduler;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;

/**
 * 
 */
public class RequestCost implements Comparable {
    private static final XLogger logger = XLogger.getLogger(RequestCost.class);

    // two different RequestCost objects MUST NOT be equal
    // order number is last criteria in comparison
    private static long ORDER_NUMBER = 0;

    private long orderNumber;

    private long static_cost = 0;

    private long timeInMilliSeconds = 0;

    // This is the Sql.. Object
    private ILockCost sqlObject;

    public RequestCost(ILockCost sqlObject) {
        this.sqlObject = sqlObject;
        if (sqlObject != null) {
            static_cost = sqlObject.getCost();
        }
        timeInMilliSeconds = System.currentTimeMillis();
        orderNumber = ORDER_NUMBER++;
    }

    public ILockCost getSqlObject() {
        return sqlObject;
    }

    public long getDynamicCost() {
        // This is the cost that we get from checking the
        // locking
        return 0;
    }

    /**
     * 
     * @return
     */
    public long getTimeInQueue() {
        long milliSeconds = System.currentTimeMillis() - timeInMilliSeconds;
        return milliSeconds;
    }

    /**
     * TODO: make more sophiticated one
     * 
     * @return
     */
    public long setTotalCost() {
        return (sqlObject == null ? 0L : static_cost + getDynamicCost()
                - getTimeInQueue());
    }

    public boolean isLarge() {
        return static_cost > Props.XDB_LARGE_QUERY_COST;
    }

    /**
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Object o) {
        final String method = "compareTo";
        logger.entering(method, new Object[] {});
        try {

            // Same instance
            if (o == this) {
                return 0;
            }
            RequestCost other = (RequestCost) o;
            long diff = static_cost + timeInMilliSeconds - other.static_cost
                    - other.timeInMilliSeconds;
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            } else {
                return orderNumber < other.orderNumber ? 1 : -1;
            }

        } finally {
            logger.exiting(method);
        }
    }
}
