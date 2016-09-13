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
package org.postgresql.stado.parser.handler;

import java.util.Stack; 
import java.util.Hashtable;

import org.postgresql.stado.exception.NoParentTreeException;
import org.postgresql.stado.optimizer.QueryTree;


/**
 * This class acts as the stack holder to hold a QueryTree as the query passes
 * through Parser to Optimizer phases.
 */
public class QueryTreeTracker {
    Stack queryTreeStack = new Stack();

    static int numberOfTracker = 0;

    Hashtable currentNumAsked = new Hashtable();

    /**
     * Class constructor.
     */
    public QueryTreeTracker() {

    }

    /**
     * It adds the QueryTree represented by aQueryTree in the QueryTree stack.
     * @param aQueryTree A QueryTree object to register and add in the stack.
     */
    public void registerTree(QueryTree aQueryTree) {
        queryTreeStack.push(aQueryTree);
    }

    /**
     * It removes and returns the QueryTree from the top of QueryTree stack.
     * @return A QueryTree object.
     */
    public QueryTree deRegisterCurrentTree() {
        QueryTree aCheckTree = (QueryTree) queryTreeStack.pop();
        return aCheckTree;
    }

    // 
    /**
     * It retrieves the parent QueryTree object from the QueryTree stack.
     *
     * @param marker The marker object corresponds to a SqlColumn Expression which is being tracked.
     * @throws org.postgresql.stado.exception.NoParentTreeException If there is only one QueryTree on the stack.
     * @return A QueryTree object representing parent query tree.
     */
    public QueryTree getParentTree(Object marker) throws NoParentTreeException {
        int valueAsked = 0;
        // If the query stack is greater than 2 -- implying that there are
        // atleast
        // two trees in the system
        if (queryTreeStack.size() >= 2) {
            // Incase we have been vistied before we will get the next query
            // tree.
            Integer val = (Integer) currentNumAsked.get(marker);
            if (val != null) {

                if (val.intValue() > 0) {
                    // This is the value that we gave before
                    valueAsked = val.intValue();

                    currentNumAsked.remove(marker);
                    valueAsked = valueAsked - 1;
                    currentNumAsked.put(marker, new Integer(valueAsked));

                    // Make a sanity check
                    if (valueAsked < 0 || valueAsked > queryTreeStack.size()) {
                        throw new NoParentTreeException();
                    }
                } else {
                    throw new NoParentTreeException();
                }
            } else {
                // If this is the first time just store the current position of
                // the
                // query tree which we will give back.
                valueAsked = queryTreeStack.size() - 2;
                currentNumAsked.put(marker, new Integer(valueAsked));
            }

            return (QueryTree) queryTreeStack.elementAt(valueAsked);
        }

        // Incase there is only one QueryTree on the stack
        throw new NoParentTreeException();
    }

    /**
     * It returns the QueryTree object from the top of stack without removing 
     * it from stack.
     * @return A QueryTree object.
     */
    public QueryTree GetCurrentTree() {
        return (QueryTree) queryTreeStack.peek();
    }
}
