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
package org.postgresql.stado.parser;

import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

import org.postgresql.stado.common.util.MetaDataUtil;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.DescribeTable;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;
import org.postgresql.stado.parser.handler.TableNameHandler;


public class SqlDescribeTable extends ObjectDepthFirst implements IXDBSql,
        IExecutable {

    private XDBSessionContext client;

    private String tableName;

    public SqlDescribeTable(XDBSessionContext client) {
        this.client = client;
    }

    @Override
    public Object visit(DescribeTable n, Object obj) {
        Object _ret = null;
        n.f0.accept(this, obj);
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f1.accept(aTableNameHandler, obj);
        tableName = aTableNameHandler.getTableName();
        return _ret;

    }

    /**
     * This will return the cost of executing this statement in time , milli
     * seconds
     */

    public long getCost() {
        return ILockCost.LOW_COST;
    }

    /**
     * This return the Lock Specification for the system
     * 
     * @param theMData
     * @return
     */
    public LockSpecification getLockSpecs() {
        LockSpecification aLspec = new LockSpecification(new Vector(),
                new Vector());
        return aLspec;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection getNodeList() {
        return Collections.EMPTY_LIST;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        if (this.client.getSysDatabase().isTableExists(tableName)) {
            return ExecutionResult.createResultSetResult(
                    ExecutionResult.COMMAND_SHOW, MetaDataUtil
                            .getDescribeTable(tableName, client));
        } else if (this.client.getSysDatabase().isViewExists(tableName)) {
            return ExecutionResult.createResultSetResult(
                    ExecutionResult.COMMAND_SHOW, MetaDataUtil.getDescribeView(
                            tableName, client));
        } else {
            XDBServerException ex = new XDBServerException("Object "
                    + tableName + " has not been found in database "
                    + client.getDBName());
            throw ex;

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }
}
