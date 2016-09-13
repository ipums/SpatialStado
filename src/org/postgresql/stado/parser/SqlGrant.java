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
import java.util.HashSet;
import java.util.LinkedList;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.IMetaDataUpdate;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncGrant;
import org.postgresql.stado.metadata.SyncRevoke;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysUser;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.Grant;
import org.postgresql.stado.parser.core.syntaxtree.Grantee;
import org.postgresql.stado.parser.core.syntaxtree.Node;
import org.postgresql.stado.parser.core.syntaxtree.NodeListOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.Privilege;
import org.postgresql.stado.parser.core.syntaxtree.PrivilegeList;
import org.postgresql.stado.parser.core.syntaxtree.Revoke;
import org.postgresql.stado.parser.core.syntaxtree.TableListForGrant;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;
import org.postgresql.stado.parser.handler.IdentifierHandler;


public class SqlGrant extends ObjectDepthFirst implements IXDBSql, IPreparable {
    private static final XLogger logger = XLogger.getLogger(SqlGrant.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private boolean grant;

    private boolean hasSelect = false;

    private boolean hasInsert = false;

    private boolean hasUpdate = false;

    private boolean hasDelete = false;

    private boolean hasReferences = false;

    private boolean hasIndex = false;

    private boolean hasAlter = false;

    private Collection<String> tableList = null;

    private Collection<String> granteeList = new HashSet<String>();

    private Collection<SysTable> iTableList;

    private Collection<SysUser> iGranteeList = new HashSet<SysUser>();

    public SqlGrant(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <REVOKE_>
     * f1 -> PrivilegeList(prn)
     * f2 -> <ON_>
     * f3 -> [ <TABLE_> ]
     * f4 -> TableListForGrant(prn)
     * f5 -> <FROM_>
     * f6 -> GranteeList(prn)
     */
    @Override
    public Object visit(Revoke n, Object argu) {
        Object _ret = null;
        grant = false;
        n.f1.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
        return _ret;
    }

    /**
     * Grammar production:
     * f0 -> <GRANT_>
     * f1 -> PrivilegeList(prn)
     * f2 -> <ON_>
     * f3 -> [ <TABLE_> ]
     * f4 -> TableListForGrant(prn)
     * f5 -> <TO_>
     * f6 -> GranteeList(prn)
     */
    @Override
    public Object visit(Grant n, Object argu) {
        Object _ret = null;
        grant = true;
        n.f1.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
        return _ret;
    }

    /**
     * Grammar production:
     * f0 -> <ALL_>
     *       | Privilege(prn) ( "," Privilege(prn) )*
     */
    @Override
    public Object visit(PrivilegeList n, Object argu) {
        Object _ret = null;
        if (n.f0.which == 0) {
            hasSelect = true;
            hasInsert = true;
            hasUpdate = true;
            hasDelete = true;
            hasReferences = true;
            hasIndex = true;
            hasAlter = true;
        } else {
            n.f0.accept(this, argu);
        }
        return _ret;
    }

    /**
     * Grammar production: f0 -> <SELECT_> | <INSERT_> | <UPDATE_> | <DELETE_> |
     * <REFERENCES_> | <INDEX_> | <ALTER_>
     */
    @Override
    public Object visit(Privilege n, Object argu) {
        Object _ret = null;
        switch (n.f0.which) {
        case 0:
            hasSelect = true;
            break;
        case 1:
            hasInsert = true;
            break;
        case 2:
            hasUpdate = true;
            break;
        case 3:
            hasDelete = true;
            break;
        case 4:
            hasReferences = true;
            break;
        case 5:
            hasIndex = true;
            break;
        case 6:
            hasAlter = true;
            break;
        }
        return _ret;
    }

    /**
     * Grammar production:
     * f0 -> <PUBLIC_>
     *       | Identifier(prn)
     */
    @Override
    public Object visit(Grantee n, Object argu) {
        if (n.f0.which == 0) {
            granteeList.add(null);
        } else {
            IdentifierHandler ih = new IdentifierHandler();
            n.f0.choice.accept(ih, argu);
            granteeList.add(ih.getIdentifier());
        }
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <STAR_>
     *       | Identifier(prn) ( "," Identifier(prn) )*
     */
    @Override
    public Object visit(TableListForGrant n, Object argu) {
        if (n.f0.which == 1) {
            tableList = new LinkedList<String>();
            IdentifierHandler tnh = new IdentifierHandler();
            NodeSequence ns = (NodeSequence) n.f0.choice;
            ((Node) ns.nodes.get(0)).accept(tnh, argu);
            tableList.add(tnh.getIdentifier());
            NodeListOptional nlo = (NodeListOptional) ns.nodes.get(1);
            for (Object node : nlo.nodes) {
                ((Node) node).accept(tnh, argu);
                tableList.add(tnh.getIdentifier());
            }
        }
        return null;
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
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> empty = Collections.emptyList();
        return new LockSpecification<SysTable>(empty, empty);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return Collections.emptyList();
    }

    /**
     * @return Returns the hasAlter.
     */
    public boolean hasAlter() {
        return hasAlter;
    }

    /**
     * @return Returns the hasDelete.
     */
    public boolean hasDelete() {
        return hasDelete;
    }

    /**
     * @return Returns the hasIndex.
     */
    public boolean hasIndex() {
        return hasIndex;
    }

    /**
     * @return Returns the hasInsert.
     */
    public boolean hasInsert() {
        return hasInsert;
    }

    /**
     * @return Returns the hasReferences.
     */
    public boolean hasReferences() {
        return hasReferences;
    }

    /**
     * @return Returns the hasSelect.
     */
    public boolean hasSelect() {
        return hasSelect;
    }

    /**
     * @return Returns the hasUpdate.
     */
    public boolean hasUpdate() {
        return hasUpdate;
    }

    /**
     * @return Returns the iGranteeList.
     */
    public Collection<SysUser> getGranteeList() {
        return iGranteeList;
    }

    /**
     * @return Returns the iTableList.
     */
    public Collection<SysTable> getTableList() {
        return iTableList;
    }

    private boolean prepared = false;

    public boolean isPrepared() {
        return prepared;
    }

    public void prepare() throws Exception {
        if (!prepared) {
            iTableList = new HashSet<SysTable>();
            if (tableList != null) {
                for (String tableName : tableList) {
                    SysTable table = database.getSysTable(tableName);
                    if (table == null) {
                        throw new XDBServerException("Table not found: " + tableName);
                    }
                    if (table.getParentTable() != null) {
                        throw new XDBServerException(
                                "Permissions on child table \""
                                        + table.getTableName()
                                        + "\" can not be changed");
                    }
                    if (table.isTemporary()) {
                        throw new XDBServerException(
                                "Permissions on temporary table \""
                                        + table.getTableName()
                                        + "\" can not be changed");
                    }
                    if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA
                            && table.getOwner() != null
                            && table.getOwner() != client.getCurrentUser()) {
                        XDBSecurityException ex = new XDBSecurityException(
                                "You do not have privilege to change permissions on "
                                        + table.getTableName());
                        logger.throwing(ex);
                        throw ex;
                    }
                    iTableList.add(table);
                }
            } else {
                for (SysTable table : database.getSysTables()) {
                    if (table.getParentTable() != null) {
                        continue;
                    }
                    if (table.isTemporary()) {
                        continue;
                    }
                    if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA
                            && table.getOwner() != null
                            && table.getOwner() != client.getCurrentUser()) {
                        continue;
                    }
                    iTableList.add(table);
                }
            }
            for (String granteeName : granteeList) {
                iGranteeList.add(granteeName == null ? null : database.getSysUser(granteeName));
            }
        }
        prepared = true;
    }

    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] { engine });
        try {

            if (!isPrepared()) {
                prepare();
            }
            IMetaDataUpdate sync = grant ? new SyncGrant(this) : new SyncRevoke(this);
            MetaData meta = MetaData.getMetaData();
            meta.beginTransaction();
            try {
                sync.execute(client);
                meta.commitTransaction(sync);
            } catch (Exception e) {
                logger.catching(e);
                meta.rollbackTransaction();
                throw e;
            }
            return ExecutionResult
                    .createSuccessResult(ExecutionResult.COMMAND_GRANT);

        } finally {
            logger.exiting(method);
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
