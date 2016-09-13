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
 * SqlAlterAddPrimary.java
 *
 *
 */

package org.postgresql.stado.parser;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.SyncAlterTableCheck;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.parser.core.syntaxtree.CheckDef;
import org.postgresql.stado.parser.core.syntaxtree.Constraint;
import org.postgresql.stado.parser.core.syntaxtree.SQLComplexExpression;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryConditionHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;

/**
 * Class for adding a PRIMARY KEY to a table
 *
 *
 */

public class SqlAlterAddCheck extends ObjectDepthFirst implements IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterAddCheck.class);

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private String constraintName = null;

    private String checkDef;

    private String[] commands;

    /**
     * @param table
     * @param client
     */
    public SqlAlterAddCheck(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
    }

    /**
     * Grammar production:
     * f0 -> <CONSTRAINT_>
     * f1 -> Identifier(prn)
     */
    @Override
    public Object visit(Constraint n, Object argu) {
        constraintName = (String) n.f1.accept(new IdentifierHandler(), argu);
        return null;
    }

    @Override
    public Object visit(SQLComplexExpression n, Object argu) {
        QueryConditionHandler qch = new QueryConditionHandler(new Command(
                Command.CREATE, this, new QueryTreeTracker(), client));
        n.accept(qch, argu);
        checkDef = qch.aRootCondition.getCondString();
        return null;
    }

    /**
     * Grammar production: f0 -> <CHECK_> f1 -> "(" f2 ->
     * skip_to_matching_brace(prn) f3 -> ")"
     */

    @Override
    public Object visit(CheckDef n, Object argu) {
        checkDef = n.f2.str;
        return null;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public String getCheckDef() {
        return checkDef;
    }

    public SqlAlterTable getParent() {
        return parent;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return commands != null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method, new Object[] {});
        try {

            parent.getTable().ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            if (constraintName == null) {
                constraintName = "CHK_" + parent.getTableName().toUpperCase();
            }
            String sql = "ADD CONSTRAINT " + IdentifierHandler.quote(constraintName) + " CHECK ("
                    + checkDef + ")";
            parent.addCommonCommand(sql);

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {
            if (commands != null && commands.length != 0) {
                engine.executeDDLOnMultipleNodes(commands,
                        parent.getNodeList(), new SyncAlterTableCheck(this),
                        client);
            }
            return null;

        } finally {
            logger.exiting(method);
        }
    }
}
