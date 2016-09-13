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

import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.Node;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.SelectTupleSpec;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;

/**
 * This class is responsible for gathering all the expressions in the projection
 * of a query.
 */
public class ProjectionListHandler extends ObjectDepthFirst {
    Command commandToExecute;

    /**
     * Class constructor.
     * @param commandToExecute
     */
    public ProjectionListHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }

    /**
     * /** f0 -> ( <STAR_> | TableName(prn) "." <STAR_> | (
     * SQLSimpleExpression(prn) ) [ SelectAliasSpec(prn) ] )
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(SelectTupleSpec n, Object argu) {
        /*
         * The argu- In this case conatins the Query Tree - The argument is set
         * by QueryTree handler -- Which actually takes care of all sql select
         * statments without
         */
        QueryTree aQueryTree = (QueryTree) argu;
        SqlExpression sqlExpr = new SqlExpression();

        switch (n.f0.which) {
        // /Please note that for -- 0 and 1 chocies I am not calling the
        // f0.accept--
        case 0:
            // Set the expression type to SQLEX_COLUMNLIST - which is to be
            // expanded
            sqlExpr.setExprType(SqlExpression.SQLEX_COLUMNLIST);
            // The expr string to *
            sqlExpr.setExprString("*");
            aQueryTree.getProjectionList().add(sqlExpr);

            break;
        /*
         * - In this case we need to do some special processing - where we
         * expand the "*" into all the columns --
         */

        case 1:
            // We create a new Sql Expression --
            // Set the expression type to SQLEX_COLUMNLIST - which is to be
            // expanded
            sqlExpr.setExprType(SqlExpression.SQLEX_COLUMNLIST);
            TableNameHandler tableNameHandler = new TableNameHandler(commandToExecute.getClientContext());
            n.f0.choice.accept(tableNameHandler, argu);
            sqlExpr.setExprString(tableNameHandler.getTableName() + ".*");
            aQueryTree.getProjectionList().add(sqlExpr);
            break;

        case 2:
            SQLExpressionHandler aSQLExpressionHandler = new SQLExpressionHandler(
                    commandToExecute);
            /*
             * This is a Handler Class which is delegated the responsibilty
             * of taking care of SQLExpressions
             */
            Node SQLExpression = ((NodeSequence) n.f0.choice).elementAt(0);
            Node SQLAlias = ((NodeSequence) n.f0.choice).elementAt(1);
            /*
             * We make call to accept - and after this point we should have the
             * following OuterAlias = Alias = columnName (if the expression is a
             * column)
             *
             */
            SQLExpression.accept(aSQLExpressionHandler, argu);
            AliasSpecHandler aAliasSpecHandler = new AliasSpecHandler();
            SQLAlias.accept(aAliasSpecHandler, argu);
            try {
                aSQLExpressionHandler.aroot.setAlias(aAliasSpecHandler
                        .getAliasName());
                aSQLExpressionHandler.aroot.setOuterAlias(aSQLExpressionHandler.aroot.getAlias());

            } catch (XDBServerException aliasEx) {
                if (aSQLExpressionHandler.aroot.getAlias() == null
                        && aSQLExpressionHandler.aroot.getAlias().equals("")) {
                    if (aSQLExpressionHandler.aroot.getExprType() == SqlExpression.SQLEX_COLUMN) {
                        aSQLExpressionHandler.aroot.setAlias(aSQLExpressionHandler.aroot.getColumn().columnAlias);
                        aSQLExpressionHandler.aroot.setOuterAlias(aSQLExpressionHandler.aroot.getColumn().columnAlias);
                    }
                }
            } catch (Exception ex) {
                throw new XDBServerException(
                        ErrorMessageRepository.CANNOT_FIND_ALIAS + " ("
                                + aSQLExpressionHandler.aroot.rebuildString()
                                + " )", ex,
                        ErrorMessageRepository.CANNOT_FIND_ALIAS_CODE);
            }
            /*
             * The handler has now completed its work and the aRoot - object
             * contains the SQLExpression We add this to the projectionList and
             * wait for the next call to TupleSpec
             */
            aQueryTree.getProjectionList().add(aSQLExpressionHandler.aroot);

            break;
        default:
            break;
        }
        return null;
    }
}
