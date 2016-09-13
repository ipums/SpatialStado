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

import java.sql.Types;

import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.*;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;


/**
 * This class is responsible for handling function syntax. The information
 * regarding the functions are held in a SqlExpression object.
 *
 */
public class FunctionHandler extends ObjectDepthFirst {
    Command commandToExecute;

    /**
     * The SQL Expression which it fills up with information
     */
    SqlExpression aSqlExpression;

    /**
     *
     * @param aSqlExpression The SQLExpression which it is responsible to fill data with
     * @param commandToExecute
     */
    public FunctionHandler(SqlExpression aSqlExpression,
            Command commandToExecute) {
        this.aSqlExpression = aSqlExpression;
        this.commandToExecute = commandToExecute;
    }

    /**
     * This class is responsible to fill in the information regarding Case
     * Statement It fill this information in the SQL Expression
     */
    class CaseHandler extends ObjectDepthFirst {
        public String caseString;

        /**
         * Constructor - Which access the SQL Expression of the parent and then
         * sets the expression type to SQLEX_CASE
         */
        CaseHandler() {

            aSqlExpression.setExprType(SqlExpression.SQLEX_CASE);
        }

        /**
         * f0 -> <CASE_> "(" SQLSimpleExpression(prn) ")" ( <WHEN_>
         * SQLSimpleExpression(prn) <THEN_> SQLSimpleExpression(prn) )* [
         * <ELSE_> SQLSimpleExpression(prn) ] <END_> | <CASE_> ( <WHEN_>
         * SQLComplexExpression(prn) <THEN_> SQLSimpleExpression(prn) )* [
         * <ELSE_> SQLSimpleExpression(prn) ] <END_>
         *
         * @param n
         * @param argu
         * @return
         */
        @Override
        public Object visit(Func_Case n, Object argu) {
            switch (n.f0.which) {
            case 0:
                processSimpleCase(n.f0.choice, argu);
                break;
            case 1:
                processGeneralCase(n.f0.choice, argu);
                break;

            }
            return null;
        }

        /**
         * Simple Case Statement <CASE_> "(" SQLSimpleExpression(prn) ")" (
         * <WHEN_> SQLSimpleExpression(prn) <THEN_> SQLSimpleExpression(prn) )*
         * <ELSE_> SQLSimpleExpression(prn) <END_> The Function processes simple
         * Case Statement
         *
         * @param n
         * @param argu
         */
        private void processSimpleCase(Node n, Object argu) {
            NodeSequence caseSequence = (NodeSequence) n;
            Node f0 = caseSequence.elementAt(0);
            // Case Token
            f0.accept(this, argu);
            // Bracket Start
            // Node f1 = caseSequence.elementAt(1);
            // Main expression to check with
            Node f2 = caseSequence.elementAt(1);
            SQLExpressionHandler aSQLHandler = new SQLExpressionHandler(
                    commandToExecute);
            f2.accept(aSQLHandler, argu);
            SqlExpression mainExpression = aSQLHandler.aroot;
            NodeListOptional f3 = (NodeListOptional) caseSequence.elementAt(2);
            if (f3.present()) {
                for (Object node : f3.nodes) {
                    NodeSequence aSimpleClauseSequence = (NodeSequence) node;
                    Node WhenSqlExpressionNode = aSimpleClauseSequence
                    .elementAt(1);
                    SQLExpressionHandler whenhandler = new SQLExpressionHandler(
                            commandToExecute);
                    WhenSqlExpressionNode.accept(whenhandler, argu);
                    SqlExpression whenExpression = whenhandler.aroot;
                    // Create a Query Condition
                    QueryCondition aNewQueryCondition = new QueryCondition(
                            mainExpression, whenExpression, "=");
                    aNewQueryCondition.rebuildString();

                    Node ThenSqlExprNode = aSimpleClauseSequence.elementAt(3);
                    SQLExpressionHandler thenhandler = new SQLExpressionHandler(
                            commandToExecute);
                    ThenSqlExprNode.accept(thenhandler, argu);
                    SqlExpression thenExpression = thenhandler.aroot;
                    aSqlExpression.getCaseConstruct().addCase(aNewQueryCondition,
                            thenExpression);
                }
                NodeOptional f5 = (NodeOptional) caseSequence.elementAt(3);
                if (f5.present()) {
                    NodeSequence fsequence = (NodeSequence) f5.node;
                    Node f20 = fsequence.elementAt(1);
                    SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                            commandToExecute);
                    f20.accept(aSqlExpressionHandler, argu);
                    aSqlExpression.getCaseConstruct().setDefaultexpr(aSqlExpressionHandler.aroot);
                }

            }

        }

        /**
         * <CASE_> ( <WHEN_> SQLComplexExpression(prn) <THEN_>
         * SQLSimpleExpression(prn) )* [ <ELSE_> SQLSimpleExpression(prn) ]
         * <END_>
         *
         * @param n
         * @param argu
         */
        private void processGeneralCase(Node n, Object argu) {
            NodeSequence caseSequence = (NodeSequence) n;
            Node f0 = caseSequence.elementAt(0);
            f0.accept(this, argu);
            NodeListOptional f1 = (NodeListOptional) caseSequence.elementAt(1);
            if (f1.present()) {
                for (Object node : f1.nodes) {
                    NodeSequence ns = (NodeSequence) node;

                    Node n2 = (Node) ns.nodes.get(1);
                    QueryConditionHandler qc = new QueryConditionHandler(
                            commandToExecute);

                    n2.accept(qc, argu);
                    Node n4 = (Node) ns.nodes.get(3);
                    SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                            commandToExecute);
                    n4.accept(aSqlExpressionHandler, argu);
                    aSqlExpression.getCaseConstruct().addCase(qc.aRootCondition,
                            aSqlExpressionHandler.aroot);
                }
            }
            // f4 -> SQLSimpleExpression(prn)
            NodeOptional f2 = (NodeOptional) caseSequence.elementAt(2);
            if (f2.present()) {
                NodeSequence fsequence = (NodeSequence) f2.node;
                Node f20 = fsequence.elementAt(1);
                SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                        commandToExecute);
                f20.accept(aSqlExpressionHandler, argu);
                aSqlExpression.getCaseConstruct().setDefaultexpr(aSqlExpressionHandler.aroot);
            }
        }
    }// CaseHandler Ends

    /**
     * This function makes a case handler object and delegates the responsiblity
     * of this case statement to the case handler object
     *
     * f0 -> <CASE_> f1 -> ( <WHEN_> SQLComplexExpression(prn) <THEN_>
     * SQLSimpleExpression(prn) )* f2 -> <ELSE_> f3 -> SQLSimpleExpression(prn)
     * f4 -> <END_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Case n, Object argu) {
        CaseHandler aCaseHandler = new CaseHandler();
        n.accept(aCaseHandler, argu);
        return null;
    }

    /**
     * This function is responsible for collecting information for adding date
     * it directly fills the SQLExpression f0 -> <ADDDATE_> f1 -> "(" f2 ->
     * SQLArgument() f3 -> "," f4 -> SQLArgument() f5 -> ")"
     *
     * @param n
     * @param argu
     * @return
     *
     */
    @Override
    public Object visit(Func_AddDate n, Object argu) {
        setFunctionInfo(IFunctionID.ADDDATE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        /*
         * Commenting this code - We migth do some changes later.It tries to
         * convert date formats from one format to the default format
         *
         * //We can either have a date or a time stamp for this function -
         * Incase this is //a constant we should change it to the default
         * format. SqlExpression aDateExpression =
         * (SqlExpression)aSqlExpression.functionParams.elementAt(0);
         *
         */
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * This function f0 -> <ADDTIME_> f1 -> "(" f2 -> SQLArgument() f3 -> "," f4 ->
     * SQLArgument() f5 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_AddTime n, Object argu) {
        setFunctionInfo(IFunctionID.ADDTIME_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        /*
         * Commenting this code
         *
         * //We Time stamp for this function - Incase this is //a constant we
         * should change it to the default format. SqlExpression aDateExpression =
         * (SqlExpression)aSqlExpression.functionParams.elementAt(0);
         */

        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DATE_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Date n, Object argu) {
        setFunctionInfo(IFunctionID.DATE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DATEDIFF_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_DateDiff n, Object argu) {
        setFunctionInfo(IFunctionID.DATEDIFF_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DAY_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Day n, Object argu) {
        setFunctionInfo(IFunctionID.DAY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DAYNAME_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_DayName n, Object argu) {
        setFunctionInfo(IFunctionID.DAYNAME_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DAYOFMONTH_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_DayOfMonth n, Object argu) {
        setFunctionInfo(IFunctionID.DAYOFMONTH_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DAYOFWEEK_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_DayOfWeek n, Object argu) {
        setFunctionInfo(IFunctionID.DAYOFWEEK_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DAYOFYEAR_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_DayOfYear n, Object argu) {
        setFunctionInfo(IFunctionID.DAYOFYEAR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <MONTHNAME_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_MonthName n, Object argu) {
        setFunctionInfo(IFunctionID.MONTHNAME_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <SUBDATE_> f1 -> "(" f2 -> SQLArgument() f3 -> "," f4 ->
     * SQLArgument() f5 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_SubDate n, Object argu) {
        setFunctionInfo(IFunctionID.SUBDATE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> "SUBTIME" f1 -> "(" f2 -> SQLArgument() f3 -> "," f4 ->
     * SQLArgument() f5 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_SubTime n, Object argu) {
        setFunctionInfo(IFunctionID.SUBTIME_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <TIME_> f1 -> "(" f2 -> SQLArgument() f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Time n, Object argu) {
        setFunctionInfo(IFunctionID.TIME_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> ( <CURDATE_> | <DATE_> ) f1 -> <PARENTHESIS>
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_CurrentDate n, Object argu) {
        setFunctionInfo(
                IFunctionID.CURRENTDATE_ID,
                IdentifierHandler.normalizeCase(((NodeToken) n.f0.choice).tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <CURRENTDATE_>
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_PgCurrentDate n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTDATE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> ( <CURTIME_> | <TIME_> ) f1 -> <PARENTHESIS>
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_CurrentTime n, Object argu) {
        setFunctionInfo(
                IFunctionID.CURRENTTIME_ID,
                IdentifierHandler.normalizeCase(((NodeToken) n.f0.choice).tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <CURRENT_TIME_> f1 -> [ "(" SQLArgument(prn)
     * ")" ]
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_PgCurrentTime n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTTIME_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <LOCALTIME_> f1 -> [ "(" SQLArgument(prn) ")" ]
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_LocalTime n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTTIME_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <CURRENT_TIMESTAMP_> f1 -> [ "("
     * SQLArgument(prn) ")" ]
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_PgCurrentTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTTIMESTAMP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <LOCALTIMESTAMP_> f1 -> [ "(" SQLArgument(prn)
     * ")" ]
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_LocalTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTTIMESTAMP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * f0 -> <YEAR_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Year n, Object argu) {
        setFunctionInfo(IFunctionID.YEAR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <MONTH_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Month n, Object argu) {
        setFunctionInfo(IFunctionID.MONTH_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <MINUTE_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Minute n, Object argu) {
        setFunctionInfo(IFunctionID.MINUTE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <SECOND_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Second n, Object argu) {
        setFunctionInfo(IFunctionID.SECOND_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <HOUR_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Hour n, Object argu) {
        setFunctionInfo(IFunctionID.HOUR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <TIMESTAMP_>
     * f1 -> <PARENTHESIS_START_>
     * f2 -> [ SQLArgument(prn) ]
     * f3 -> <PARENTHESIS_CLOSE_>
     */
    @Override
    public Object visit(Func_TimeStamp n, Object argu) {
        if (n.f2.present()) {
            setFunctionInfo(IFunctionID.TIMESTAMP_ID,
                    IdentifierHandler.normalizeCase(n.f0.tokenImage));
            n.f2.accept(this, argu);
        } else {
            setFunctionInfo(IFunctionID.CURRENTTIMESTAMP_ID,
                    IdentifierHandler.normalizeCase(n.f0.tokenImage));
        }
        return null;
    }

    /**
     * f0 -> <WEEKOFYEAR_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_WeekOfYear n, Object argu) {
        setFunctionInfo(IFunctionID.WEEKOFYEAR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <NOW_> f1 -> <PARENTHESIS>
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Now n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTTIMESTAMP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * f0 -> SQLSimpleExpression(prn)
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(SQLArgument n, Object argu) {
        SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                commandToExecute);
        n.f0.accept(aSqlExpressionHandler, argu);
        aSqlExpression.getFunctionParams().add(aSqlExpressionHandler.aroot);
        return null;
    }

    // Arthematic Functions Start Here

    /**
     * f0 -> <ABS_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Abs n, Object argu) {
        setFunctionInfo(IFunctionID.ABS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <CEIL_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Ceil n, Object argu) {
        setFunctionInfo(IFunctionID.CEIL_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <CEILING_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Ceiling n, Object argu) {
        setFunctionInfo(IFunctionID.CEIL_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <EXP_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Exp n, Object argu) {
        setFunctionInfo(IFunctionID.EXP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <FLOOR_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Floor n, Object argu) {
        setFunctionInfo(IFunctionID.FLOOR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LN_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_LN n, Object argu) {
        setFunctionInfo(IFunctionID.LN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LOG_> f1 -> "(" f2 -> SQLArgument(prn) f5 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Log n, Object argu) {
        setFunctionInfo(IFunctionID.LOG_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <PI_> f1 -> "(" f2 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_PI n, Object argu) {
        setFunctionInfo(IFunctionID.PI_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * f0 -> <USER_> f1 -> [ "(" ")" ]
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_User n, Object argu) {
        setFunctionInfo(
                IFunctionID.CURRENTUSER_ID,
                IdentifierHandler.normalizeCase(((NodeToken) n.f0.choice).tokenImage));
        aSqlExpression.setConstantValue("'"
                + commandToExecute.getClientContext().getCurrentUser().getName().replaceAll(
                        "'", "''") + "'");
        return null;
    }

    /**
     * f0 -> <POWER_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument (prn) f5 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Power n, Object argu) {
        setFunctionInfo(IFunctionID.POWER_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <SIGN_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_Sign n, Object argu) {
        setFunctionInfo(IFunctionID.SIGN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <ASIN_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Func_TAsin n, Object argu) {
        setFunctionInfo(IFunctionID.ASIN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <ATAN2_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_TATan2 n, Object argu) {
        setFunctionInfo(IFunctionID.ATAN2_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <ATN2_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_TATn2 n, Object argu) {
        setFunctionInfo(IFunctionID.ATN2_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <ATAN_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_TAtan n, Object argu) {
        setFunctionInfo(IFunctionID.ATAN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <COS_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_TCos n, Object argu) {
        setFunctionInfo(IFunctionID.COS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <ACOS_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_TACos n, Object argu) {
        setFunctionInfo(IFunctionID.ACOS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LOG10_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Log10 n, Object argu) {
        setFunctionInfo(IFunctionID.LOG10_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> ( <MOD_> | <MODULE_> ) f1 -> "(" f2 -> SQLArgument(prn) f3 -> ","
     * f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Mod n, Object argu) {
        setFunctionInfo(IFunctionID.MOD_ID,
                IdentifierHandler.normalizeCase(((NodeToken) n.f0.choice).tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <SQRT_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Sqrt n, Object argu) {
        setFunctionInfo(IFunctionID.SQRT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <COSH_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_TCosh n, Object argu) {
        setFunctionInfo(IFunctionID.COSH_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <FLOAT_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Float n, Object argu) {
        setFunctionInfo(IFunctionID.FLOAT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <GREATEST_> f1 -> "(" f2 -> FunctionArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Greatest n, Object argu) {
        setFunctionInfo(IFunctionID.GREATEST_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <COALESCE_> f1 -> "("  f2 -> FunctionArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Coalesce n, Object argu) {
        setFunctionInfo(IFunctionID.COALESCE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LEAST_> f1 -> "(" f2 -> FunctionArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Least n, Object argu) {
        setFunctionInfo(IFunctionID.LEAST_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <COT_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_TCot n, Object argu) {
        setFunctionInfo(IFunctionID.COT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DEGREE_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_TDegree n, Object argu) {
        setFunctionInfo(IFunctionID.DEGREES_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <RADIANS_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Radians n, Object argu) {
        setFunctionInfo(IFunctionID.RADIANS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <SIN_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Sin n, Object argu) {
        setFunctionInfo(IFunctionID.SIN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <TAN_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Tan n, Object argu) {
        setFunctionInfo(IFunctionID.TAN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <ROUND_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> [ "," SQLArgument(prn) ]
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_Round n, Object argu) {
        setFunctionInfo(IFunctionID.ROUND_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <ASCII_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Ascii n, Object argu) {
        setFunctionInfo(IFunctionID.ASCII_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> ( <INDEX_> | <INSTR_> ) f1 -> "(" f2 -> SQLArgument(prn) f3 -> ","
     * f4 -> SQLArgument(prn) f5 -> [ "," SQLArgument(prn) ] f6 -> [ ","
     * SQLArgument(prn) ] f7 -> ")"
     */
    @Override
    public Object visit(Func_Index n, Object argu) {
        if (n.f0.which == 0) {
            setFunctionInfo(IFunctionID.INDEX_ID,
                    IdentifierHandler.normalizeCase("index"));
        } else {
            setFunctionInfo(IFunctionID.INSTR_ID,
                    IdentifierHandler.normalizeCase("instr"));
        }
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        n.f6.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LEFT_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> , f4 ->
     * SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Left n, Object argu) {
        setFunctionInfo(IFunctionID.LEFT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <RIGHT_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> , f4 ->
     * SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Right n, Object argu) {
        setFunctionInfo(IFunctionID.RIGHT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LENGTH_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Length n, Object argu) {
        setFunctionInfo(IFunctionID.LENGTH_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LOWER_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Lower n, Object argu) {
        setFunctionInfo(IFunctionID.LOWER_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <LPAD_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> [ "," SQLArgument(prn) ]
     * f6 -> ")"
     */
    @Override
    public Object visit(Func_Lpad n, Object argu) {
        setFunctionInfo(IFunctionID.LPAD_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LTRIM_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> [ ","
     * SQLArgument(prn) ] f4 -> ")"
     */

    @Override
    public Object visit(Func_Ltrim n, Object argu) {
        setFunctionInfo(IFunctionID.LTRIM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <REPLACE_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> [ "," SQLArgument(prn) ] f6 -> ")"
     */
    @Override
    public Object visit(Func_Replace n, Object argu) {
        setFunctionInfo(IFunctionID.REPLACE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <TRIM_> f1 -> "(" f2 -> ( <BOTH> | <LEADING> |
     * <TRAILING> ) f3 -> [ SQLArgument(prn) ] f4 -> <FROM_> f5 ->
     * SQLArgument(prn) f6 -> ")"
     */
    @Override
    public Object visit(Func_Trim n, Object argu) {
        setFunctionInfo(IFunctionID.TRIM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        SqlExpression aExp = new SqlExpression();
        NodeToken theToken = (NodeToken) n.f2.choice;
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(theToken.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp);

        n.f3.accept(this, argu);

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setArgSeparator(" ");
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp1.setConstantValue(n.f4.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp1);

        n.f5.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <RPAD_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> [ "," SQLArgument(prn) ]
     * f6 -> ")"
     */
    @Override
    public Object visit(Func_Rpad n, Object argu) {
        setFunctionInfo(IFunctionID.RPAD_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <RTRIM_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> [ "," SQLArgument(prn) ]
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_Rtrim n, Object argu) {
        setFunctionInfo(IFunctionID.RTRIM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <SUBSTR_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * "," f4 -> SQLArgument(prn) f5 -> [ "," SQLArgument(prn) ] f6 -> ")"
     */
    @Override
    public Object visit(Func_SubStr n, Object argu) {
        setFunctionInfo(IFunctionID.SUBSTR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <DATABASE_> f1 -> <PARENTHESIS>
     */
    @Override
    public Object visit(Func_Database n, Object argu) {
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        aSqlExpression.setConstantValue("'"
                + commandToExecute.getClientContext().getDBName() + "'");
        aSqlExpression.setAlias(n.f0.tokenImage);
        return null;
    }

    /**
     * f0 -> <VERSION_>
     */
    @Override
    public Object visit(Func_Version n, Object argu) {
        setFunctionInfo(
                IFunctionID.VERSION_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * f0 -> <VALUE_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> ( "," SQLArgument(prn) )* f6 -> ")"
     */
    @Override
    public Object visit(Func_Value n, Object argu) {
        setFunctionInfo(IFunctionID.VALUE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <AVERAGE_> f1 -> "(" f2 -> [ "DISTINCT" ] f3 -> SQLArgument(prn) f4 -> ")"
     */
    @Override
    public Object visit(Func_Avg n, Object argu) {
        setFunctionInfo(IFunctionID.AVG_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }

    class CountHandle extends ObjectDepthFirst {

        /**
         * Grammar production:
         * f0 -> <COUNT_>
         * f1 -> "("
         * f2 -> ( <STAR_> | [ <DISTINCT_> | <ALL_> ] SQLArgument(prn) )
         * f3 -> ")"
         */
        @Override
        public Object visit(Func_Count n, Object argu) {
            if (n.f2.which == 0) {
                setFunctionInfo(IFunctionID.COUNT_STAR_ID,
                        IdentifierHandler.normalizeCase("COUNT(*)"));
                return null;
            } else {
                NodeSequence ns = (NodeSequence) n.f2.choice;
                NodeOptional no = (NodeOptional) ns.nodes.get(0);
                if (no.present()) {
                    NodeChoice nc = (NodeChoice) no.node;
                    if (nc.which == 0) {
                        aSqlExpression.setDistinctGroupFunction(true);
                    } else {
                        aSqlExpression.setAllCountGroupFunction(true);
                    }
                }
                setFunctionInfo(IFunctionID.COUNT_ID,
                        IdentifierHandler.normalizeCase("COUNT"));
                ((Node) ns.nodes.get(1)).accept(this, argu);
            }
            return null;
        }

        /**
         * f0 -> SQLSimpleExpression(prn)
         */
        @Override
        public Object visit(SQLArgument n, Object argu) {
            SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                    commandToExecute);
            n.f0.accept(aSqlExpressionHandler, argu);
            aSqlExpression.getFunctionParams().add(aSqlExpressionHandler.aroot);
            return null;
        }
    }

    /**
     * Grammar production:
     * f0 -> <COUNT_> "(" <STAR_> ")"
     *       | <COUNT_> "(" [ "DISTINCT" ] SQLArgument(prn) ")"
     *       | <COUNT_> "(" [ "ALL" ] SQLArgument(prn) ")"
     */
    @Override
    public Object visit(Func_Count n, Object argu) {
        n.accept(new CountHandle(), argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <MAX_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_Max n, Object argu) {
        setFunctionInfo(IFunctionID.MAX_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <MIN_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_Min n, Object argu) {
        setFunctionInfo(IFunctionID.MIN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> ( <STDDEV_> | <STDDEV_POP_> | <STDDEV_SAMP_> )
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_Stdev n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.STDEV_ID;
            break;
        case 1:
            funcID = IFunctionID.STDEVPOP_ID;
            break;
        case 2:
            funcID = IFunctionID.STDEVSAMP_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));

        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> ( <VARIANCE_> | <VARIANCE_POP_> | <VARIANCE_SAMP_> )
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_Variance n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.VARIANCE_ID;
            break;
        case 1:
        case 3:
            funcID = IFunctionID.VARIANCEPOP_ID;
            break;
        case 2:
        case 4:
            funcID = IFunctionID.VARIANCESAMP_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }

    /*
     * f0 -> <SUM_> f1 -> "(" f2 -> [ "DISTINCT" ] f3 -> SQLArgument(prn) f4 ->
     * ")"
     */
    /**
     * f0 -> <SUM_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Sum n, Object argu) {
        setFunctionInfo(IFunctionID.SUM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <UPPER_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_Upper n, Object argu) {
        setFunctionInfo(IFunctionID.UPPER_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    // Arthematic Funtion end Here
    @Override
    public Object visit(NodeToken n, Object argu) {
        aSqlExpression.setExprString(aSqlExpression.getExprString() + n.tokenImage);
        return null;
    }

    /**
     * f0 -> <SOUNDEX_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_SoundEx n, Object argu) {
        setFunctionInfo(IFunctionID.SOUNDEX_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <INITCAP_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_InitCap n, Object argu) {
        setFunctionInfo(IFunctionID.INITCAP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <LFILL_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> [ "," SQLArgument(prn) ] f6 -> ")"
     */
    @Override
    public Object visit(Func_LFill n, Object argu) {
        setFunctionInfo(IFunctionID.LFILL_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <MAPCHAR_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> [ "," <INT_LITERAL> ]
     * f4 -> [ "," Identifier(prn) ]
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_MapChar n, Object argu) {
        setFunctionInfo(IFunctionID.MAPCHAR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        if (n.f3.present()) {
            NodeToken theToken = (NodeToken) ((NodeSequence) n.f3.node)
            .elementAt(1);
            SqlExpression aExp = new SqlExpression();
            aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp.setConstantValue(theToken.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp);

        }
        if (n.f4.present()) {
            SqlExpression aExp = new SqlExpression();
            aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp.setConstantValue((String) ((NodeSequence) n.f4.node)
                    .elementAt(1).accept(new IdentifierHandler(), argu));
            aSqlExpression.getFunctionParams().add(aExp);
        }
        return null;
    }

    /**
     * f0 -> <NUM_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_NUM n, Object argu) {
        setFunctionInfo(IFunctionID.NUM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <CONCAT_WORD> f1 -> "(" f2 -> SQLArgument(prn) f3 -> "," f4 ->
     * SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Concat n, Object argu) {
        setFunctionInfo(IFunctionID.CONCAT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <IDENTIFIER_NAME> f1 -> "(" f2 -> [
     * FunctionArgument(prn) ] f3 -> ")"
     */
    @Override
    public Object visit(Func_Custom n, Object argu) {
        setFunctionInfo(IFunctionID.CUSTOM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <TRUNC_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> [
     * "," SQLArgument(prn) ] f4 -> ")"
     *
     */
    @Override
    public Object visit(Func_Trunc n, Object argu) {
        setFunctionInfo(IFunctionID.TRUNC_ID, IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <CAST_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * <AS_> f4 -> ( types() | <NULL_> ) f5 -> ")"
     */
    @Override
    public Object visit(Func_Cast n, Object argu) {
        n.f2.accept(this, argu);
        setFunctionInfo(IFunctionID.CAST_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));

        DataTypeHandler typeHandler = new DataTypeHandler();
        if (n.f4.which == 0) {
            n.f4.accept(typeHandler, argu);
        } else {
            typeHandler = new DataTypeHandler(Types.NULL, 0, 0, 0);
        }
        this.aSqlExpression.setExpTypeOfCast(typeHandler);
        return null;
    }

    /**
     * Grammar production: f0 -> <TIMEOFDAY_> f1 -> <PARENTHESIS>
     */
    @Override
    public Object visit(Func_TimeOfDay n, Object argu) {
        setFunctionInfo(IFunctionID.TIMEOFDAY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <ISFINITE_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * ")"
     */
    @Override
    public Object visit(Func_IsFinite n, Object argu) {
        setFunctionInfo(IFunctionID.ISFINITE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <DATETRUNC_> f1 -> "(" f2 -> SQLArgument(prn)
     * f3 -> "," f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_DateTrunc n, Object argu) {
        setFunctionInfo(IFunctionID.DATETRUNC_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <DATEPART_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * "," f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_DatePart n, Object argu) {
        setFunctionInfo(IFunctionID.DATEPART_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <AGE_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> [
     * "," SQLArgument(prn) ] f4 -> ")"
     */
    @Override
    public Object visit(Func_Age n, Object argu) {
        setFunctionInfo(IFunctionID.AGE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <BIT_LENGTH_> f1 -> "(" f2 -> SQLArgument(prn)
     * f3 -> ")"
     */

    @Override
    public Object visit(Func_BitLength n, Object argu) {
        setFunctionInfo(IFunctionID.AGE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <OCTET_LENGTH_> f1 -> "(" f2 ->
     * SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_OctetLength n, Object argu) {
        setFunctionInfo(IFunctionID.OCTETLENGTH_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> ( <CHAR_LENGTH_> | <CHARACTER_LENGTH_> ) f1 ->
     * "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_CharLength n, Object argu) {
        setFunctionInfo(IFunctionID.CHARLENGTH_ID,
                IdentifierHandler.normalizeCase("char_length"));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <CONVERT_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ( <USING_> | "," )
     * f4 -> ( <STRING_LITERAL> | Identifier(prn) )
     * f5 -> [ "," ( <STRING_LITERAL> | Identifier(prn) ) ]
     * f6 -> ")"
     */
    @Override
    public Object visit(Func_Convert n, Object argu) {
        setFunctionInfo(IFunctionID.CONVERT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);

        aSqlExpression.setArgSeparator(" ");

        SqlExpression aExp = new SqlExpression();
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        NodeToken token = (NodeToken) n.f3.choice;
        if (token.tokenImage.compareTo(",") != 0) {
            aExp.setConstantValue(token.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp);
            aSqlExpression.setArgSeparator(" ");
        } else {
            aSqlExpression.setArgSeparator(",");
        }

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        IdentifierHandler ih = new IdentifierHandler();
        if (n.f4.which == 0) {
            aExp1.setConstantValue(((NodeToken) n.f4.choice).tokenImage);
        } else {
            aExp1.setConstantValue((String) n.f4.choice.accept(ih, argu));
        }
        aSqlExpression.getFunctionParams().add(aExp1);

        if (n.f5.present()) {
            SqlExpression aExp2 = new SqlExpression();
            aExp2.setExprType(SqlExpression.SQLEX_CONSTANT);
            NodeChoice nc = (NodeChoice) ((NodeSequence) n.f5.node).elementAt(1);
            if (nc.which == 0) {
                aExp2.setConstantValue(((NodeToken) nc.choice).tokenImage);
            } else {
                aExp2.setConstantValue((String) nc.choice.accept(ih, argu));
            }
            aSqlExpression.getFunctionParams().add(aExp2);
        }

        return null;
    }

    /**
     * Grammar production: f0 -> <EXTRACT_> f1 -> "(" f2 -> DatetimeField() f3 ->
     * <FROM_> f4 -> SQLArgument(prn) f5 -> ")"
     */
    /*
     * Grammar production: f0 -> <EXTRACT_> f1 -> "(" f2 -> ( <YEAR_FROM> |
     * <QUARTER_FROM> | <MONTH_FROM> | <WEEK_FROM> | <DAY_FROM> | <HOUR_FROM> |
     * <MINUTE_FROM> | <SECOND_FROM> ) f3 -> SQLArgument(prn) f4 -> ")"
     */

    @Override
    public Object visit(Func_Extract n, Object argu) {
        setFunctionInfo(IFunctionID.EXTRACT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        NodeToken theToken = (NodeToken) n.f2.choice;
        SqlExpression aExp = new SqlExpression();
        // aExp.argSeparator = " ";
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(theToken.tokenImage.substring(0,
                theToken.tokenImage.length() - 4));
        aSqlExpression.getFunctionParams().add(aExp);

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setArgSeparator("  ");
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp1.setConstantValue("FROM");
        aSqlExpression.getFunctionParams().add(aExp1);

        n.f3.accept(this, argu);

        return null;
    }

    /**
     * Grammar production: f0 -> <OVERLAY_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * <PLASING_> f4 -> SQLArgument(prn) f5 -> <FROM_> f6 -> SQLArgument(prn) f7 -> [
     * <FOR_> SQLArgument(prn) ] f8 -> ")"
     */
    @Override
    public Object visit(Func_Overlay n, Object argu) {
        setFunctionInfo(IFunctionID.OVERLAY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        n.f2.accept(this, argu);

        SqlExpression aExp = new SqlExpression();
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(n.f3.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp);

        n.f4.accept(this, argu);

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setArgSeparator("  ");
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp1.setConstantValue(n.f5.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp1);

        n.f6.accept(this, argu);

        if (n.f7.present()) {
            NodeToken tk = (NodeToken) ((NodeSequence) n.f7.node).elementAt(0);
            SQLArgument sqlarg = (SQLArgument) ((NodeSequence) n.f7.node)
            .elementAt(1);
            SqlExpression aExp2 = new SqlExpression();
            aExp2.setArgSeparator("  ");
            aExp2.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp2.setConstantValue(tk.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp2);
            sqlarg.accept(this, argu);
        }

        return null;
    }

    /**
     * Grammar production: f0 -> <SUBSTRING_> f1 -> "(" f2 -> SQLArgument(prn)
     * f3 -> [ <FROM_> SQLArgument(prn) ] f4 -> [ <FOR_> SQLArgument(prn) ] f5 ->
     * ")"
     */
    @Override
    public Object visit(Func_Substring n, Object argu) {
        setFunctionInfo(IFunctionID.SUBSTRING_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        n.f2.accept(this, argu);

        if (n.f3.present()) {
            NodeToken tk = (NodeToken) ((NodeSequence) n.f3.node).elementAt(0);
            SQLArgument sqlarg = (SQLArgument) ((NodeSequence) n.f3.node)
            .elementAt(1);
            SqlExpression aExp1 = new SqlExpression();
            aExp1.setArgSeparator("  ");
            aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp1.setConstantValue(tk.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp1);
            sqlarg.accept(this, argu);
        }
        if (n.f4.present()) {
            NodeToken tk = (NodeToken) ((NodeSequence) n.f4.node).elementAt(0);
            SQLArgument sqlarg = (SQLArgument) ((NodeSequence) n.f4.node)
            .elementAt(1);
            SqlExpression aExp1 = new SqlExpression();
            aExp1.setArgSeparator("  ");
            aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp1.setConstantValue(tk.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp1);
            sqlarg.accept(this, argu);
        }

        return null;
    }

    /**
     * Grammar production: f0 -> <POSITION_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * <IN_> f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Position n, Object argu) {
        setFunctionInfo(IFunctionID.OVERLAY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        n.f2.accept(this, argu);

        SqlExpression aExp = new SqlExpression();
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(n.f3.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp);

        n.f4.accept(this, argu);

        return null;
    }

    /**
     * Grammar production: f0 -> <TO_HEX_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * ")"
     */
    @Override
    public Object visit(Func_ToHex n, Object argu) {
        setFunctionInfo(IFunctionID.TO_HEX_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <PG_CLIENT_ENCODING_> f1 -> <PARENTHESIS>
     */
    @Override
    public Object visit(Func_PgClientEncoding n, Object argu) {
        setFunctionInfo(IFunctionID.PG_CLIENT_ENCODING_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <QUOTE_LITERAL_> f1 -> "(" f2 ->
     * SQLArgument(prn) f3 -> ")"
     */
    @Override
    public Object visit(Func_QuoteLiteral n, Object argu) {
        setFunctionInfo(IFunctionID.QUOTE_LITERAL_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <QUOTE_IDENT_> f1 -> "(" f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_QuoteIdent n, Object argu) {
        setFunctionInfo(IFunctionID.QUOTE_IDENT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <MD5_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * ")"
     */
    @Override
    public Object visit(Func_Md5 n, Object argu) {
        setFunctionInfo(IFunctionID.MD5_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <CHR_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * ")"
     */
    @Override
    public Object visit(Func_Chr n, Object argu) {
        setFunctionInfo(IFunctionID.CHR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <TRANSLATE_> f1 -> "(" f2 -> SQLArgument(prn)
     * f3 -> "," f4 -> SQLArgument(prn) f5 -> "," f6 -> SQLArgument(prn) f7 ->
     * ")"
     */
    @Override
    public Object visit(Func_Translate n, Object argu) {
    	if (n.f7.present()) {
    		setFunctionInfo(IFunctionID.ST_TRANSLATE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
    	} else {
            setFunctionInfo(IFunctionID.TRANSLATE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
    	}

        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
		n.f7.accept(this, argu);


        return null;
    }

    /**
     * Grammar production:
     * f0 -> <TO_ASCII_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> [ "," SQLArgument(prn) ]
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_ToAscii n, Object argu) {
        setFunctionInfo(IFunctionID.TO_ASCII_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <STRPOS_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * "," f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_StrPos n, Object argu) {
        setFunctionInfo(IFunctionID.STRPOS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <SPLIT_PART_> f1 -> "(" f2 -> SQLArgument(prn)
     * f3 -> "," f4 -> SQLArgument(prn) f5 -> "," f6 -> SQLArgument(prn) f7 ->
     * ")"
     */
    @Override
    public Object visit(Func_SplitPart n, Object argu) {
        setFunctionInfo(IFunctionID.SPLIT_PART_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <REPEAT_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * "," f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Repeat n, Object argu) {
        setFunctionInfo(IFunctionID.REPEAT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <GETBIT_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_GetBit n, Object argu) {
        setFunctionInfo( IFunctionID.GETBIT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <GET_BYTE_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_GetByte n, Object argu) {
        setFunctionInfo( IFunctionID.GETBYTE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <TO_DATE_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> [ "," SQLArgument(prn) ]
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_ToDate n, Object argu) {
        setFunctionInfo(IFunctionID.TODATE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this,argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <CLOCK_TIMESTAMP_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public Object visit(Func_ClockTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.CLOCK_TIMESTAMP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <STATEMENT_TIMESTAMP_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public Object visit(Func_StatementTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.STATEMENT_TIMESTAMP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <TRANSACTION_TIMESTAMP_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public Object visit(Func_TransactionTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.TRANSACTION_TIMESTAMP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <ENCODE_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * "," f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public Object visit(Func_Encode n, Object argu) {
        setFunctionInfo(IFunctionID.ENCODE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <DECODE_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ( "," SQLArgument(prn) )*
     * f6 -> ")"
     */
    @Override
    public Object visit(Func_Decode n, Object argu) {
        setFunctionInfo(IFunctionID.DECODE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <BTRIM_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> [ "," SQLArgument(prn) ]
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_Btrim n, Object argu) {
        setFunctionInfo(IFunctionID.BTRIM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <WIDTH_BUCKET_> f1 -> "(" f2 ->
     * SQLArgument(prn) f3 -> "," f4 -> SQLArgument(prn) f5 -> "," f6 ->
     * SQLArgument(prn) f7 -> "," f8 -> SQLArgument(prn) f9 -> ")"
     */
    @Override
    public Object visit(Func_Width_bucket n, Object argu) {
        setFunctionInfo(IFunctionID.WIDTH_BUCKET_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
        n.f8.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <SETSEED_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * ")"
     */
    @Override
    public Object visit(Func_Setseed n, Object argu) {
        setFunctionInfo(IFunctionID.SETSEED_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <RANDOM_> f1 -> "(" f2 -> ")"
     */
    @Override
    public Object visit(Func_Random n, Object argu) {
        setFunctionInfo(IFunctionID.RANDOM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }

    /**
     * Grammar production: f0 -> <CBRT_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * ")"
     */
    @Override
    public Object visit(Func_Cbrt n, Object argu) {
        setFunctionInfo(IFunctionID.CBRT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <NULLIF_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_NullIf n, Object argu) {
        setFunctionInfo(IFunctionID.NULLIF_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <SET_BIT_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ","
     * f6 -> SQLArgument(prn)
     * f7 -> ")"
     */
    @Override
    public Object visit(Func_SetBit n, Object argu) {
        setFunctionInfo(IFunctionID.SETBIT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <SET_BYTE_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ","
     * f6 -> SQLArgument(prn)
     * f7 -> ")"
     */
    @Override
    public Object visit(Func_SetByte n, Object argu) {
        setFunctionInfo(IFunctionID.SETBYTE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <TO_CHAR_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> [ "," SQLArgument(prn) ]
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_ToChar n, Object argu) {
        setFunctionInfo(IFunctionID.TOCHAR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <TO_NUMBER_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_ToNumber n, Object argu) {
        setFunctionInfo(IFunctionID.TONUMBER_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <TO_TIMESTAMP_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_ToTimestamp n, Object argu) {
        setFunctionInfo(IFunctionID.TOTIMESTAMP_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <ADD_MONTHS_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_AddMonths n, Object argu) {
        setFunctionInfo(IFunctionID.ADDMONTHS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <JUSTIFY_DAYS_>
     * f1 -> "("
     * f2 -> [ <INTERVAL_> ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_JustifyDays n, Object argu) {
        setFunctionInfo(IFunctionID.JUSTIFYDAYS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <JUSTIFY_Hours_>
     * f1 -> "("
     * f2 -> [ <INTERVAL_> ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_JustifyHours n, Object argu) {
        setFunctionInfo(IFunctionID.JUSTIFYHOURS_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <JUSTIFY_INTERVAL_>
     * f1 -> "("
     * f2 -> [ <INTERVAL_> ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_JustifyInterval n, Object argu) {
        setFunctionInfo(IFunctionID.JUSTIFYINTERVAL_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <LAST_DAY_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_LastDay n, Object argu) {
        setFunctionInfo(IFunctionID.LASTDAY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <MONTHS_BETWEEN_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_MonthsBetween n, Object argu) {
        setFunctionInfo(IFunctionID.MONTHSBETWEEN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <NEXT_DAY_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_NextDay n, Object argu) {
        setFunctionInfo(IFunctionID.NEXTDAY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <CURRENT_DATABASE_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public Object visit(Func_CurrentDatabase n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTDATABASE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setConstantValue("'"
                + commandToExecute.getClientContext().getSysDatabase().getDbname().replaceAll(
                        "'", "''") + "'");
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <CURRENT_SCHEMA_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public Object visit(Func_CurrentSchema n, Object argu) {
        setFunctionInfo(IFunctionID.CURRENTSCHEMA_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <BIT_AND_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_BitAnd n, Object argu) {
        setFunctionInfo(IFunctionID.BITAND_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <BIT_OR_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_BitOr n, Object argu) {
        setFunctionInfo(IFunctionID.BITOR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> ( <BOOL_AND_> | <EVERY_> )
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_BoolAnd n, Object argu) {
        setFunctionInfo(n.f0.which == 0 ? IFunctionID.BOOLAND_ID : IFunctionID.EVERY_ID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <BOOL_OR_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public Object visit(Func_BoolOr n, Object argu) {
        setFunctionInfo(IFunctionID.BOOLOR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> ( <CORR_> | <COVAR_POP_> | <COVAR_SAMP_> )
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_CorrCov n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.CORR_ID;
            break;
        case 1:
            funcID = IFunctionID.COVARPOP_ID;
            break;
        case 2:
            funcID = IFunctionID.COVARSAMP_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));

        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> ( <REGR_AVGX_> | <REGR_AVGY_> | <REGR_COUNT_> | <REGR_INTERCEPT_> | <REGR_R2_>
     * | <REGR_SLOPE_> | <REGR_SXX_> | <REGR_SXY_> | <REGR_SYY_> )
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_Regr n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.REGRAVX_ID;
            break;
        case 1:
            funcID = IFunctionID.REGRAVY_ID;
            break;
        case 2:
            funcID = IFunctionID.REGRCOUNT_ID;
            break;
        case 3:
            funcID = IFunctionID.REGRINTERCEPT_ID;
            break;
        case 4:
            funcID = IFunctionID.REGRR2_ID;
            break;
        case 5:
            funcID = IFunctionID.REGRSLOPE_ID;
            break;
        case 6:
            funcID = IFunctionID.REGRSXX_ID;
            break;
        case 7:
            funcID = IFunctionID.REGRSXY_ID;
            break;
        case 8:
            funcID = IFunctionID.REGRSYY_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }
    /**
     * Grammar production:
     * f0 -> <REGEXP_REPLACE_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ","
     * f6 -> SQLArgument(prn)
     * f7 -> [ "," SQLArgument(prn) ]
     * f8 -> ")"
     */
    @Override
    public Object visit(Func_RegexReplace n, Object argu) {
        setFunctionInfo(IFunctionID.REGEXPREPLACE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        n.f6.accept(this, argu);
        n.f7.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <ABBREV_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Abbrev n, Object argu) {
        setFunctionInfo(IFunctionID.ABBREV_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <BROADCAST_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Broadcast n, Object argu) {
        setFunctionInfo(IFunctionID.BROADCAST_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <FAMILY_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Family n, Object argu) {
        setFunctionInfo(IFunctionID.FAMILY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <HOST_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Host n, Object argu) {
        setFunctionInfo(IFunctionID.HOST_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <HOSTMASK_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Hostmask n, Object argu) {
        setFunctionInfo(IFunctionID.HOSTMASK_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <MASKLEN_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Masklen n, Object argu) {
        setFunctionInfo(IFunctionID.MASKLEN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <NETMASK_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Netmask n, Object argu) {
        setFunctionInfo(IFunctionID.NETMASK_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <NETWORK_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Network n, Object argu) {
        setFunctionInfo(IFunctionID.NETWORK_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <SET_MASKLEN_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public Object visit(Func_Set_Masklen n, Object argu) {
        setFunctionInfo(IFunctionID.SET_MASKLEN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <TEXT_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ")"
     */
    @Override
    public Object visit(Func_Text n, Object argu) {
        setFunctionInfo(IFunctionID.TEXT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        return null;
    }

	/**
	 * Grammar production: 
	 * f0 -> <ST_ASTEXT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_AsText n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_TRANSFORM_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */
	@Override
	public Object visit(Func_ST_Transform n, Object argu) {
		setFunctionInfo(IFunctionID.ST_TRANSFORM_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_DISTANCE_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */
	@Override
	public Object visit(Func_ST_Distance n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DISTANCE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_DWITHIN_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> "," 
	 * f6 -> SQLArgument(prn) 
	 * f7 -> ")"
	 */
	@Override
	public Object visit(Func_ST_DWithin n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DWITHIN_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_Length_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> [ "," SQLArgument(prn) ] 
	 * f4 -> ")"
	 */
	@Override
	public Object visit(Func_ST_Length n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LENGTH_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_AREA_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> [ "," SQLArgument(prn) ] 
	 * f4 -> ")"
	 */
	@Override
	public Object visit(Func_ST_Area n, Object argu) {
		setFunctionInfo(IFunctionID.ST_AREA_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_ASBINARY_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> [ "," SQLArgument(prn) ] 
	 * f4 -> ")"
	 */
	@Override
	public Object visit(Func_ST_AsBinary n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASBINARY_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_BOUNDARY_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_Boundary n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BOUNDARY_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_BUFFER_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> [ "," SQLArgument(prn) ] 
	 * f6 -> ")"
	 */
	@Override
	public Object visit(Func_ST_Buffer n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BUFFER_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f5.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_CENTROID_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_Centroid n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CENTROID_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ADDMEASURE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ADDMEASURE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ADDPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ADDPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_AFFINE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_AFFINE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_AREA2D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_AREA2D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_ASEWKB_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> [ "," SQLArgument(prn) ] 
	 * f4 -> ")"
	 */
	@Override
	public Object visit(Func_ST_ASEWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASEWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_ASEWKT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_ASEWKT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASEWKT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ASGEOJSON n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASGEOJSON_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		n.f4.accept(this, argu);
		n.f5.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ASGML n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASGML_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		n.f4.accept(this, argu);
		n.f5.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ASHEXEWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASHEXEWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ASKML n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASKML_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ASSVG n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ASSVG_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_AZIMUTH n, Object argu) {
		setFunctionInfo(IFunctionID.ST_AZIMUTH_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_BDMPOLYFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BDMPOLYFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_BDPOLYFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BDPOLYFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_BUILDAREA n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BUILDAREA_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_BYTEA n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BYTEA_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CHIP_IN n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CHIP_IN_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CHIP_OUT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CHIP_OUT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CLOSESTPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CLOSESTPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_COLLECT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> [ "," SQLArgument(prn) ] 
	 * f4 -> ")"
	 */
	@Override
	public Object visit(Func_ST_COLLECT n, Object argu) {
		if (n.f3.present()) {
		    setFunctionInfo(IFunctionID.ST_COLLECT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		} else {
		    setFunctionInfo(IFunctionID.ST_COLLECT_AGG_ID,
					IdentifierHandler.normalizeCase(n.f0.choice.toString()));			
		}
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_COLLECTIONEXTRACT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */
	@Override
	public Object visit(Func_ST_COLLECTIONEXTRACT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_COLLECTIONEXTRACT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_COMBINE_BBOX n, Object argu) {
		setFunctionInfo(IFunctionID.ST_COMBINE_BBOX_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_COMPRESSION n, Object argu) {
		setFunctionInfo(IFunctionID.ST_COMPRESSION_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CONTAINS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CONTAINS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CONTAINSPROPERLY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CONTAINSPROPERLY_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CONVEXHULL n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CONVEXHULL_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_COORDDIM n, Object argu) {
		setFunctionInfo(IFunctionID.ST_COORDDIM_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_COVEREDBY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_COVEREDBY_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_COVERS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_COVERS_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CROSSES n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CROSSES_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_CURVETOLINE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_CURVETOLINE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_DATATYPE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DATATYPE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_DFULLYWITHIN n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DFULLYWITHIN_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_DIFFERENCE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DIFFERENCE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_DIMENSION n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DIMENSION_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_DISJOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DISJOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_DISTANCE_SPHERE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DISTANCE_SPHERE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_DISTANCE_SPHEROID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_DISTANCE_SPHEROID_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ENDPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ENDPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString())); 
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ENVELOPE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ENVELOPE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_EQUALS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_EQUALS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_EXTERIORRING n, Object argu) {
		setFunctionInfo(IFunctionID.ST_EXTERIORRING_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString())); 
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_FACTOR n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FACTOR_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_FIND_EXTENT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FIND_EXTENT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_FORCERHR n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FORCERHR_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_FORCE_2D_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_FORCE_2D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FORCE_2D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_FORCE_3D_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_FORCE_3D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FORCE_3D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_FORCE_3DM_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_FORCE_3DM n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FORCE_3DM_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_FORCE_3DZ_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_FORCE_3DZ n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FORCE_3DZ_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_FORCE_4D_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_FORCE_4D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FORCE_4D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_FORCE_COLLECTION_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_FORCE_COLLECTION n, Object argu) {
		setFunctionInfo(IFunctionID.ST_FORCE_COLLECTION_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_GEOMETRYTYPE_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_GEOMETRYTYPE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMETRYTYPE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOGFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOGFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOGFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOGFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOGRAPHYFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOGRAPHYFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOHASH n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOHASH_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOMCOLLFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMCOLLFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOMCOLLFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMCOLLFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOMFROMEWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMFROMEWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_GEOMFROMEWKT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_GEOMFROMEWKT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMFROMEWKT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOMFROMGML n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMFROMGML_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOMFROMKML n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMFROMKML_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_GEOMFROMTEXT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> [ "," SQLArgument(prn) ] 
	 * f4 -> ")"
	 */
	@Override
	public Object visit(Func_ST_GEOMFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GEOMFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_GMLTOSQL n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GMLTOSQL_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_HASARC n, Object argu) {
		setFunctionInfo(IFunctionID.ST_HASARC_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_HAUSDORFFDISTANCE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_HAUSDORFFDISTANCE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f5.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_HEIGHT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_HEIGHT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_INTERIORRINGN n, Object argu) {
		setFunctionInfo(IFunctionID.ST_INTERIORRINGN_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString())); 
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_INTERSECTION n, Object argu) {
		setFunctionInfo(IFunctionID.ST_INTERSECTION_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_INTERSECTS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_INTERSECTS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ISCLOSED n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ISCLOSED_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ISEMPTY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ISEMPTY_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ISRING n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ISRING_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ISSIMPLE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ISSIMPLE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ISVALID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ISVALID_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ISVALIDREASON n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ISVALIDREASON_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LENGTH2D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LENGTH2D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LENGTH2D_SPHEROID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LENGTH2D_SPHEROID_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LENGTH3D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LENGTH3D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LENGTH3D_SPHEROID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LENGTH3D_SPHEROID_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LENGTH_SPHEROID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LENGTH_SPHEROID_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LINECROSSINGDIRECTION n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINECROSSINGDIRECTION_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LINEFROMMULTIPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINEFROMMULTIPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LINEFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINEFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LINEFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINEFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LINEMERGE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINEMERGE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LINESTRINGFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINESTRINGFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LINETOCURVE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINETOCURVE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_LINE_INTERPOLATE_POINT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_LINE_INTERPOLATE_POINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINE_INTERPOLATE_POINT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_LINE_LOCATE_POINT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_LINE_LOCATE_POINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINE_LOCATE_POINT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_LINE_SUBSTRING_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> "," 
	 * f6 -> SQLArgument(prn) 
	 * f7 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_LINE_SUBSTRING n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LINE_SUBSTRING_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LOCATEBETWEENELEVATIONS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LOCATEBETWEENELEVATIONS_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_LOCATE_ALONG_MEASURE_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_LOCATE_ALONG_MEASURE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LOCATE_ALONG_MEASURE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_LOCATE_BETWEEN_MEASURES_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> "," 
	 * f6 -> SQLArgument(prn) 
	 * f7 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_LOCATE_BETWEEN_MEASURES n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LOCATE_BETWEEN_MEASURES_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_LONGESTLINE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_LONGESTLINE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_M n, Object argu) {
		setFunctionInfo(IFunctionID.ST_M_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_MAKEENVELOPE_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> "," 
	 * f6 -> SQLArgument(prn) 
	 * f7 -> "," 
	 * f8 -> SQLArgument(prn) 
	 * f9 -> "," 
	 * f10 -> SQLArgument(prn) 
	 * f11 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_MAKEENVELOPE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MAKEENVELOPE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		n.f8.accept(this, argu);
		n.f10.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MAKEPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MAKEPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f5.accept(this, argu);
		n.f6.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MAKEPOINTM n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MAKEPOINTM_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MAKEPOLYGON n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MAKEPOLYGON_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f3.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MAXDISTANCE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MAXDISTANCE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MEMCOLLECT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MEMCOLLECT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MEM_SIZE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MEM_SIZE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MINIMUMBOUNDINGCIRCLE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MINIMUMBOUNDINGCIRCLE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MLINEFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MLINEFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MLINEFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MLINEFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MPOINTFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MPOINTFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MPOINTFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MPOINTFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MPOLYFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MPOLYFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MPOLYFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MPOLYFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_MULTI_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_MULTI n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MULTI_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MULTILINEFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MULTILINEFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MULTILINESTRINGFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MULTILINESTRINGFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MULTIPOINTFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MULTIPOINTFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MULTIPOINTFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MULTIPOINTFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MULTIPOLYFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MULTIPOLYFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_MULTIPOLYGONFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_MULTIPOLYGONFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_NDIMS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_NDIMS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_NPOINTS_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_NPOINTS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_NPOINTS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_NRINGS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_NRINGS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_NUMGEOMETRIES n, Object argu) {
		setFunctionInfo(IFunctionID.ST_NUMGEOMETRIES_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_NUMINTERIORRING n, Object argu) {
		setFunctionInfo(IFunctionID.ST_NUMINTERIORRING_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_NUMINTERIORRINGS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_NUMINTERIORRINGS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString())); 
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_NUMPOINTS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_NUMPOINTS_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_ORDERINGEQUALS_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */	 
	@Override
	public Object visit(Func_ST_ORDERINGEQUALS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ORDERINGEQUALS_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_OVERLAPS n, Object argu) {
		setFunctionInfo(IFunctionID.ST_OVERLAPS_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_PERIMETER n, Object argu) {
		setFunctionInfo(IFunctionID.ST_PERIMETER_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_PERIMETER2D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_PERIMETER2D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_PERIMETER3D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_PERIMETER3D_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POINT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POINTFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POINTFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POINTFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POINTFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POINTN n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POINTN_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POINTONSURFACE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POINTONSURFACE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POINT_INSIDE_CIRCLE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POINT_INSIDE_CIRCLE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POLYFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POLYFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POLYFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POLYFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POLYGON n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POLYGON_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POLYGONFROMTEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POLYGONFROMTEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POLYGONFROMWKB n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POLYGONFROMWKB_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POSTGIS_GIST_JOINSEL n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POSTGIS_GIST_JOINSEL_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_POSTGIS_GIST_SEL n, Object argu) {
		setFunctionInfo(IFunctionID.ST_POSTGIS_GIST_SEL_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_RELATE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_RELATE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f5.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_REMOVEPOINT_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */
	@Override
	public Object visit(Func_ST_REMOVEPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_REMOVEPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_REVERSE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_REVERSE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ROTATEX n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ROTATEX_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ROTATEY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ROTATEY_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_ROTATEZ n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ROTATEZ_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SCALE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SCALE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		n.f7.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_SEGMENTIZE_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_SEGMENTIZE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SEGMENTIZE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SETFACTOR n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SETFACTOR_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_SETPOINT_ID_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> "," 
	 * f6 -> SQLArgument(prn) 
	 * f7 -> ")"
	 */	
	@Override
	public Object visit(Func_ST_SETPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SETPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_SETSRID_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */
	@Override
	public Object visit(Func_ST_SETSRID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SETSRID_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SHIFT_LONGITUDE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SHIFT_LONGITUDE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SHORTESTLINE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SHORTESTLINE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_SIMPLIFY_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn) 
	 * f5 -> ")"
	 */
	@Override
	public Object visit(Func_ST_SIMPLIFY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SIMPLIFY_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SIMPLIFYPRESERVETOPOLOGY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SIMPLIFYPRESERVETOPOLOGY_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production:
	 * f0 -> <ST_SNAPTOGRID_>
	 * f1 -> <PARENTHESIS_START_>
	 * f2 -> SQLArgument(prn)
	 * f3 -> ","
	 * f4 -> SQLArgument(prn)
	 * f5 -> [ "," SQLArgument(prn) ]
	 * f6 -> [ "," SQLArgument(prn) ]
	 * f7 -> [ "," SQLArgument(prn) ]
	 * f8 -> [ "," SQLArgument(prn) ]
	 * f9 -> <PARENTHESIS_CLOSE_>
	 */
	@Override
	public Object visit(Func_ST_SNAPTOGRID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SNAPTOGRID_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f5.accept(this, argu);
		n.f6.accept(this, argu);
		n.f7.accept(this, argu);
		n.f8.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SRID n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SRID_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString())); 
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_STARTPOINT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_STARTPOINT_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString())); 
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SUMMARY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SUMMARY_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SYMDIFFERENCE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SYMDIFFERENCE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_SYMMETRICDIFFERENCE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_SYMMETRICDIFFERENCE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_TEXT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_TEXT_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_TOUCHES n, Object argu) {
		setFunctionInfo(IFunctionID.ST_TOUCHES_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_TRANSLATE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_TRANSLATE_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu); 
		n.f7.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_TRANSSCALE n, Object argu) {
		setFunctionInfo(IFunctionID.ST_TRANSSCALE_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		n.f6.accept(this, argu);
		n.f8.accept(this, argu);
		n.f10.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_WIDTH n, Object argu) {
		setFunctionInfo(IFunctionID.ST_WIDTH_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_WITHIN n, Object argu) {
		setFunctionInfo(IFunctionID.ST_WITHIN_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_WKBTOSQL n, Object argu) {
		setFunctionInfo(IFunctionID.ST_WKBTOSQL_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_WKTTOSQL n, Object argu) {
		setFunctionInfo(IFunctionID.ST_WKTTOSQL_ID,
				IdentifierHandler.normalizeCase(n.f0.tokenImage));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_X n, Object argu) {
		setFunctionInfo(IFunctionID.ST_X_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_Y n, Object argu) {
		setFunctionInfo(IFunctionID.ST_Y_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	@Override
	public Object visit(Func_ST_Z n, Object argu) {
		setFunctionInfo(IFunctionID.ST_Z_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_ZMFLAG_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_ZMFLAG n, Object argu) {
		setFunctionInfo(IFunctionID.ST_ZMFLAG_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_BOX2D_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_BOX2D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BOX2D_ID, 
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_BOX3D_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_BOX3D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_BOX3D_ID, 
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_GEOMETRY> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_GEOMETRY n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMETRY_ID, 
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_GEOMETRYN> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> "," 
	 * f4 -> SQLArgument(prn)
	 * f5 -> ")"
	 */
	@Override
	public Object visit(Func_ST_GEOMETRYN n, Object argu) {
		setFunctionInfo(IFunctionID.ST_GEOMETRYN_ID, 
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		n.f4.accept(this, argu);
		return null;
	}

	/**
	 * Grammar production: 
	 * f0 -> <ST_EXTENT> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_EXTENT n, Object argu) {
		setFunctionInfo(IFunctionID.ST_EXTENT_ID, 
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}
 
	/**
	 * Grammar production: 
	 * f0 -> <ST_EXTENT3D> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn)
	 * f3 -> ")"
	 */
	@Override
	public Object visit(Func_ST_EXTENT3D n, Object argu) {
		setFunctionInfo(IFunctionID.ST_EXTENT3D_ID, 
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

	
	// ----------Old Names of the Spatail Functions

	/**
	 * Grammar production: 
	 * f0 -> <POSTGIS_DROPBBOX_> 
	 * f1 -> "(" 
	 * f2 -> SQLArgument(prn) 
	 * f3 -> ")"
	 */	 
	@Override
	public Object visit(Func_POSTGIS_DROPBBOX n, Object argu) {
		setFunctionInfo(IFunctionID.POSTGIS_DROPBBOX_ID,
				IdentifierHandler.normalizeCase(n.f0.choice.toString()));
		n.f2.accept(this, argu);
		return null;
	}

    // ----------Helper Functions
    /**
     * This is a helper function which sets the function ID and the function
     * name
     *
     * @param funcid
     * @param functionName
     */
    private void setFunctionInfo(int funcid, String functionName) {
        aSqlExpression.setFunctionId(funcid);
        aSqlExpression.setFunctionName(functionName);
        
        
    }
}
