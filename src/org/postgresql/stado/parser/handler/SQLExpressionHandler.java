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

import java.util.Observable;
import java.util.Observer;
import java.util.Stack;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.core.syntaxtree.CidrLiteral;
import org.postgresql.stado.parser.core.syntaxtree.DateLiteral;
import org.postgresql.stado.parser.core.syntaxtree.FloatingPointNumber;
import org.postgresql.stado.parser.core.syntaxtree.FunctionCall;
import org.postgresql.stado.parser.core.syntaxtree.GeometryLiteral;
import org.postgresql.stado.parser.core.syntaxtree.InetLiteral;
import org.postgresql.stado.parser.core.syntaxtree.IntegerLiteral;
import org.postgresql.stado.parser.core.syntaxtree.IntervalLiterals;
import org.postgresql.stado.parser.core.syntaxtree.IsBooleanExpression;
import org.postgresql.stado.parser.core.syntaxtree.IsNullExpression;
import org.postgresql.stado.parser.core.syntaxtree.MacaddrLiteral;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.NullLiterals;
import org.postgresql.stado.parser.core.syntaxtree.PreparedStmtParameter;
import org.postgresql.stado.parser.core.syntaxtree.PseudoColumn;
import org.postgresql.stado.parser.core.syntaxtree.SQLComparitiveExpressionOperand;
import org.postgresql.stado.parser.core.syntaxtree.SQLComplexExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLMultiplicativeExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLMultiplicativeExpressionOperand;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrimaryExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLSimpleExpression;
import org.postgresql.stado.parser.core.syntaxtree.SimpleExpressionOperand;
import org.postgresql.stado.parser.core.syntaxtree.TableColumn;
import org.postgresql.stado.parser.core.syntaxtree.TextLiterals;
import org.postgresql.stado.parser.core.syntaxtree.TimeLiteral;
import org.postgresql.stado.parser.core.syntaxtree.TimeStampLiteral;
import org.postgresql.stado.parser.core.syntaxtree.binaryLiteral;
import org.postgresql.stado.parser.core.syntaxtree.booleanLiteral;
import org.postgresql.stado.parser.core.syntaxtree.extendbObject;
import org.postgresql.stado.parser.core.syntaxtree.hex_decimalLiteral;
import org.postgresql.stado.parser.core.syntaxtree.numberValue;
import org.postgresql.stado.parser.core.syntaxtree.stringLiteral;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;


/**
 * This class provides the required functionality to manipulate and fill up
 * the information via calls to visit() method series for SQLExpression object
 * as the expression is processed in the parser phase.
 */

public class SQLExpressionHandler extends ObjectDepthFirst implements Observer {
    private static final XLogger logger = XLogger.getLogger(SQLExpressionHandler.class);

    private Command commandToExecute;

    public SqlExpression aroot = null;

    // -- This is an SQL Expression -- which is presently under consideration I
    // have added this
    // here so that we can get hold of the expression that we are presently
    // working on and add
    // the tokens to the expr string.

    // Later on a call to the -- children will be used to construct the
    // expression
    Stack<SqlExpression> sCurrent = new Stack<SqlExpression>();

    SysDatabase database = null;

    public SQLExpressionHandler(Command command) {
        this.commandToExecute = command;
        database = MetaData.getMetaData().getSysDatabase(command.getDBName());
    }

    /*
     * The Following functions are to make this class Observable
     */

    /**
     * f0 -> SQLMultiplicativeExpression(prn) f1 -> (
     * SimpleExpressionOperand(prn) )*
     */
    @Override
    public Object visit(SQLSimpleExpression n, Object obj) {
        /*-- Log -- Create a SQL Expression - This expression is the master expression
         - Will I get the SQLExpression call again - Yes
         In case we have a PrimarySQL Expression as a SubQuery then we might land
         up with this kind of situation
         - How about handling the SubQuery part in a different Class itself?
         Will I then be able to reuse code - For expression?
         Yes -- Secondly If we create a new SQLExpressionHandler for these expression
         in the select clause -- we should not have any problem.
         - Will I then Have to handle Select Clause Here To -- I think a Select Tuple Clause
         should be handled here-- Since for every SelectTuple we are going to need a new SqlExpression

         - Finally I decided to send in the node through rather than returning the
         value
         - Finally I decided to do neither but rather have some shared data in the class
         which will be accessible by the functions and will fill in the values
         This will be a SqlExpression and we will have to make the tree with reference to it.

         - Allow for a public variable which will hold the root.
         - Pass the argument in -- Presently working on this.
         */

        /*
         * Since the expansion of this Non terminal is SQLMul (+,- )
         * SimpleExpressionOperand The SQL Expression returned by SimpleExpr
         * will already have the left expr filled - we will set the right expr
         * and return back the master expression
         */

        /*
         * Final Algorithm : Create a new SQL Expression for
         * SQLMultiplicativeExpression and wait for the expression to get the
         * left expression
         *  - Send this as the argument to SQLMultiplicativeOperand -- -- And
         * allow it to create the rigthexpression and place it.
         */

        /*
         * Create a helper object and send it through the accept function
         */
        ExpressionArgumentHelper exprHelp = new ExpressionArgumentHelper();
        exprHelp.argument = new SqlExpression();

        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("SqlExpressionHandler.java  139  : "
                    + exprHelp.argument.toString());
        }

        /*
         * Add the SqlExpression to the Vector - which keeps track of present
         * SqlExpression on the stack -- we use this to add expr strings as we
         * encounter tokens. -- A similar treat ment will be given to the
         * SQLComplexExpr
         */
        sCurrent.push(exprHelp.argument);

        /*
         * This is a call to Multiplicative Expression -- we Should get a SQL
         * Expression all set in the exprHelp.argument
         */
        n.f0.accept(this, exprHelp);

        /*
         * This Might blow up if the argument does not match the expression -
         * TODO: check for null and check for type match
         */
        /*
         * We will Now use the same Expression Arg helper to get hold of the
         * second Part of the argument
         */
        /*
         * This is a optional List -- of + or - or | followed by a mulexpression
         */

        if (n.f1.present()) {
            /*
             * In case we have multiple Operands -- We Still expect only one
             * simple expression out of this processing - which will be the root
             * of the expression -- It is important that I dont change the
             * masterSql Expression -- It will not be the same but will be
             * linked to its parent- what we get here will be the Root of the
             * expression.
             */
            n.f1.accept(this, exprHelp);
            /*
             * We now put the previously built expression in the left side of
             * master expression The master expression has to be of type
             * Operand.
             */
            /*
             * This node will be of type operand.
             */
            aroot = exprHelp.argument;
        } else {
            /* In case we have a single operand */
            aroot = exprHelp.argument;
        }
        /*
         * There is no significance of this statement -- we deal every thing
         * through the aRoot public variable
         */

        /*
         * Before going Out -- Remove the occurrence of the created SqlExpression
         * from the stack
         */
        sCurrent.pop();
        return aroot;
    }

    /**
     * Grammar production:
     * f0 -> [ <SQUARE_ROOT_> | <CUBE_ROOT_> | <FACTORIAL_PREFIX_> | <ABSOLUTE_> | <NOT_BITWISE_> ]
     * f1 -> [ <PLUS_> | <MINUS_> ]
     * f2 -> ( FunctionCall(prn) | TableColumn(prn) | PseudoColumn(prn) | numberValue(prn) | "(" SQLComplexExpression(prn) ")" | booleanLiteral(prn) | stringLiteral(prn) | NullLiterals(prn) | IntervalLiterals(prn) | TextLiterals(prn) | PreparedStmtParameter(prn) | TimeStampLiteral(prn) | TimeLiteral(prn) | DateLiteral(prn) | binaryLiteral(prn) | hex_decimalLiteral(prn) | IntegerLiteral(prn) )
     * f3 -> [ <FACTORIAL_> ]
     * f4 -> [ "::" types() ]
     */
    @Override
    public Object visit(SQLPrimaryExpression n, Object argu) {
        /*
         * We are getting a SQLExpression in the argument ---
         */

        String sign = null;
        String operandSign = null;
        // check if it's a FACTORIAL_PREFIX, ABSOLUTE, CUBE ROOT or SQUARE ROOT operator
        if (n.f0.present()) {
            NodeToken aNodeToken = (NodeToken) ((NodeChoice) n.f0.node).choice;
            sign = aNodeToken.tokenImage;
        }

        /* We first check if the sign is present */
        if (n.f1.present()) {

            NodeChoice aNodeChoice = (NodeChoice) n.f1.node;
            switch (aNodeChoice.which) {
            case 0:
            case 1:
                NodeToken aToken = (NodeToken) aNodeChoice.choice;
                if (n.f0.present()) {
                    operandSign = aToken.tokenImage;
                } else {
                    sign = aToken.tokenImage;
                }
                break;
            }
        }

        if (n.f3.present()) {
            NodeToken aNodeToken = (NodeToken) n.f3.node;

            if (n.f1.present()) {
                operandSign = sign;
            }

            sign = aNodeToken.tokenImage;
        }

        /*
         * Now the F2 can be a call to the -- Function / TableColum/
         * PseudoColum/NumberValue/ SQLComplexExpression TODO: Check whether The
         * Sql Expression is not null and secondly whether we are getting
         * operators .We should not be getting expresson of type operators. --
         * The argu - is the SqlExpression which is yet not filled with
         * information --
         */
        n.f2.accept(this, argu);
        /*
         * we have the SqlExpression in argu -- Since this is a top level
         * expression we need to assign it the sign
         *
         */
        SqlExpression aSqlExpression = ((ExpressionArgumentHelper) argu).argument;
        if (sign == null) {
            if (aSqlExpression.getUnaryOperator().equals("")) {
                aSqlExpression.setUnaryOperator("+");
            }
        } else {
            // check if it's sign in context of unary operator
            if (!aSqlExpression.getUnaryOperator().equals("")) {
                aSqlExpression.setOperandSign(aSqlExpression.getUnaryOperator());
            }

            aSqlExpression.setUnaryOperator(sign);
        }

        if (operandSign != null) {
            aSqlExpression.setOperandSign(operandSign);
        }

        /* This statement has no relevance */
        if (aSqlExpression.getExprType() == SqlExpression.SQLEX_PARAMETER) {
            aSqlExpression.setExprDataType(new ExpressionType());
        }

        if (n.f4.present()) {
            SqlExpression newSqlExpression = new SqlExpression();
            newSqlExpression.setExprType(SqlExpression.SQLEX_FUNCTION);
            newSqlExpression.setFunctionId(IFunctionID.CAST_ID);
            newSqlExpression.setFunctionName("::");
            newSqlExpression.getFunctionParams().add(aSqlExpression);
            DataTypeHandler typeHandler = new DataTypeHandler();
            n.f4.accept(typeHandler, argu);
            newSqlExpression.setExpTypeOfCast(typeHandler);
            aSqlExpression = newSqlExpression;
            ((ExpressionArgumentHelper) argu).argument = aSqlExpression;
        }

        return aSqlExpression;
    }

    /**
     * Grammar production:
     * f0 -> <PARENTHESIS_START_>
     * f1 -> <SELECT_>
     * f2 -> [ <ALL_> | <DISTINCT_> ]
     * f3 -> SelectList(prn)
     * f4 -> FromClause(prn)
     * f5 -> [ WhereClause(prn) ]
     * f6 -> [ LimitClause(prn) ]
     * f7 -> <PARENTHESIS_CLOSE_>
     */

    @Override
    public Object visit(PseudoColumn n, Object argu) {
        Object _ret = null;
        SqlExpression aSqlExpression = ((ExpressionArgumentHelper) argu).argument;

        // We will get a SqlExpression - in the argument and we have to make it
        // a Query Tree Expression
        QueryTree subQueryTree = new QueryTree();

        QueryTree parentTree = null;
            parentTree = commandToExecute.getaQueryTreeTracker().GetCurrentTree();
        commandToExecute.getaQueryTreeTracker().registerTree(subQueryTree);

        n.f0.accept(this, argu);

        // Check if the node is present -- all node optionals have this
        // function which indicate whether this particular node is present
        // or not.
        if (n.f2.present()) {
            // Now we know that the node is present - extract the node from the
            // under lying
            // member variable of F2
            NodeChoice aChoice = (NodeChoice) n.f2.node;

            // Check what choice the user has Made
            if (aChoice.which == 0) {
                // 0 - is for ALL as ALL is the first element in th list of F2
                subQueryTree.setDistinct(false);
            } else { // 1 - is for distinct
                subQueryTree.setDistinct(true);
            }
        } else {
            // Just incase - f2 is not present
            subQueryTree.setDistinct(false);
        }
        n.f1.accept(this, argu);
        n.f2.accept(this, argu);
        // This will fill the projection list of the tree
        // In order to deal with this we will generate a projection list
        ProjectionListHandler aProjectionListHandler = new ProjectionListHandler(
                commandToExecute);
        n.f3.accept(aProjectionListHandler, subQueryTree);

        // From Clause handler
        FromClauseHandler aFromClauseHandler = new FromClauseHandler(
                commandToExecute);
        n.f4.accept(aFromClauseHandler, subQueryTree);

        // Where Clause
        WhereClauseHandler aWhereClauseHandler = new WhereClauseHandler(
                commandToExecute);
        n.f5.accept(aWhereClauseHandler, subQueryTree);

        // Limit
        QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                commandToExecute);
        n.f6.accept(aQueryTreeHandler, subQueryTree);

        // Syntax Sugar
        n.f7.accept(this, argu);

        QueryTreeHandler.checkAndExpand(subQueryTree.getProjectionList(),
                subQueryTree.getRelationNodeList(), database, commandToExecute);

        // Projections
        QueryTreeHandler.checkAndFillTableNames(subQueryTree.getProjectionList(),
                subQueryTree.getRelationNodeList(), subQueryTree.getProjectionList(),
                QueryTreeHandler.PROJECTION,
                commandToExecute.getaQueryTreeTracker(), database);
        // Where Clause
        if (subQueryTree.getWhereRootCondition() != null) {
            QueryTreeHandler.ProcessWhereCondition(
                    subQueryTree.getWhereRootCondition(), subQueryTree,
                    commandToExecute.getaQueryTreeTracker(), database);
        }
        // Fill Expr Data Types
        QueryTreeHandler.FillAllExprDataTypes(subQueryTree,
                MetaData.getMetaData().getSysDatabase(
                        commandToExecute.getDBName()));
        aSqlExpression.setExprType(SqlExpression.SQLEX_SUBQUERY);
        aSqlExpression.setSubqueryTree(subQueryTree);
        subQueryTree.setQueryType(QueryTree.SCALAR);
        subQueryTree.setParentQueryTree(parentTree);
        commandToExecute.getaQueryTreeTracker().deRegisterCurrentTree();

        subQueryTree.setContainsAggregates(QueryTreeHandler.isAggregateQuery(subQueryTree));

        subQueryTree.processSubTree(aSqlExpression,
                commandToExecute.getaQueryTreeTracker());

        return _ret;
    }

    /**
     * f0 -> Func_CurrentDate() | Func_CurrentTime() | Func_CurrentTimeStamp() |
     * Func_Year() | Func_Month() | Func_Minute() | Func_Second() |
     * Func_AddDate() | Func_AddTime() | Func_Date() | Func_DateDiff() |
     * Func_Day() | Func_DayName() | Func_DayOfMonth() | Func_DayOfWeek() |
     * Func_DayOfYear() | Func_MonthName() | Func_SubDate() | Func_SubTime() |
     * Func_Time() | Func_TimeStamp() | Func_WeekOfYear() | Func_Now() | <ABS_>
     * "(" SQLArgument(prn) ")" | <AVERAGE_> "(" FunctionArgument(prn) ")" |
     * <COUNT_> "(" SQLArgument(prn) ")" | <COUNT_> "(" <STAR_> ")" | <EXTRACT_>
     * "(" types() "," SQLArgument(prn) ")" | <MAX_> "(" SQLArgument(prn) ")" |
     * <UPPER_> "(" SQLArgument(prn) ")" | <VERSION_> "(" ")" | <SUBSTRING_> "("
     * SQLArgument(prn) "," position(prn) "," [ length(prn) ] ")" | <TRIM_>
     * TrimSpec(prn) | <TRUNC_> "(" length(prn) "," length(prn) ")" | <RIGHT_>
     * "(" SQLArgument(prn) ")" | <LEFT_> "(" SQLArgument(prn) "," length(prn)
     * ")" | <LENGHT_> "(" SQLArgument(prn) ")" | <LOWER_> "(" SQLArgument(prn)
     * ")"
     */

    @Override
    public Object visit(FunctionCall n, Object argu) {
        SqlExpression aSqlExpression = ((ExpressionArgumentHelper) argu).argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_FUNCTION);
        FunctionHandler funcHandler = new FunctionHandler(aSqlExpression,
                commandToExecute);
        n.f0.choice.accept(funcHandler, argu);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> ( <STAR_> | <DIVIDE_> | <MOD_> | <DIV_> | <MODULO_> | <AND_BITWISE_> | <OR_BITWISE_> |
     *         <XOR_BITWISE_> | <NOT_BITWISE_> | <SHIFT_LEFT_BITWISE_> | <SHIFT_RIGHT_BITWISE_>
     *         <REGEX_MATCHES_CASE_INSTV_> | <REGEX_NOT_MATCHES_> | <REGEX_NOT_MATCHES_CASE_INSTV_>  )
     * f1 -> SQLPrimaryExpression(prn)
     */
    @Override
    public Object visit(SQLMultiplicativeExpression n, Object argu) {
        /*
         * We will expect a new SQL Expression here -- One of them will be the
         * Primary and the second one will be of type operand -- all this is
         * taken care of by the argument-- we already have anew one set here
         */
        n.f0.accept(this, argu);

        /*
         * TODO - Check for if the left expression is NULL We will throw an
         * exception here TODO :We however need to do this later when we decide
         * on the error handling strategy
         */
        if (n.f1.present()) {

            /*
             * We are now sure that we have more expressions to take care of --
             * This implies that there will be multiple calls to this function
             * Multi- below -- We Just send a new agrument here --
             */
            // SqlExpression aSqlExpression = new SqlExpression();
            /* Set the expression in the argument to this new expression */
            // exprHelper.argument = aSqlExpression;
            /*
             * We send this as an agrument -- to f1 -- This Function will have
             * to make a call to MultiplicativeOperand
             */
            n.f1.accept(this, argu);

        }

            /*
         -- TODO:
             * This expression cannot be of operand type. So we need to make
             * checks and add error handling-- Design of error handling
             * mechanism left
             */
            return null;
    }

    @Override
    public Object visit(SQLComparitiveExpressionOperand n, Object argu) {
        n.f0.accept(this, argu);

        return null;
    }

    @Override
    public Object visit(IsNullExpression n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;

        SqlExpression leftsqlexpr = exprHelper.argument;

        SqlExpression msqlexpr = new SqlExpression();

        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("SqlExpressionHandler.java  560  : "
                    + msqlexpr.toString());
        }
        // Push the SQL Expression-- Logic Starts
        SqlExpression sqlchild = sCurrent.pop();
        msqlexpr.setExprString(msqlexpr.getExprString() + sqlchild.getExprString());
        sCurrent.push(msqlexpr);
        // Logic Ends --

        msqlexpr.setLeftExpr(leftsqlexpr);

        msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
        if (n.f1.present()) {
            msqlexpr.setOperator("is not");        	
        } else {
            msqlexpr.setOperator("is");
        }

        n.f0.accept(this, argu);
        n.f1.accept(this, argu);
        /*
         * We will need to create a new Sql Expression here -- and we will
         * insert this as the right child of the Operand expression just created
         */
        SqlExpression rightSqlExpr = new SqlExpression();
        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("SqlExpressionHandler.java  606  : "
                    + rightSqlExpr.toString());
        }

        rightSqlExpr.setExprType(SqlExpression.SQLEX_CONSTANT);
        rightSqlExpr.setConstantValue(null);
        // Push the SQL Expression
        sCurrent.push(rightSqlExpr);
        //
        exprHelper.argument = rightSqlExpr;
        n.f2.accept(this, argu);
        /*
         * TODO: I have to check that the sql expression that I get is not null
         * and is not of type Operand.
         */
        msqlexpr.setRightExpr(exprHelper.argument);
        /*
         * Once we have all the required information we will return the
         * SqlExpression in the argument
         */
        exprHelper.argument = msqlexpr;

        // SQL -Child
        sqlchild = sCurrent.pop();
        msqlexpr.setExprString(msqlexpr.getExprString() + sqlchild.getExprString());
        //

        return null;
    }

    /**
     * Grammar production:
     * f0 -> ( <STAR_> | <DIVIDE_> | <MOD_> | <DIV_> | <MODULO_> | <AND_BITWISE_> | <OR_BITWISE_> | <XOR_BITWISE_> | <SHIFT_LEFT_BITWISE_> | <SHIFT_RIGHT_BITWISE_> | <CONTAINED_WITHIN_OR_EQUALS_> | <CONTAINS_OR_EQUALS_> )
     * f1 -> SQLPrimaryExpression(prn)
     */
    @Override
    public Object visit(SQLMultiplicativeExpressionOperand n, Object argu) {
        /*
         * We have now reached the terminal point -- A decison will need to be
         * made here regarding the type of expression that we need to make here --
         * Todo: We create a SQLexpression here - I think we will introduce a
         * constructor which will take In parameters -- to create different
         * types of nodes --- Localization of code and code reuse -- Presently
         * just doing it here
         */
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;

        SqlExpression leftsqlexpr = exprHelper.argument;

        SqlExpression msqlexpr = new SqlExpression();

        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("SqlExpressionHandler.java  560  : "
                    + msqlexpr.toString());
        }
        // Push the SQL Expression-- Logic Starts
        SqlExpression sqlchild = sCurrent.pop();
        msqlexpr.setExprString(msqlexpr.getExprString() + sqlchild.getExprString());
        sCurrent.push(msqlexpr);
        // Logic Ends --

        msqlexpr.setLeftExpr(leftsqlexpr);


        switch (n.f0.which) {
        case 0/* STAR */:
            /*
             * TODO : I am just coding the obvious parts -- later on i will come
             * here to Fill in more details
             */
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("*");
            break;
        case 1/* DIVIDE */:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("\\");
            break;

        case 2/* MOD */:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("MOD");
            break;

        case 3/* DIV */:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("DIV");
            break;
        case 4/* MODULO */:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("%");
            break;

        case 5/*AND_BITWISE*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("&");
            break;

        case 6/*OR_BITWISE*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("|");
            break;

        case 7/*XOR_BITWISE*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("#");
            break;

        case 8/*SHIFT_LEFT_BITWISE*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("<<");
            break;

        case 9/*SHIFT_RIGHT_BITWISE*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator(">>");
            break;

        case 10/*CONTAINED_WITHIN_OR_EQUALS*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("<<=");
            break;

        case 11/*CONTAINS_OR_EQUALS_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator(">>=");
            break;

        case 12/*GREATER_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator(">");
            break;

        case 13/*GREATER_EQUALS_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator(">=");
            break;

        case 14/*LESS_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("<");
            break;

        case 15/*LESS_EQUALS_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("<=");
            break;

        case 16/*EQUALS_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("=");
            break;

        case 17/*NOT_EQUALS_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("!=");
            break;

        case 18/*NOT_EQUALS_2_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("<>");
            break;
            
        case 19/*GIS_OVERLAPS_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("&&");
            break;
            
        case 20/*GIS_OVERLAPS_RIGHT_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("&>");
            break;
            
        case 21/*GIS_OVERLAPS_LEFT_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("&<");
            break;

        case 22/*GIS_OVERLAPS_BELOW_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("&<|");
            break;
            
        case 23/*GIS_OVERLAPS_ABOVE_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("|&>");
            break;
            
        case 24/*GIS_SAME_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("~=");
            break;
            
        case 25/*GIS_STRICT_BELOW_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("<<|");
            break;
            
        case 26/*GIS_STRICT_ABOVE_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("|>>");
            break;

        case 27/*NOT_BITWISE_ really for the GIS contains operator*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("~");
            break;

        case 28/*ABSOLUTE_ really for the GIS contained operator*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("@");
            break;

        default:
            /*
             * I dont expect any other operand here -- But it comes here TODO:
             * Need to develop an error handling strategy
             */
            break;
        }
        
        
        /*
         * TODO: Check if this call is really required? all it will do is make a call
         * in the end to the Terminals (* or its siblings)
         */
       
        n.f0.accept(this, argu);
        /*
         * We will need to create a new Sql Expression here -- and we will
         * insert this as the right child of the Operand expression just created
         */
        SqlExpression rightSqlExpr = new SqlExpression();
        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("SqlExpressionHandler.java  606  : "
                    + rightSqlExpr.toString());
        }

        // Push the SQL Expression
        sCurrent.push(rightSqlExpr);
        //
        exprHelper.argument = rightSqlExpr;
        n.f1.accept(this, argu);
        /*
         * TODO: I have to check that the sql expression that I get is not null
         * and is not of type Operand.
         */
        msqlexpr.setRightExpr(exprHelper.argument);
        /*
         * Once we have all the required information we will return the
         * SqlExpression in the argument
         */
        exprHelper.argument = msqlexpr;

        // SQL -Child
        sqlchild = sCurrent.pop();
        msqlexpr.setExprString(msqlexpr.getExprString() + sqlchild.getExprString());
        //
        
        return null;
    }

    /**
     * f0 -> ( <PLUS_> | <MINUS_> | <CONCAT_> ) f1 ->
     * SQLMultiplicativeExpression(prn)
     * f2 -> [ <FACTORIAL_> ]
     */
    @Override
    public Object visit(SimpleExpressionOperand n, Object argu) {
        /*
         * This will get a similar treatment as the multiplicativeOperand - I
         * have Just cut and pasted -- Only changes made are to switch statement
         */

        /*
         * We have now reached the terminal point -- A decison will need to be
         * made here regarding the type of expression that we need to make here --
         * Todo: We create a SQLexpression here - I think we will introduce a
         * constructor which will take In parameters -- to create different
         * types of nodes --- Localization of code and code reuse -- Presently
         * just doing it here
         */
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression leftsqlexpr = exprHelper.argument;

        SqlExpression msqlexpr = new SqlExpression();
        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("SqlExpressionHandler.java  650  : "
                    + msqlexpr.toString());
        }

        // -- Push -- Logic Starts
        SqlExpression sqlchild = sCurrent.pop();
        msqlexpr.setExprString(msqlexpr.getExprString() + sqlchild.getExprString());
        sCurrent.push(msqlexpr);
        // Logic --Ends
        msqlexpr.setLeftExpr(leftsqlexpr);

        switch (n.f0.which) {
        case 0/*PLUS */:
            /*
            TODO : I am just coding the obvious parts -- later on i will come here to
            Fill in more details
            */
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("+");
            break;

        case 1/*MINUS*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("-");
            break;

        case 2/*CONCAT*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("||");
            break;

        case 3/*SQUARE_ROOT*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("|/");
            break;

        case 4/*CUBE_ROOT*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("||/");
            break;

        case 5/*FACTORIAL_PREFIX*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("!!");
            break;

        case 6/*ABSOLUTE*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("@");
            break;

        case 7/*BITWISE_NOT_*/:
            msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
            msqlexpr.setOperator("~");
            break;

        default:

            /*
             * I dont expect any other operand here -- But it comes here TODO:
             * Need to develop an error handling strategy
             */
            break;
        }
        /*
         * Check if this call is required? all it will do is make a call
         * in the end to the Terminals (* or its siblings)
         */
        n.f0.accept(this, argu);
        /*
         * We will need to create a new Sql Expression here -- and we will
         * insert this as the right child of the Operand expression just created
         */

        SqlExpression rightSqlExpr = new SqlExpression();
        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("SqlExpressionHandler.java  693  : "
                    + rightSqlExpr.toString());
        }

        // -- Push
        sCurrent.push(rightSqlExpr);
        // --
        exprHelper.argument = rightSqlExpr;
        n.f1.accept(this, argu);
        /*
         * TODO: I have to check that the sql expression that I get is not null
         * and is not of type Operand.
         */
        msqlexpr.setRightExpr(exprHelper.argument);
        /*
         * Once we have all the required information we will return the
         * SqlExpression
         */
        exprHelper.argument = msqlexpr;
        // --- logic
        sqlchild = sCurrent.pop();
        msqlexpr.setExprString(msqlexpr.getExprString() + sqlchild.getExprString());
        // -- Logic
        return null;
    }

    /**
     * f0 -> ( TableName(prn) "." <IDENTIFIER_NAME> | <IDENTIFIER_NAME> ) The
     * above expansion is saved in extendbObject
     */
    @Override
    public Object visit(TableColumn n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        n.f0.accept(this, argu);
       
        return aSqlExpression;
    }

    /**
     * Grammar production:
     * f0 -> ( TableName(prn) "." Identifier(prn) | Identifier(prn) )
     */
    @Override
    public Object visit(extendbObject n, Object argu) {

        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_COLUMN);
        aSqlExpression.setColumn(new AttributeColumn());
        IdentifierHandler ih = new IdentifierHandler();
        switch (n.f0.which) {
        case 0:
            TableNameHandler aTableNameHandler = new TableNameHandler(
                    commandToExecute.getClientContext());
            ((NodeSequence) n.f0.choice).elementAt(0).accept(aTableNameHandler, null);
            String tableName = aTableNameHandler.getTableName();

            // Set the alias name same as that of column name - This
            // will later change to the true alias name if an
            // alias is specified.
            aSqlExpression.getColumn().columnName = (String) ((NodeSequence) n.f0.choice).elementAt(2).accept(ih, argu);
            // We cant decide until we have gone through the from clause
            // whether a particular column expression is infact an alias or
            // is it a table Name , so let us just set the tableAlias

            // Incase this is a table Name we will change it to a tempTable
            // format, else we will let it remain as such.

            aSqlExpression.getColumn().setTableAlias(tableName);
            aSqlExpression.setOuterAlias(aSqlExpression.getColumn().columnName);

            break;
        case 1:
            aSqlExpression.getColumn().columnName = (String) n.f0.choice.accept(ih, argu);
            // We dont know the table Name or the table alias at this point
            // however to keep quorum
            // we should populate these fields and keep them in sycnhronization,
            // until they are forced
            // to move out.
            aSqlExpression.setAlias(aSqlExpression.setOuterAlias(aSqlExpression.getColumn().columnName));

            break;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
        return null;
    }

    /**
     * f0 -> <DECIMAL_LITERAL>
     */
    @Override
    public Object visit(numberValue n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        /*
         * TODO: Shift these lines to the constructor of the SqlExpression --
         * For Code Localization.
         */
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        /*
         * Y - UnaryOperator --Will be taken care of in Y - Constant Value-
         * taken care of above , exprString- taken care of above, isComplete -
         * Take care of at initiallization M- Alias Will be taken care of in the
         * alias Spec
         */
        n.f0.accept(this, argu);
        return null;
    }

    /**
     * f0 -> <DECIMAL_LITERAL>
     */
    @Override
    public Object visit(stringLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        /*
         * TODO: Shift these lines to the constructor of the SqlExpression --
         * For Code Localization.
         */
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        /*
         * Y - UnaryOperator --Will be taken care of in Y - Constant Value-
         * taken care of above , exprString- taken care of above, isComplete -
         * Take care of at initiallization M- Alias Will be taken care of in the
         * alias Spec
         */
        n.f0.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <TEXT_LITERAL>
     */

    @Override
    public Object visit(TextLiterals n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.CLOB_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    /**
     * Grammar production:
     * f0 -> <PLACE_HOLDER>
     */
    @Override
    public Object visit(PreparedStmtParameter n, Object argu) {
        SqlExpression aSqlExpression = getSqlExpression(argu);
        aSqlExpression.setExprType(SqlExpression.SQLEX_PARAMETER);
        aSqlExpression.setExprDataType(new ExpressionType());
        int number = Integer.parseInt(n.f0.tokenImage.substring(1));
        aSqlExpression.setParamNumber(number);
        commandToExecute.registerParameter(number, aSqlExpression);
        return null;
    }

    @Override
    public Object visit(hex_decimalLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.BINARY_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    @Override
    public Object visit(binaryLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        anExprDataType.type = ExpressionType.BINARY_TYPE;
        anExprDataType.length = n.f0.tokenImage.length() - 3; // do not count b''
        aSqlExpression.setExprDataType(anExprDataType);
        return null;
    }

    /**
     * Grammar production: f0 -> <DECIMAL_LITERAL> | <INT_LITERAL> |
     * <SCIENTIFIC_LITERAL>
     */

    @Override
    public Object visit(FloatingPointNumber n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        NodeToken aPlainNumberToken = (NodeToken) n.f0.choice;
        aSqlExpression.setConstantValue(aPlainNumberToken.tokenImage);
        return null;
    }

    private SqlExpression getSqlExpression(Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        if (aSqlExpression == null) {
            aSqlExpression = new SqlExpression();
        }
        return aSqlExpression;
    }

    /**
     * Instead of delegating individual nodes of SQLComplex Expression -- We are
     * delegating the ComplexExpression itself.
     */

    /**
     * Grammar production: f0 -> SQLAndExpression(prn) f1 -> (
     * SQLORExpression(prn) )*
     */

    @Override
    public Object visit(SQLComplexExpression n, Object argu) {
        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);
        SqlExpression sqlExpression = getSqlExpression(argu);
        n.accept(qch, argu);
        QueryCondition qc = qch.aRootCondition;
        // The Query Conditions that we get here will be actually a expressions
        // This will return us a Query Condition -- and Query Condition is of
        // many
        // types
        if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {

            SqlExpression.copy(qc.getExpr(), sqlExpression);
            // --makeExpressionString( sqlExpression.exprString);// --TODO
            // Change this to condstring
            sqlExpression.setExprString("(" + sqlExpression.getExprString());

        } else// Implying it is of type condition
        {

            sqlExpression.setQueryCondition(qc);
            sqlExpression.setExprType(SqlExpression.SQLEX_CONDITION);
            makeExpressionString(qc.getCondString());// This wont Work For Now
        }
        return null;
    }

    @Override
    public Object visit(NodeToken n, Object argu) {
        makeExpressionString(n.tokenImage);
        return null;
    }

    private void makeExpressionString(String exprToken) {
        try {
            SqlExpression sqlExpr = sCurrent.peek();
            sqlExpr.setExprString(sqlExpr.getExprString() + exprToken);
        } catch (Exception ex) {
            // This implies that we have not pushed any expression -- on the
            // stack and we have got some
            // element -- The <STAR_> <IDNAME.star> are caught here
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }

    }

    /* In order to get expression string being added to SQLExpression */
    public void updateQuery(String condition) {

        makeExpressionString(condition);
    }

    public void update(Observable obs, Object obj) {
        updateQuery((String) obj);
    }

    /*
     * We will have to set a variable in the SQL expression which indicated that
     * this is a constant of type boolean
     */

    /**
     * f0 -> <TRUE_> | <FALSE_>
     */
    @Override
    public Object visit(booleanLiteral n, Object argu) {
        n.f0.accept(this, argu);
        NodeToken aChoiceToken = (NodeToken) n.f0.choice;
        SqlExpression sqlExpression = getSqlExpression(argu);
        sqlExpression.setConstantValue(aChoiceToken.tokenImage);
        sqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);

        return null;
    }

    /**
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(NullLiterals n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        // aSqlExpression.constantValue = n.f0.tokenImage;
        aSqlExpression.setConstantValue(null);
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        n.f0.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <INTERVAL_LITERAL>
     */

    @Override
    public Object visit(IntervalLiterals n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        // aSqlExpression.constantValue = n.f0.tokenImage;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        // n.f1.accept(this, argu);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.INTERVAL_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    /**
    * Grammar production:
    * f0 -> <INTEGER_LITERAL>
    */

    @Override
    public Object visit(IntegerLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.INT_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    /**
     * Grammar production: f0 -> <TIMESTAMP_LITERAL>
     */

    @Override
    public Object visit(TimeStampLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                ExpressionType.TIMESTAMPLEN, 0, 0);
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;

    }

    /**
     * Grammar production: f0 -> <TIME_LITERAL>
     */
    @Override
    public Object visit(TimeLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.setExpressionType(ExpressionType.TIME_TYPE,
                ExpressionType.TIMELEN, 0, 0);
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;

    }

    /**
     * Grammar production: f0 -> <DATE_LITERAL>
     */
    @Override
    public Object visit(DateLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.setExpressionType(ExpressionType.DATE_TYPE,
                ExpressionType.DATELEN, 0, 0);
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;

    }

    /**
     * Grammar production: f0 -> <MACADDR_LITERAL>
     */

    @Override
    public Object visit(MacaddrLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.MACADDR_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    /**
     * Grammar production: f0 -> <CIDR_LITERAL>
     */

    @Override
    public Object visit(CidrLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.CIDR_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    /**
     * Grammar production: f0 -> <INET_LITERAL>
     */

    @Override
    public Object visit(InetLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.INET_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    /**
     * Grammar production: f0 -> <GEOMETRY_LITERAL>
     */

    @Override
    public Object visit(GeometryLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.GEOMETRY_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }

    /**
     * Grammar production: f0 -> <BOX2D_LITERAL>
     */

    /*
    @Override
    public Object visit(Box2DLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.BOX2D_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }
*/
    /**
     * Grammar production: f0 -> <BOX3D_LITERAL>
     */

    /*
    @Override
    public Object visit(Box3DLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.BOX3D_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }
*/
    /**
     * Grammar production: f0 -> <BOX3DEXTENT_LITERAL>
     */
/*
    @Override
    public Object visit(Box3DExtentLiteral n, Object argu) {
        ExpressionArgumentHelper exprHelper = (ExpressionArgumentHelper) argu;
        SqlExpression aSqlExpression = exprHelper.argument;
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.BOX3DEXTENT_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return null;
    }
*/

}
