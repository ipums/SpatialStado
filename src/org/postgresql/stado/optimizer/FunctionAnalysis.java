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
package org.postgresql.stado.optimizer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.StringTokenizer;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.engine.io.DataTypes;
import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.NotAlphaNumericException;
import org.postgresql.stado.exception.NotNumericException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.handler.DataTypeHandler;
import org.postgresql.stado.parser.handler.IFunctionID;


/**
 *
 * FunctionAnalysis Class is reponsible for doing the symantic checks for the
 * all the supported functions
 *
 */
public class FunctionAnalysis {

    public static final int SOUNDEXCOLLEN = 4;

    // This is a guess
    public static final int MAX_INET_CIDR_TEXT_LEN = 50;

    /**
     *
     * @return ExpressionType which is returned by this particular expression
     * @param database
     * @param functionExpr The Sql Expression which holds information regarding the
     *            function
     */
    public static ExpressionType analyzeAverageParameter(
            SqlExpression functionExpr, SysDatabase database) {
        // Check if the expression type is really FUNCTION
        if (functionExpr.getExprType() != SqlExpression.SQLEX_FUNCTION) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_EXPRESSION_OBJECT_NULL, 0,
                    ErrorMessageRepository.ILLEGAL_EXPRESSION_OBJECT_NULL_CODE);
        }

        // Get the SQL Arguments -- We expect only one argument for this
        // function - The function is a SET
        // Function and can take any expresson which yields a numeric result.
        ExpressionType expressionType = null;
        if (functionExpr.getFunctionParams().size() > 0) {

            SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);
            expressionType = aSqlExpression
            .setExpressionResultType(aSqlExpression, database);
            if (!expressionType.isNumeric() && !expressionType.isBit()
                    && !expressionType.isCharacter()) {
                throw new XDBServerException(
                        ErrorMessageRepository.INVALID_DATATYPE + "( "
                        + aSqlExpression.getExprString() + " )", 0,
                        ErrorMessageRepository.INVALID_DATATYPE_CODE);
            }

            ExpressionType expressionTypeToReturn = new ExpressionType();

            switch (expressionType.type) {
            case ExpressionType.SMALLINT_TYPE:
            case ExpressionType.INT_TYPE:
            case ExpressionType.BIGINT_TYPE:
                expressionTypeToReturn.setExpressionType(
                        ExpressionType.NUMERIC_TYPE, 32,
                        0, 0);
                break;

            case ExpressionType.FLOAT_TYPE:
                expressionTypeToReturn.setExpressionType(
                        ExpressionType.DOUBLEPRECISION_TYPE, 32, 0, 0);
                break;

            default:
                expressionTypeToReturn = expressionType;
            }
            return expressionTypeToReturn;

        }
        return expressionType;
    }

    /**
     *
     * @return ExpressionType which is returned by this particular expression
     * @param functionExpr The Sql Expression which holds information regarding the
     *            function
     */

    public static ExpressionType analyzeCountParameter(
            SqlExpression functionExpr) {
        ExpressionType aExpressionType = new ExpressionType();
        aExpressionType.setExpressionType(
                ExpressionType.BIGINT_TYPE, ExpressionType.BIGINTLEN,
                0, 0);
        return aExpressionType;
    }

    /**
     * Function Signature Input - any expression Output - INT
     *
     * @return ExpressionType which is returned by this particular expression
     * @param functionExpr The Sql Expression which holds information regarding the
     *            function
     */
    public static ExpressionType analyzeLengthParameter(
            SqlExpression functionExpr) {
        // No analysis required.
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprType;
    }

    // ----Date and Time Function Parameter Analysis

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeDateTruncParameter(
            SqlExpression functionExpr) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                ExpressionType.TIMESTAMPLEN, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeAgeParameter(SqlExpression functionExpr) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.INTERVAL_TYPE, -1, -1, -1);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeDatePartParameter(
            SqlExpression functionExpr) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeExtractParameter(
            SqlExpression functionExpr) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprType;
    }

    /**
     *
     * @return
     * @param functionExpr
     */

    public static ExpressionType analyzeIsFiniteParameter(
            SqlExpression functionExpr) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeNowParameter(SqlExpression functionExpr) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                ExpressionType.TIMESTAMPLEN, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeTimeOfDayParameter(
            SqlExpression functionExpr) {

        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.VARCHAR_TYPE, 50, 0, 0);
        return exprType;
    }

    /**
     * These functions take one parameter and return INT Input - TIME/TIMESTAMP
     * OutPut- INT
     *
     * @return ExpressionType which is returned by this particular expression
     * @param database
     * @param functionExpr The Sql Expression which holds information regarding the
     *            function
     */
    public static ExpressionType analyzeHour_Min_SecParameter(
            SqlExpression functionExpr, SysDatabase database) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);

        if (isTime(aSqlExpression, database)
                || isTimeStamp(aSqlExpression, database)) {
            exprType.setExpressionType(ExpressionType.INT_TYPE, 10, 0, 0);
        } else {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP
                    + " ( " + functionExpr.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP_CODE);
        }

        return exprType;
    }

    /**
     * Input - TIME/TIMESTAMP , TIME/TIMESTAMP Output- TIME/TIMESTAMP
     *
     * @return ExpressionType which is returned by this particular expression
     * @param database
     * @param functionExpr The Sql Expression which holds information regarding the
     *            function
     */
    public static ExpressionType analyzeAddTime_SubTimeParameter(
            SqlExpression functionExpr, SysDatabase database) {
        SqlExpression aSqlExpression;
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);

        if (isTime(aSqlExpression2, database) == true) {
            aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
            ExpressionType exprType = new ExpressionType();

            if (isTime(aSqlExpression, database)) {
                exprType.setExpressionType(ExpressionType.TIME_TYPE,
                        ExpressionType.TIMELEN, 0, 0);
                return exprType;

            } else if (isTimeStamp(aSqlExpression, database)) {
                exprType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                        ExpressionType.TIMESTAMPLEN, 0, 0);
                return exprType;

            }

            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP
                    + " ( " + aSqlExpression.rebuildString() + " ) ",
                    0,
                    ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP_CODE);

        }
        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP + " ( "
                + aSqlExpression2.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP_CODE);

    }

    /**
     * Input : Date /TimeStamp, INT OutPut : Date/ TimeStamp
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     */
    public static ExpressionType analyzeAddDate_SubDateParameter(
            SqlExpression functionExpr, SysDatabase database) {
        SqlExpression aSqlExpression;
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if (aSqlExpression2.getExprDataType() == null) {
            aSqlExpression2.setExpressionResultType(aSqlExpression2, database);
        }
        if (aSqlExpression2.getExprDataType().isNumeric()) {
            aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
        } else {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                    + functionExpr.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
        }

        ExpressionType aExpressionType = new ExpressionType();

        if (isDate(aSqlExpression, database)) {

            aExpressionType.setExpressionType(ExpressionType.DATE_TYPE,
                    ExpressionType.DATELEN, 0, 0);
            return aExpressionType;

        }
        if (isTimeStamp(aSqlExpression, database)) {
            aExpressionType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                    ExpressionType.TIMESTAMPLEN, 0, 0);
            return aExpressionType;
        }

        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP_CODE);

    }

    /**
     * Input : Date/ TimeStamp OutPut : Date
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     */

    public static ExpressionType analyzeDateParameter(
            SqlExpression functionExpr, SysDatabase database) {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);
        if (isDate(aSqlExpression, database)
                || isTimeStamp(aSqlExpression, database)) {
            ExpressionType aExpressionType = new ExpressionType();
            aExpressionType.setExpressionType(ExpressionType.DATE_TYPE,
                    ExpressionType.DATELEN, 0, 0);
            return aExpressionType;
        }

        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP_CODE);

    }
    /**
     * Input: VARCHAR OutPut: TIMESTAMP
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     */
    public static ExpressionType analyzeToDateParameter(
            SqlExpression functionExpr, SysDatabase database) {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if (aSqlExpression1.getExprDataType() == null
                || aSqlExpression1.getExprDataType().type == 0) {
            aSqlExpression1.setExpressionResultType(aSqlExpression1, database);
        }
        if(aSqlExpression1.getExprType() == SqlExpression.SQLEX_CONSTANT
                && aSqlExpression1.getExprDataType().type != ExpressionType.VARCHAR_TYPE
                && aSqlExpression1.getExprDataType().type != ExpressionType.CHAR_TYPE
                && aSqlExpression1.getExprDataType().type != ExpressionType.DATE_TYPE
                && aSqlExpression1.getExprDataType().type != ExpressionType.TIMESTAMP_TYPE) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }

        if (!isCharacter(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as TIMESTAMP
        ExpressionType aExpressionType = new ExpressionType();
        aExpressionType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                ExpressionType.TIMESTAMPLEN, 0, 0);
        return aExpressionType;


    }

    /**
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     */
    public static ExpressionType analyzeDateDiff(SqlExpression functionExpr,
            SysDatabase database) {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(1);
        if (isDate(aSqlExpression, database)
                || isTimeStamp(aSqlExpression, database)) {
            if (isDate(aSqlExpression1, database)
                    || isTimeStamp(aSqlExpression1, database)) {
                ExpressionType aExpressionType = new ExpressionType();
                aExpressionType.setExpressionType(ExpressionType.FLOAT_TYPE,
                        32, 0, 0);
                return aExpressionType;
            }
        }
        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP_CODE);
    }

    /**
     * Input :Date/TimeStamp Output:CHAR
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */

    public static ExpressionType analyzeDayName_MonthName_Parameter(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);

        if (isDate(aSqlExpression, database)
                || isTimeStamp(aSqlExpression, database)) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.CHAR_TYPE,
                    ExpressionType.MONTHORDAYNAME, 0, 0);
            return exprT;
        }
        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_DATE_TIMESTAMP + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_DATE_TIMESTAMP_CODE);
    }

    /**
     * Input : Date/TimeStamp Output:Int
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */

    public static ExpressionType analyzeDayOfMonth_DayOfWeek_DayOfYear_Month_Year_WeekOfYear_Parameter(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);

        if (isDate(aSqlExpression, database)
                || isTimeStamp(aSqlExpression, database)) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.INT_TYPE, 10, 0, 0);
            return exprT;
        }

        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_DATE_TIMESTAMP + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_DATE_TIMESTAMP_CODE);
    }

    /**
     * Input : TimeStamp OutPut : TimeStamp Or Input : Time , Date Output :
     * TimeStamp
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */

    public static ExpressionType analyzeTimeStamp(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        int size = functionExpr.getFunctionParams().size();
        ExpressionType exprT = new ExpressionType();

        if (size == 1) {
            SqlExpression aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
            if (isTimeStamp(aSqlExpression, database)) {
                exprT.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                        ExpressionType.TIMESTAMPLEN, 0, 0);
                return exprT;
            } else {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_DATE_TIMESTAMP
                        + " ( " + aSqlExpression.rebuildString()
                        + " ) ",
                        0,
                        ErrorMessageRepository.EXPRESSION_NOT_DATE_TIMESTAMP_CODE);
            }
        } else if (size == 2) {
            SqlExpression aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
            SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
            .get(1);
            if (isDate(aSqlExpression, database)) {
                if (isTime(aSqlExpression2, database)) {
                    exprT.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                            ExpressionType.TIMESTAMPLEN, 0, 0);
                    return exprT;
                } else {
                    throw new XDBServerException(
                            ErrorMessageRepository.EXPRESSION_NOT_TIME + " ( "
                            + aSqlExpression.rebuildString() + " ) ",
                            0, ErrorMessageRepository.EXPRESSION_NOT_TIME_CODE);
                }
            } else {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_DATE + " ( "
                        + aSqlExpression.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_DATE_CODE);

            }
        } else {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
    }

    /**
     * Input : Time, TimeStamp Output : Time
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */
    public static ExpressionType analyzeTime(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {

        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);

        if (isTime(aSqlExpression, database)
                || isTimeStamp(aSqlExpression, database)) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.DATE_TYPE,
                    ExpressionType.DATELEN, 0, 0);
            return exprT;
        }

        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_TIME_TIMESTAMP_CODE);
    }

    // ----------------End of Time- Date Functions
    // ----------------------------------------------------------

    // -----------------Arthematic Functions Start
    // here-------------------------------------------------------
    /**
     * Input : Any Output: Same as input
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */
    public static ExpressionType analyzeMax_MinParameter(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {
        // Only one parameter expected
        SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);

        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }
        if (aSqlExpression.getExprDataType().type == ExpressionType.INTERVAL_TYPE) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 50, 0, 0);
            return exprT;
        }

        return aSqlExpression.getExprDataType();

    }

    /**
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */
    public static ExpressionType analyzeAbsParameter(
            SqlExpression functionExpr, SysDatabase database)
    throws XDBServerException

    {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);

        if (isNumeric(aSqlExpression, database)) {
            return aSqlExpression.getExprDataType();
        }

        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
    }

    /**
     * Functions which take in Input - Numeric Type OutPut - Float Throw -
     * Exception on AlphaNumeric Type NumberOfParameterAllowed - 1
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */
    public static ExpressionType analyzeSumParameter(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {
        // Check if the expression type is really FUNCTION

        if (functionExpr.getExprType() != SqlExpression.SQLEX_FUNCTION) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_EXPRESSION_OBJECT_NULL, 0,
                    ErrorMessageRepository.ILLEGAL_EXPRESSION_OBJECT_NULL_CODE);

        }

        // Get the SQL Arguments -- We expect only one argument for this
        // function - The function is a SET
        // Function and can take any expresson which yields a numeric result.
        ExpressionType expressionTypeToReturn = new ExpressionType();
        ExpressionType expressionType = null;

        if (functionExpr.getFunctionParams().size() > 0) {
            SqlExpression aSqlExpression = functionExpr.getFunctionParams().get(0);
            expressionType = aSqlExpression
            .setExpressionResultType(aSqlExpression, database);
            if (!expressionType.isNumeric() && !expressionType.isBit()
                    && !expressionType.isCharacter()) {
                throw new XDBServerException(
                        ErrorMessageRepository.INVALID_DATATYPE + " ( "
                        + aSqlExpression.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.INVALID_DATATYPE_CODE);
            }

            switch (expressionType.type) {
            case ExpressionType.SMALLINT_TYPE:
            case ExpressionType.INT_TYPE:
                expressionTypeToReturn.setExpressionType(
                        ExpressionType.BIGINT_TYPE, ExpressionType.BIGINTLEN,
                        0, 0);
                break;

            case ExpressionType.BIGINT_TYPE:
                expressionTypeToReturn.setExpressionType(
                        ExpressionType.NUMERIC_TYPE, 32,
                        0, 0);
                break;

            case ExpressionType.FLOAT_TYPE:
                expressionTypeToReturn.setExpressionType(
                        ExpressionType.DOUBLEPRECISION_TYPE, 32, 0, 0);
                break;

            case ExpressionType.BOOLEAN_TYPE:
                expressionTypeToReturn.setExpressionType(
                        ExpressionType.INT_TYPE, 0, 0, 0);
                break;

            default:
                expressionTypeToReturn = expressionType;
            }
            return expressionTypeToReturn;
        }
        return expressionType;
    }

    /**
     * Input - Numeric Type OutPut - INT
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */

    public static ExpressionType analyzeCeil_Floor_Sign(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException

    {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);

        if (isNumeric(aSqlExpression, database)) {
            return aSqlExpression.getExprDataType();
        }

        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
    }

    /**
     * INPUT - Numeric Type Output - DECIMAL
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */
    public static ExpressionType analyzeExp_LN_POWER_ASIN_ATAN_COS_COT_DEGREES_RADIANS_SIN_TAN_ACOS_LOG10_SQRT_COSH(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {
        SqlExpression aSqlExpression = functionExpr.getFunctionParams()
        .get(0);

        if (isNumeric(aSqlExpression, database)) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
            return exprT;
        }

        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                + aSqlExpression.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
    }

    /**
     * Input - Numeric , Numeric Output - Float
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */

    public static ExpressionType analyzeLog_Float_ATAN2(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {

        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if (!isNumeric(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
        }

        if (isNumeric(aSqlExpression2, database)) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
            return exprT;
        }
        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                + aSqlExpression2.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
    }

    /**
     * Input - Numeric , Numeric or Null Output - Float
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws IllegalArgumentException
     *             Incase the argument type does not match the signature of the
     *             function
     * @throws ColumnNotFoundException
     */

    public static ExpressionType analyzeTrunc(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {

        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = null;
        if (functionExpr.getFunctionParams().size() == 2) {
            aSqlExpression2 = functionExpr.getFunctionParams()
            .get(1);
        }

        if (!isNumeric(aSqlExpression1, database) &&
                !isMacaddr(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_MACADDR + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_MACADDR_CODE);
        }

        if (aSqlExpression2 == null || isNumeric(aSqlExpression2, database)) {
            ExpressionType exprT = new ExpressionType();
            if (isMacaddr(aSqlExpression1, database)) {
                exprT.setExpressionType(ExpressionType.MACADDR_TYPE, -1, -1, -1);
            }
            else {
                exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
            }
            return exprT;
        }
        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                + aSqlExpression2.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
    }
    /**
     *
     * Input: sqlExpr1, sqlExpr2 Output: Null/sqlExpr1
     *
     * @param functionExpr
     *
     * @param database
     *
     * @throws org.postgresql.stado.exception.ColumnNotFoundException
     *
     * @return
     *
     */
    public static ExpressionType analyzeNullIf(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {

        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        //set the return type same as that of sqlExpr1
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(aSqlExpression1.getExprDataType().type
                , aSqlExpression1.getExprDataType().length, aSqlExpression1.getExprDataType().precision
                , aSqlExpression1.getExprDataType().scale);
        return exprT;
    }

    /**
     *
     * Input: VARCHAR, INT, INT Output: BYTEA
     *
     * @param functionExpr
     *
     * @param database
     *
     * @throws org.postgresql.stado.exception.ColumnNotFoundException
     *
     * @return
     *
     */
    public static ExpressionType analyzeSetBitByte(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        SqlExpression aSqlExpression3 = functionExpr.getFunctionParams()
        .get(2);
        if(!isCharacter(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isNumeric(aSqlExpression2,database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
        }
        if(!isNumeric(aSqlExpression3,database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                    + aSqlExpression3.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
        }
        //set the return type as BYTEA
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.BLOB_TYPE, -1, 0, 0);
        return exprT;
    }
    /**
     *
     * Input: VARCHAR, INT Output: INT
     *
     * @param functionExpr
     *
     * @param database
     *
     * @throws org.postgresql.stado.exception.ColumnNotFoundException
     *
     * @return
     *
     */
    public static ExpressionType analyzeGetBitByte(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if(!isCharacter(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isNumeric(aSqlExpression2,database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
        }
        //set the return type as INT
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, ExpressionType.INTLEN, 0, 0);
        return exprT;
    }
    /**
     * Input:DATE/INT/DOUBLE/NUMERIC, VARCHAR Output: VARCHAR
     */
    public static ExpressionType analyzeToChar(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        /**
         * we dont want the type of first sqlExpr as other than
         * DATE/INT/DOUBLE/NUMERIC
         */
        if(!isDate(aSqlExpression1, database) && !isTimeStamp(aSqlExpression1, database)) {
            if(!isNumeric(aSqlExpression1, database)) {
                throw new XDBServerException(
                        ErrorMessageRepository.INVALID_DATATYPE + " ( "
                        + aSqlExpression1.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.INVALID_DATATYPE_CODE);
            }
        }
        if(!isCharacter(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as VARCHAR
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
        return exprT;
    }

    /**
     * Input:VARCHAR, VARCHAR Output: NUMERIC
     */
    public static ExpressionType analyzeToNumber(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if(!isCharacter(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isCharacter(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as NUMERIC
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.NUMERIC_TYPE, ExpressionType.INTLEN, 5, 0);
        return exprT;
    }

    /**
     * Input:TIMESTAMP, NUMERIC Output: TIMESTAMP
     */
    public static ExpressionType analyzeAddMonth(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if(!isTime(aSqlExpression1, database) && !isTimeStamp(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isNumeric(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as TIMESTAMP
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.TIMESTAMP_TYPE, ExpressionType.TIMESTAMPLEN, 0, 0);
        return exprT;
    }

    /**
     * Input:INTERVAL Output: INTERVAL
     */
    public static ExpressionType analyzeJustify(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        functionExpr.getFunctionParams()
        .get(0);
        /*if(!isTime(aSqlExpression1, database) && !isTimeStamp(aSqlExpression1, database)) {
             throw new XDBServerException(
                     ErrorMessageRepository.INVALID_DATATYPE + " ( "
                     + aSqlExpression1.rebuildString() + " ) ", 0,
                     ErrorMessageRepository.INVALID_DATATYPE_CODE);
         } */
        //set the return type as TIMESTAMP
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.TIMESTAMP_TYPE, ExpressionType.TIMESTAMPLEN, 0, 0);
        return exprT;
    }

    /**
     * Input:TIMESTAMP, TIMESTAMP Output: NUMERIC
     */
    public static ExpressionType analyzeMonthsBetween(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if(!isTime(aSqlExpression1, database) && !isTimeStamp(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isTime(aSqlExpression2, database) && !isTimeStamp(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as NUMERIC
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.NUMERIC_TYPE, ExpressionType.INTLEN, 5, 0);
        return exprT;
    }

    /**
     * Input:TIMESTAMP, TEXT Output: TIMESTAMP
     */
    public static ExpressionType analyzeNextDay(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if(!isTime(aSqlExpression1, database) && !isTimeStamp(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isCharacter(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as TIMESTAMP
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.TIMESTAMP_TYPE, ExpressionType.TIMESTAMPLEN, 0, 0);
        return exprT;
    }

    /**
     * Input:TEXT, TEXT, TEXT, [TEXT] Output: TEXT
     */
    public static ExpressionType analyzeRegexpReplace(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        SqlExpression aSqlExpression3 = functionExpr.getFunctionParams()
        .get(2);
        if(!isCharacter(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isCharacter(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isCharacter(aSqlExpression3, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression3.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(functionExpr.getFunctionParams().size() == 4) {
            SqlExpression aSqlExpression4 = functionExpr.getFunctionParams()
            .get(3);
            if(!isCharacter(aSqlExpression4, database)) {
                throw new XDBServerException(
                        ErrorMessageRepository.INVALID_DATATYPE + " ( "
                        + aSqlExpression4.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.INVALID_DATATYPE_CODE);
            }
        }
        //set the return type as VARCHAR
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
        return exprT;
    }

    /**
     * Input: sqlExpr Output: type of sqlExpr
     */
    public static ExpressionType analyzeBitAnd(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {

        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        if(!isNumeric(aSqlExpression1, database) && !isBit(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(aSqlExpression1.getExprDataType() == null) {
            aSqlExpression1.setExprDataType(aSqlExpression1.setExpressionResultType(aSqlExpression1, database));
        }
        //set the return type same as that of sqlExpr
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(aSqlExpression1.getExprDataType().type
                , aSqlExpression1.getExprDataType().length, aSqlExpression1.getExprDataType().precision
                , aSqlExpression1.getExprDataType().scale);
        return exprT;

    }

    /**
     * Input: BOOLEAN Output: BOOLEAN
     */
    public static ExpressionType analyzeBoolAnd(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {

        functionExpr.getFunctionParams()
        .get(0);
        //set the return type as BOOLEAN
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, -1, -1, -1);
        return exprT;

    }
    /**
     *
     * @param functionExpr
     * @param database
     * @throws org.postgresql.stado.exception.ColumnNotFoundException
     * @return
     */

    public static ExpressionType analyzeLeft_Right(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {

        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if (isNumeric(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
        }

        if (isNumeric(aSqlExpression2, database)) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
            return exprT;
        }
        throw new XDBServerException(
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                + aSqlExpression2.rebuildString() + " ) ", 0,
                ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
    }


    /**
     *
     * Inout : Any Variable numeric/alpha numeric Out : Any Variable
     * numeric/alpha numeric
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws ColumnNotFoundException
     *             Note : We are not checking all the parameters in the list
     *             only the first this will mean that we allow the underlying
     *             database to take care of any errors that might arise due to
     *             numeric and alpha numeric being present in the same list
     */
    public static ExpressionType analyzeGreatest_Least(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {
        // We need to check whether all the expression in the parameter list
        // have the same
        // type -- This check is ultimately done by the UnderLying db - So let
        // us leave it here

        // If numeric - retun FLOAT
        // If Alpha Numeric - Return Char - Of length Max
        SqlExpression parameter = functionExpr.getFunctionParams()
        .get(0);
        if (isNumeric(parameter, database)) {
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 38, 38, 0);
            return exprT;
        } else {
            ExpressionType exprT = new ExpressionType();
            int maxLenghtStr = 0;
            for (SqlExpression aSqlExpr : functionExpr.getFunctionParams()) {
                int length = aSqlExpr.getExprDataType().length;
                if (length > maxLenghtStr) {
                    maxLenghtStr = length;
                }
            }
            exprT.setExpressionType(ExpressionType.CHAR_TYPE, maxLenghtStr, 0,
                    0);
            return exprT;
        }

    }

    /**
     *
     * Inout : Any data-type Out : Any data-type
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws ColumnNotFoundException
     *             Note : We are not checking all the parameters in the list
     *             only the first this will mean that we allow the underlying
     *             database to take care of any errors that might arise due to
     *             numeric and alpha numeric being present in the same list
     */
    public static ExpressionType analyzeCoalesce(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {
        // If numeric - try and determine type to return
        ExpressionType returnType = null;
        // If Alpha Numeric - Return Char - Of length Max
        // Zahid: since the first (and sub-sequent) param could be null, iterate over
        // all params unless a non-null param is found or whole list is iterated
        for (SqlExpression parameter : functionExpr.getFunctionParams()) {
            if (parameter.getExprDataType() != null && parameter.getExprDataType().type != 0) {
                if (isNumeric(parameter, database)) {
                    if (returnType == null) {
                        returnType = parameter.getExprDataType();
                    } else {
                        returnType = ExpressionType.MergeNumericTypes(
                                returnType, parameter.getExprDataType());
                    }
                }
                else if ( isDate(parameter, database) ||
                        isTimeStamp(parameter, database)) {
                    ExpressionType aExpressionType = new ExpressionType();
                    aExpressionType.setExpressionType(ExpressionType.DATE_TYPE,
                            ExpressionType.DATELEN, 0, 0);
                    return aExpressionType;
                }
                else {
                    ExpressionType exprT = new ExpressionType();
                    int maxLenghtStr = 0;
                    for (SqlExpression aSqlExpr : functionExpr.getFunctionParams()) {
                        int length = aSqlExpr.getExprDataType().length;
                        if (length > maxLenghtStr) {
                            maxLenghtStr = length;
                        }
                    }
                    exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, maxLenghtStr,
                            0, 0);
                    return exprT;
                }
            }
        }
        return returnType;
    }

    /**
     *
     * Input - Numeric | DOUBLE PRECISION | TIMESTAMP [, Numeric | TEXT] Output - FLOAT | TIMESTAMP
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     * @throws ColumnNotFoundException
     */

    public static ExpressionType analyzeRound(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        boolean expr1IsNumeric = isNumeric(aSqlExpression1, database);
        boolean expr1IsDate = false;

        if (!expr1IsNumeric) {
            expr1IsDate = isTimeStamp(aSqlExpression1, database) || isDate(aSqlExpression1, database);
        }

        if (!expr1IsNumeric && !expr1IsDate) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }

        if (functionExpr.getFunctionParams().size() == 2) {
            SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
            .get(1);

            if(expr1IsDate) {
                exprT.setExpressionType(ExpressionType.TIMESTAMP_TYPE, ExpressionType.TIMESTAMPLEN, 0, 0);
                return exprT;
            }

            boolean expr2IsNumeric = isNumeric(aSqlExpression2, database);

            if (expr2IsNumeric == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                        + aSqlExpression2.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
            }
        }

        if (expr1IsNumeric) {
            switch (aSqlExpression1.getExprDataType().type) {
            case ExpressionType.REAL_TYPE:
            case ExpressionType.FLOAT_TYPE:
            case ExpressionType.DOUBLEPRECISION_TYPE:
                exprT.setExpressionType(
                        ExpressionType.DOUBLEPRECISION_TYPE, 32, 0, 0);
                break;

            default:
                exprT.setExpressionType(ExpressionType.NUMERIC_TYPE, 32, 0, 0);
            }
        } else {
            exprT.setExpressionType(ExpressionType.TIMESTAMP_TYPE, ExpressionType.TIMESTAMPLEN, 0, 0);
        }

        return exprT;
    }

    /**
     * Input - None Output - Float
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */
    public static ExpressionType analyzePI(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeSetSeed(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 10, 10, 0);
        return exprT;
    }

    // ---------------------String Function Start here
    // --------------------------------------------------

    /**
     * Input - Alpanumeric Output - Numeric Float type
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     *
     * We will let the under lying database decide on this one -- just check to
     * make sure that we have @ least one parameter -- The parser takes care of
     * that
     *
     */

    public static ExpressionType analyzeNUM(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeMod(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        if (functionExpr.getExprDataType() == null) {
            exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
        } else {
            exprT = functionExpr.getExprDataType();
        }

        // typecast inExactNumeric mod parameters to numeric
        // EDB does not support this. This is a hack for Microstrategy
        for (int j = 0; j < functionExpr.getFunctionParams().size(); j++) {
            SqlExpression  ase = functionExpr.getFunctionParams().get(j);
            if (ase.getExprDataType().isInExactNumeric()) {
                SqlExpression newSqlExpression = new SqlExpression();
                newSqlExpression.setExprType(SqlExpression.SQLEX_FUNCTION);
                newSqlExpression.setFunctionId(IFunctionID.CAST_ID);
                newSqlExpression.setFunctionName("::");
                newSqlExpression.getFunctionParams().add(ase);
                int length = ase.getExprDataType().length;
                // It would be better if we cast to NUMERIC without precision/scale (max possible)
                // but this is not supported by our templates and there is a compatibility issue
                newSqlExpression.setExpTypeOfCast(new DataTypeHandler(Types.NUMERIC, 0, 2 * length, length));
                functionExpr.getFunctionParams().set(j, newSqlExpression);
            }
        }

        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @return
     */

    public static ExpressionType analyzeVarianceOrStddev(
            SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        if (functionExpr.getFunctionParams().get(0).getExprType() == ExpressionType.FLOAT_TYPE) {
            exprT.setExpressionType(ExpressionType.DOUBLEPRECISION_TYPE, 32, 0,
                    0);
        } else {
            exprT.setExpressionType(ExpressionType.NUMERIC_TYPE, 32, 0, 0);
        }
        return exprT;
    }
    /**
     * Input: double precision, double precision Output: double precision
     */
    public static ExpressionType analyzeCoRegFunc(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if(!isNumeric(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isNumeric(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as DOUBLE PRECISION
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.DOUBLEPRECISION_TYPE, 32, 0, 0);
        return exprT;
    }

    /**
     * Input: double precision, double precision Output: bigint
     */
    public static ExpressionType analyzeRegrCount(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
        .get(1);
        if(!isNumeric(aSqlExpression1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(!isNumeric(aSqlExpression2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExpression2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        //set the return type as BIGINT
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.BIGINT_TYPE, 32, 0, 0);
        return exprT;
    }

    /**
     * Input - AlphaNumeric OutPut - Alpha Numeric
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     *
     * Notes : We cannot have a max value returned as the database has a
     * restriction on the row size of the database.- Therefore while creating
     * database tables this will fail if the any column size is already the max
     * size
     */

    public static ExpressionType analyzeAscii_Upper_Lower_Soundex_InitCap(
            SqlExpression functionExpr, SysDatabase database) {

        ExpressionType exprT = new ExpressionType();

        if (functionExpr.getFunctionId() == IFunctionID.SOUNDEX_ID) {

            exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, SOUNDEXCOLLEN,
                    10, 0);
        } else {
            if (functionExpr.getFunctionId() == IFunctionID.LOWER_ID
                    || functionExpr.getFunctionId() == IFunctionID.UPPER_ID
                    || functionExpr.getFunctionId() == IFunctionID.INITCAP_ID
                    || functionExpr.getFunctionId() == IFunctionID.ASCII_ID) {
                // Incase of upper, Lower , Lfill and Tail Concat
                SqlExpression functionparam = functionExpr.getFunctionParams()
                .get(0);
                if (isNumeric(functionparam, database)) {
                    convertNumericToChar(functionparam);
                }
                // For Upper and Lower clearly we have to return the lenght as
                // the
                // length of the columns
                exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                        functionparam.getExprDataType().length, 10, 0);
            }

        }

        return exprT;
    }

    /**
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression.
     *         It returns a VARCHAR data type
     *
     * Note : Mapchar can have different parameters. For
     * eg. MAPCHAR (X,N,I) or MAPCHAR(X,I) or MAPCHAR(x) - In this case n is the
     * length of the column X if N is not specified
     *
     */
    public static ExpressionType analyzeMapchar(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        if (functionExpr.getFunctionId() == IFunctionID.MAPCHAR_ID) {
            int numParam = functionExpr.getFunctionParams().size();
            int requiredLength = 0;
            SqlExpression aSqlExpression = null;
            switch (numParam) {
            case 0:
                // There are no parameters
                throw new XDBServerException(
                        ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                        ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);

            case 1:
                aSqlExpression = functionExpr.getFunctionParams()
                .get(0);
                if (isNumeric(aSqlExpression, database)) {
                    convertNumericToChar(aSqlExpression);
                }
                requiredLength = aSqlExpression.getExprDataType().length;
                exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                        requiredLength, 0, 0);
                break;

            case 2:
                // One parameter, incase we have 2 parameters
                aSqlExpression = functionExpr.getFunctionParams()
                .get(1);
                aSqlExpression.rebuildExpression();
                if (aSqlExpression.getExprDataType().type == ExpressionType.CHAR_TYPE) {
                    aSqlExpression = functionExpr.getFunctionParams()
                    .get(0);
                    if (isNumeric(aSqlExpression, database)) {
                        convertNumericToChar(aSqlExpression);
                    }
                    requiredLength = aSqlExpression.getExprDataType().length;
                    exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                            requiredLength, 0, 0);

                } else {
                    aSqlExpression = functionExpr.getFunctionParams()
                    .get(1);
                    requiredLength = Integer
                    .parseInt(aSqlExpression.getConstantValue());
                    exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                            requiredLength, 0, 0);

                }
                break;
            case 3:
                // Incase we have 3 parameters then we need send the second
                // parameters
                // length
                aSqlExpression = functionExpr.getFunctionParams()
                .get(1);
                requiredLength = Integer.parseInt(aSqlExpression.getConstantValue());
                exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                        requiredLength, 0, 0);
                break;
            default:
                // Will never reach here - The parser wont let it
                throw new XDBServerException(
                        ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                        ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
            }
        }
        return exprT;
    }

    /**
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     *
     * Notes : Lfill can be called with 3 paramerters or 2 parameters
     * 1.lfill(x,a,n) 2.lfill(x,a)
     */
    public static ExpressionType analyzeLfill(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        if (functionExpr.getFunctionId() == IFunctionID.LFILL_ID) {
            int numParam = functionExpr.getFunctionParams().size();
            switch (numParam) {
            case 0:
            case 1:
                // There are no parameters
                throw new XDBServerException(
                        ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                        ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);

            case 3:
                // Incase we have 3 parameters then we need send the second
                // parameters
                // length
                SqlExpression aSqlExpression3 = functionExpr.getFunctionParams()
                .get(2);
                if (isNumeric(aSqlExpression3, database) == false) {

                    throw new XDBServerException(
                            ErrorMessageRepository.EXPRESSION_NOT_NUMERIC
                            + " ( " + aSqlExpression3.rebuildString()
                            + " ) ", 0,
                            ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);

                    // throw new XDBSemanticException("The parameter " +
                    // aSqlExpression3.rebuildString() + "should" +
                    // "be numeric in expression " +
                    // functionExpr.rebuildString() );
                }
            case 2:
                // One parameter, incase we have 2 parameters
                SqlExpression aSqlExpression = functionExpr.getFunctionParams()
                .get(0);
                if (isNumeric(aSqlExpression, database)) {
                    convertNumericToChar(aSqlExpression);
                }
                int requiredLength = aSqlExpression.getExprDataType().length;
                exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                        requiredLength, 0, 0);
                break;
            default:
                throw new XDBServerException(
                        ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                        ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);

            }
        }
        return exprT;
    }

    /**
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     *
     * Does symantic analysis for CONCAT This function only works with 2
     * parameters
     */
    public static ExpressionType analyzeConcat(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExp2 = functionExpr.getFunctionParams()
        .get(1);
        try {
            if (isNumeric(aSqlExp1, database)) {
                convertNumericToChar(aSqlExp1);
            }
            if (isNumeric(aSqlExp2, database)) {
                convertNumericToChar(aSqlExp2);
            }
        } catch (XDBServerException xdbex) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }

        int requiredLength = aSqlExp1.getExprDataType().length
        + aSqlExp2.getExprDataType().length;
        exprT.setExpressionType(ExpressionType.CHAR_TYPE, requiredLength, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeStrPos(SqlExpression functionExpr,
            SysDatabase database) {
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams().get(0);
        SqlExpression aSqlExp2 = functionExpr.getFunctionParams().get(1);

        if (!isCharacter(aSqlExp1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExp1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if (!isCharacter(aSqlExp2, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExp2.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeBtrim(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);

        int requiredLength = aSqlExp1.getExprDataType().length;

        exprT.setExpressionType(ExpressionType.CHAR_TYPE, requiredLength + 1,
                0, 0);
        return exprT;
    }

    /**
     * Input VARCHAR [, VARCHAR] Output: VARCHAR
     */
    public static ExpressionType analyzeToASCII(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);
        if(!isCharacter(aSqlExp1, database)) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                    + aSqlExp1.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
        if(functionExpr.getFunctionParams().size() == 2) {
            SqlExpression aSqlExp2 = functionExpr.getFunctionParams()
            .get(1);
            if(!isCharacter(aSqlExp2, database)) {
                throw new XDBServerException(
                        ErrorMessageRepository.INVALID_DATATYPE + " ( "
                        + aSqlExp2.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.INVALID_DATATYPE_CODE);
            }
        }

        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, aSqlExp1.getExprDataType().length, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeChr(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();

        exprT.setExpressionType(ExpressionType.CHAR_TYPE, 2, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeConvert(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);

        int requiredLength = aSqlExp1.getExprDataType().length;

        exprT.setExpressionType(ExpressionType.CHAR_TYPE,
                requiredLength * 2 + 1, 0, 0);
        return exprT;
    }

    /**
     *
     * Input: any type, any type, ....../ VARCHAR, VARCHAR Output: any tpye/ BYTEA
     *
     * @param functionExpr
     *
     * @param database
     *
     * @return
     *
     */

    public static ExpressionType analyzeDecode(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);
        //for decode(VARCHAR, VARCHAR)
        if(functionExpr.getFunctionParams().size() == 2) {
            //set the return type as BYTEA
            exprT.setExpressionType(ExpressionType.BLOB_TYPE, -1, 0, 0);
            return exprT;
        } else {
            //for decode(value0, value1, value1a [, value2, value2a,.... default]
            for(int i=1; i<functionExpr.getFunctionParams().size(); i=i+2) {

                if(aSqlExp1.getExprType() == SqlExpression.SQLEX_CONSTANT && aSqlExp1.getConstantValue()
                        .equalsIgnoreCase(functionExpr.getFunctionParams().get(i).getConstantValue())) {
                    SqlExpression targetExpr = functionExpr.getFunctionParams().get(++i);
                    //set the return type as that of matched sqlExpression (targetExpr)
                    exprT.setExpressionType(targetExpr.getExprDataType().type, targetExpr.getExprDataType().length,
                            targetExpr.getExprDataType().precision, targetExpr.getExprDataType().scale);
                    return exprT;
                }
            }
        }
        SqlExpression defaultExpr = functionExpr.getFunctionParams().get(functionExpr.getFunctionParams().size() - 1);
        //set the return type as that of last sqlExpr
        exprT.setExpressionType(defaultExpr.getExprDataType().type, defaultExpr.getExprDataType().length,
                defaultExpr.getExprDataType().precision, defaultExpr.getExprDataType().scale);
        return exprT;
    }

    /**
     *
     * Input: BYTEA, TEXT Output: TEXT
     *
     * @param functionExpr
     *
     * @param database
     *
     * @return
     *
     */

    public static ExpressionType analyzeEncode(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeMd5(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();

        exprT.setExpressionType(ExpressionType.CHAR_TYPE, 32, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeQuote(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);

        int requiredLength = aSqlExp1.getExprDataType().length;

        exprT.setExpressionType(ExpressionType.CHAR_TYPE, requiredLength + 3,
                0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeRepeat(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExp1 = functionExpr.getFunctionParams()
        .get(0);
        SqlExpression aSqlExp2 = functionExpr.getFunctionParams()
        .get(1);

        int requiredLength = aSqlExp1.getExprDataType().length;
        if (aSqlExp2.getExprType() == SqlExpression.SQLEX_CONSTANT) {
            requiredLength = aSqlExp1.getExprDataType().length
            * Integer.parseInt(aSqlExp2.getConstantValue());
        } else {
            requiredLength = 256;
        }
        exprT.setExpressionType(ExpressionType.CHAR_TYPE, requiredLength, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeToHex(SqlExpression functionExpr,
            SysDatabase database) {
        ExpressionType exprT = new ExpressionType();

        exprT.setExpressionType(ExpressionType.CHAR_TYPE, 16, 0, 0);
        return exprT;
    }

    /**
     * This function will perform the conversion from numeric to the charachter.
     */
    public static void convertNumericToChar(SqlExpression aSqlExpression) {
        switch (aSqlExpression.getExprDataType().type) {
        case ExpressionType.SMALLINT_TYPE:
        case ExpressionType.INT_TYPE:
        case ExpressionType.BIT_TYPE:
            aSqlExpression.getExprDataType().length = 11;
            aSqlExpression.getExprDataType().type = ExpressionType.CHAR_TYPE;
            break;
        case ExpressionType.FLOAT_TYPE:
        case ExpressionType.REAL_TYPE:
            aSqlExpression.getExprDataType().length = aSqlExpression.getExprDataType().length + 6;
            aSqlExpression.getExprDataType().type = ExpressionType.CHAR_TYPE;
            break;
        case ExpressionType.DECIMAL_TYPE:
        case ExpressionType.NUMERIC_TYPE:
            aSqlExpression.getExprDataType().length = aSqlExpression.getExprDataType().precision + 2;
            aSqlExpression.getExprDataType().type = ExpressionType.CHAR_TYPE;
            break;
        case ExpressionType.DOUBLEPRECISION_TYPE:
            aSqlExpression.getExprDataType().length = 33;
            aSqlExpression.getExprDataType().type = ExpressionType.CHAR_TYPE;
            break;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + "( "
                    + aSqlExpression.getExprDataType().type + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);
        }
    }

    /**
     *
     * Input : Any , AlphaNumeric
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */

    public static ExpressionType analyzeLtrim_Rtirm_Trim(
            SqlExpression functionExpr, SysDatabase database)
    throws ColumnNotFoundException {

        /*
         * We dont have to check the expression type of the first parameter
         */
        if (functionExpr.getFunctionParams().size() == 2) {
            SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
            .get(1);

            if (isAlphaNumeric(aSqlExpression2, database) == false) {

                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC
                        + " ( " + aSqlExpression2.rebuildString()
                        + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);
            }
        }
        ExpressionType exprT = new ExpressionType();
        SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
        .get(0);
        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                aSqlExpression1.getExprDataType().length, 0, 0);
        return exprT;
    }

    /**
     * Input :
     *
     * @least 2 parameters and
     * @most 4 parameters Output : AlphaNumeric
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     *
     * Forwarding the error handling to the database
     */

    public static ExpressionType analyzeIndex_InStr(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        int params = functionExpr.getFunctionParams().size();

        switch (params) {
        case 0:
        case 1:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        case 4:
            SqlExpression aSqlExpression3 = functionExpr.getFunctionParams()
            .get(3);
            if (isNumeric(aSqlExpression3, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                        + aSqlExpression3.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);

            }
        case 3:
            SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
            .get(2);
            if (isNumeric(aSqlExpression2, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                        + aSqlExpression2.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);

            }
        case 2:
            SqlExpression aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
            SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
            .get(1);
            if (isAlphaNumeric(aSqlExpression, database)) {

                if (isAlphaNumeric(aSqlExpression1, database)) {
                    ExpressionType exprT = new ExpressionType();
                    exprT.setExpressionType(ExpressionType.INT_TYPE, 10, 10, 0);
                    return exprT;
                } else {
                    throw new XDBServerException(
                            ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC
                            + " ( " + aSqlExpression1.rebuildString()
                            + " ) ",
                            0,
                            ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);

                }
            } else {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC
                        + " ( " + aSqlExpression.rebuildString()
                        + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);

            }

        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);

        }

    }

    /**
     * This function handles signatures of LPAD and RPAD
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     *
     */
    public static ExpressionType analyzeLpad_Rpad(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        int params = functionExpr.getFunctionParams().size();
        int n = 0;
        int a = 0;

        switch (params) {
        case 0:
        case 1:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        case 4:
            SqlExpression aSqlExpression3 = functionExpr.getFunctionParams()
            .get(3);
            if (aSqlExpression3.getConstantValue() != null) {
                n = Integer.parseInt(aSqlExpression3.getConstantValue());
            } else if (isNumeric(aSqlExpression3, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                        + aSqlExpression3.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
            }
        case 3:
            SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
            .get(2);
            if (isAlphaNumeric(aSqlExpression2, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC
                        + " ( " + aSqlExpression2.rebuildString()
                        + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);
            }
            a = aSqlExpression2.getExprDataType().length;
        case 2:
            SqlExpression aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
            SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
            .get(1);
            if (isAlphaNumeric(aSqlExpression, database) == false) {
                convertNumericToChar(aSqlExpression);
            }
            if (isNumeric(aSqlExpression1, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC + " ( "
                        + aSqlExpression.rebuildString() + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_NUMERIC_CODE);
            }

            int lengthx = aSqlExpression.getExprDataType().length;
            int lenghty = Integer.parseInt(aSqlExpression1.getConstantValue());
            int requiredLength = 0;
            if (n != 0) {
                requiredLength = n;
            } else {
                requiredLength = lengthx + a * lenghty;
            }

            if (isAlphaNumeric(aSqlExpression, database)
                    && isNumeric(aSqlExpression1, database)) {
                ExpressionType exprT = new ExpressionType();
                exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                        requiredLength, 0, 0);
                return exprT;
            }
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);

        }
    }

    /**
     * This function handles the signature of Replace
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */
    public static ExpressionType analyzeReplace(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        int params = functionExpr.getFunctionParams().size();

        switch (params) {
        case 0:
        case 1:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        case 3:
            SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
            .get(2);
            if (isAlphaNumeric(aSqlExpression2, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC
                        + " ( " + aSqlExpression2.rebuildString()
                        + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);
            }
        case 2:
            SqlExpression aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
            SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
            .get(1);
            if (isAlphaNumeric(aSqlExpression, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC
                        + " ( " + aSqlExpression.rebuildString()
                        + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);
            }

            if (isAlphaNumeric(aSqlExpression1, database) == false) {
                throw new XDBServerException(
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC
                        + " ( " + aSqlExpression1.rebuildString()
                        + " ) ", 0,
                        ErrorMessageRepository.EXPRESSION_NOT_ALPHANUMERIC_CODE);
            }

            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                    aSqlExpression1.getExprDataType().length
                    * aSqlExpression1.getExprDataType().length, 0, 0);
            return exprT;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
    }

    /**
     * Handles the various syntax of SubString
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */
    public static ExpressionType analyzeSubString(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        int params = functionExpr.getFunctionParams().size();

        switch (params) {
        case 0:
        case 1:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);

        case 3:
            SqlExpression aSqlExpression2 = functionExpr.getFunctionParams()
            .get(2);
            if (isNumeric(aSqlExpression2, database) == false) {
                throw new NotNumericException(aSqlExpression2, functionExpr);
            }
        case 2:
            SqlExpression aSqlExpression = functionExpr.getFunctionParams()
            .get(0);
            SqlExpression aSqlExpression1 = functionExpr.getFunctionParams()
            .get(1);
            if (isAlphaNumeric(aSqlExpression, database) == false) {
                throw new NotAlphaNumericException(aSqlExpression1,
                        functionExpr);
            }
            if (isNumeric(aSqlExpression1, database) == false) {
                throw new NotNumericException(aSqlExpression1, functionExpr);
            }
            ExpressionType exprT = new ExpressionType();
            exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                    aSqlExpression.getExprDataType().length, 0, 0);
            return exprT;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);

        }
    }

    /**
     * Analyzes the functions for length
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */
    public static ExpressionType analyzeLength(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 10, 0, 0);
        return exprT;
    }

    /* --- Misc - Functions Start here Input - none Output - char */
    /**
     * Analyzes version - TODO This is to be implemented by
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */
    public static ExpressionType analyzeDatabase_Version_User(
            SqlExpression functionExpr) {
        // No parameters for this function
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE,
                ExpressionType.VERSION, 0, 0);
        return exprT;
    }

    /**
     * Handles value function
     *
     * @param functionExpr
     *            The Sql Expression which holds information regarding the
     *            function
     * @return ExpressionType which is returned by this particular expression
     */
    public static ExpressionType analyzeValue(SqlExpression functionExpr,
            SysDatabase database) throws ColumnNotFoundException {
        for (SqlExpression aSqlExpression : functionExpr.getFunctionParams()) {
            if (aSqlExpression.getExprDataType() == null) {
                aSqlExpression
                .setExpressionResultType(aSqlExpression, database);
            }
        }
        ExpressionType exprT = new ExpressionType();
        if (functionExpr.getFunctionParams().size() > 0) {
            exprT.setExpressionType(functionExpr.getFunctionParams().get(functionExpr.getFunctionParams().size() - 1).getExprDataType().type, 10, 0, 0);
        } else {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeCustom(SqlExpression functionExpr,
            SysDatabase database) {
        String function;
        for (int overloadid = 0;; overloadid++) {
            function = functionExpr.getFunctionName()
            + (overloadid == 0 ? "" : "," + overloadid);
            // Check paramcount
            String paramCountStr = Property.get("xdb.sqlfunction." + function
                    + ".paramcount");
            if (paramCountStr != null) {
                int idx = paramCountStr.indexOf("+");
                if (idx > 0) {
                    try {
                        int paramCount = Integer.parseInt(paramCountStr
                                .substring(0, idx).trim());
                        if (functionExpr.getFunctionParams().size() < paramCount) {
                            continue;
                        }
                    } catch (NumberFormatException nfe) {
                        // Ignore, do not check
                    }
                } else {
                    boolean match = false;
                    StringTokenizer st = new StringTokenizer(paramCountStr, "|");
                    while (st.hasMoreTokens()) {
                        String aCase = st.nextToken();
                        try {
                            int paramCount = Integer.parseInt(aCase.trim());
                            if (functionExpr.getFunctionParams().size() == paramCount) {
                                match = true;
                                break;
                            }
                        } catch (NumberFormatException nfe) {
                            // Ignore, do not check
                        }
                    }
                    if (!match) {
                        continue;
                    }
                }
            } else {
                if (overloadid > 0) {
                    throw new XDBServerException(
                            ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                            ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
                }
            }
            // Check param types
            int count = 0;
            boolean match = true;
            for (SqlExpression sqlexpr : functionExpr.getFunctionParams()) {
                count++;
                if (sqlexpr.getExprDataType() == null) {
                    sqlexpr.setExpressionResultType(sqlexpr, database);
                }
                if (sqlexpr.getExprDataType().type != ExpressionType.NULL_TYPE) {
                    String typeStr = DataTypes
                    .getJavaTypeDesc(sqlexpr.getExprDataType().type);
                    String paramTypeStr = Property.get("xdb.sqlfunction."
                            + function + ".arg" + count, typeStr);
                    paramTypeStr = paramTypeStr
                    .toUpperCase()
                    .replaceAll(
                            "ANYCHAR", "CHAR|VARCHAR|LONGVARCHAR|CLOB")
                    .replaceAll("ANYDATETIME", "DATE|TIME|TIMESTAMP")
                    .replaceAll("ANYNUMBER",
                    "ANYINT|FLOAT|REAL|DOUBLE PRECISION|NUMERIC|DECIMAL")
                    .replaceAll("ANYINT",
                    "BYTE|SMALLINT|INTEGER|BIGINT");
                    if (paramTypeStr.indexOf(typeStr) < 0) {
                        match = false;
                        break;
                    }
                }
            }
            if (match) {
                break;
            }
        }
        ExpressionType exprT = new ExpressionType();
        String paramTypeStr = Property.get("xdb.sqlfunction." + function
                + ".returntype");
        if (paramTypeStr != null) {
            int i = paramTypeStr.indexOf("(");
            int javaType;
            int length = -1;
            int precision = -1;
            int scale = -1;
            if (i < 0) {
                javaType = DataTypes.getJavaType(paramTypeStr);
            } else {
                javaType = DataTypes.getJavaType(paramTypeStr.substring(0, i)
                        .trim());
                switch (javaType) {
                case Types.VARCHAR:
                case Types.FLOAT:
                case Types.REAL:
                    // parse length
                    int j = paramTypeStr.indexOf(")", i + 1);
                    if (j > i + 1) {
                        try {
                            length = Integer.parseInt(paramTypeStr.substring(
                                    i + 1, j).trim());
                        } catch (NumberFormatException nfe) {
                        }
                    }
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    // parse precision and scale
                    j = paramTypeStr.indexOf(")", i + 1);
                    if (j > i + 1) {
                        int k = paramTypeStr.indexOf(",", i + 1);
                        if (k > i + 1 && k + 1 < j) {
                            try {
                                precision = Integer.parseInt(paramTypeStr
                                        .substring(i + 1, k).trim());
                            } catch (NumberFormatException nfe) {
                            }
                            try {
                                scale = Integer.parseInt(paramTypeStr
                                        .substring(k + 1, j).trim());
                            } catch (NumberFormatException nfe) {
                            }
                        } else {
                            try {
                                precision = Integer.parseInt(paramTypeStr
                                        .substring(i + 1, j).trim());
                            } catch (NumberFormatException nfe) {
                            }
                            scale = 0;
                        }
                    }
                    break;
                default:
                }
            }
            exprT.setExpressionType(javaType, length, precision, scale);
        }
        return exprT;
    }

    // ----------------------HELPER
    // FUMCTIONS-------------------------------------------------------------
    /**
     * Helper function , which determines if the particular expression is a
     * TimeStamp or not
     *
     * @param aSqlExpression
     *            SQLExpression to check
     * @return returns true if the column or value is timestamp
     * @throws ColumnNotFoundException
     */

    private static boolean isTimeStamp(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {
        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }
        if (aSqlExpression.getExprDataType().type == ExpressionType.TIMESTAMP_TYPE) {
            return true;
        } else {

            if (aSqlExpression.getExprDataType().isNumeric()) {
                return false;
            } else {
                try {
                    Timestamp.valueOf(aSqlExpression.getConstantValue().replaceAll(
                            "'", ""));
                    return true;
                } catch (Exception ex) {
                    return false;
                }
            }
        }

    }

    /**
     *
     * @param aSqlExpression
     * @return
     * @throws ColumnNotFoundException
     *
     * This function determines whether the constant value is of type TIME.It
     * uses java.sql.time to check for the format.TODO: May be we will have to
     * change this if we support various formats
     */
    private static boolean isTime(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {
        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }

        if (aSqlExpression.getExprDataType().type == ExpressionType.TIME_TYPE) {
            return true;
        } else {

            if (aSqlExpression.getExprDataType().isNumeric()) {
                return false;
            } else {
                try {
                    Time.valueOf(aSqlExpression.getConstantValue().replaceAll("'",
                    ""));
                    return true;
                } catch (Exception ex) {
                    return false;

                }
            }
        }

    }

    /**
     * Checks to see if the expression is a date
     *
     * @param aSqlExpression
     *            The SQLExpression to check
     * @return If the expression is a date the it returns true
     * @throws ColumnNotFoundException
     */

    private static boolean isDate(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {
        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }
        if (aSqlExpression.getExprDataType().type == ExpressionType.DATE_TYPE) {
            return true;
        }
        {
            if (aSqlExpression.getExprDataType().isNumeric()) {
                return false;
            } else {
                try {
                    Date.valueOf(aSqlExpression.getConstantValue().replaceAll("'",
                    ""));
                    return true;
                } catch (Exception ex) {
                    return false;
                }
            }

        }

    }

    /**
     * Check if the expression is numeric
     *
     * @param aSqlExpression
     *            Expression to check
     * @return TRUE if the expression is numeric
     * @throws ColumnNotFoundException
     */

    private static boolean isNumeric(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {
        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }

        return aSqlExpression.getExprDataType().isNumeric();
    }

    /**
     * Check if the expression is bit
     *
     * @param aSqlExpression
     *            Expression to check
     * @return TRUE if the expression is bit
     * @throws ColumnNotFoundException
     */

    private static boolean isBit(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {
        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }

        return aSqlExpression.getExprDataType().isBit();
    }

    /**
     * Check if the expression is String
     *
     * @param aSqlExpression
     *            Expression to check
     * @return TRUE if the expression is String
     * @throws ColumnNotFoundException
     */

    private static boolean isCharacter(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {
        if (aSqlExpression.getExprDataType() == null
                || aSqlExpression.getExprDataType().type == 0) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }
        if(aSqlExpression.getExprType() == SqlExpression.SQLEX_CONSTANT
                && aSqlExpression.getExprDataType().type != ExpressionType.VARCHAR_TYPE
                && aSqlExpression.getExprDataType().type != ExpressionType.CHAR_TYPE) {
            return false;
        }
        return true;
    }

    /**
     * Checks to see if the expression is a alpha numeric
     *
     * @param aSqlExpression
     *            Expression to Check
     * @return true if the expression is alpha numeric
     * @throws ColumnNotFoundException
     */

    private static boolean isAlphaNumeric(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {

        if (aSqlExpression.getExprDataType() == null) {
            aSqlExpression.setExpressionResultType(aSqlExpression, database);
        }
        if (aSqlExpression.getExprDataType().isNumeric()) {
            convertNumericToChar(aSqlExpression);
        } else {
            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CONSTANT) {

                try {
                    Integer.parseInt(aSqlExpression.rebuildString());
                } catch (Exception ex) {
                    return true;
                }

                return false;
            }
        }
        return true;
    }

    /**
     * Checks to see if the expression is a macaddr type
     *
     * @param aSqlExpression
     *            Expression to Check
     * @return true if the expression is macaddr
     * @throws ColumnNotFoundException
     */

    private static boolean isMacaddr(SqlExpression aSqlExpression,
            SysDatabase database) throws ColumnNotFoundException {
        if (aSqlExpression.getExprDataType().type == ExpressionType.MACADDR_TYPE) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check to see if the function is a group function
     *
     * @param functionId
     *            The function id of the function
     * @return true if it is a group function
     */
    public static boolean isGroupFunction(int functionId) {
        if (functionId == IFunctionID.COUNT_ID
                || functionId == IFunctionID.SUM_ID
                || functionId == IFunctionID.MAX_ID
                || functionId == IFunctionID.MIN_ID
                || functionId == IFunctionID.AVG_ID
                || functionId == IFunctionID.STDEV_ID
                || functionId == IFunctionID.STDEVPOP_ID
                || functionId == IFunctionID.STDEVSAMP_ID
                || functionId == IFunctionID.VARIANCE_ID
                || functionId == IFunctionID.VARIANCEPOP_ID
                || functionId == IFunctionID.VARIANCESAMP_ID
                || functionId == IFunctionID.BOOLAND_ID
                || functionId == IFunctionID.EVERY_ID
                || functionId == IFunctionID.BOOLOR_ID
                || functionId == IFunctionID.BITAND_ID
                || functionId == IFunctionID.BITOR_ID) {
            return true;
        } else {
            return false;
        }
    }

    public static ExpressionType analyzeCurrDateTime(SqlExpression functionExpr) {
        // There are no paramters for this function
        ExpressionType exprType = new ExpressionType();
        exprType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                ExpressionType.TIMESTAMPLEN, 0, 0);
        return exprType;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeAbbrev(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.CHAR_TYPE,
                MAX_INET_CIDR_TEXT_LEN, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeBroadcast(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INET_TYPE, -1, -1, -1);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeFamily(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeHost(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.CHAR_TYPE,
                MAX_INET_CIDR_TEXT_LEN, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeHostmask(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INET_TYPE, -1, -1, -1);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeMasklen(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeNetmask(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INET_TYPE, -1, -1, -1);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeNetwork(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.CIDR_TYPE, -1, -1, -1);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeSet_Masklen(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        /**
         * set_masklen has two forms
         * 1) set_masklen(inet, int): return type inet
         * 2) set_masklen(cidr, int): return type cidr
         * Set the return type as the type of the first argument
         */
        SqlExpression aSqlExp = functionExpr.getFunctionParams().get(0);
        ExpressionType paramType = aSqlExp.getExprDataType();
        if (paramType == null || paramType.type != ExpressionType.INET_TYPE
                && paramType.type != ExpressionType.CIDR_TYPE) {
            throw new XDBServerException(
                    ErrorMessageRepository.INVALID_DATATYPE + " ( "
                            + aSqlExp.rebuildString() + " ) ", 0,
                    ErrorMessageRepository.INVALID_DATATYPE_CODE);

        }
        exprT.setExpressionType(paramType.type, paramType.length,
                paramType.precision, paramType.scale);
        return exprT;
    }

    /**
     *
     * @param functionExpr
     * @param database
     * @return
     */

    public static ExpressionType analyzeText(SqlExpression functionExpr) {
        ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.CHAR_TYPE,
                MAX_INET_CIDR_TEXT_LEN, 0, 0);
        return exprT;
    }

}
