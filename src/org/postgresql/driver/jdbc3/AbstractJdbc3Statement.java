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
package org.postgresql.driver.jdbc3;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.Vector;

import org.postgresql.driver.core.BaseConnection;
import org.postgresql.driver.core.Field;
import org.postgresql.driver.core.QueryExecutor;
import org.postgresql.driver.core.Utils;
import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

/**
 * This class defines methods of the jdbc3 specification.  This class extends
 * org.postgresql.driver.jdbc2.AbstractJdbc2Statement which provides the jdbc2
 * methods.  The real Statement class (for jdbc2) is org.postgresql.driver.jdbc3.Jdbc3Statement
 */
public abstract class AbstractJdbc3Statement extends org.postgresql.driver.jdbc2.AbstractJdbc2Statement
{
    private final int rsHoldability;

    public AbstractJdbc3Statement (AbstractJdbc3Connection c, int rsType, int rsConcurrency, int rsHoldability) throws SQLException
    {
        super(c, rsType, rsConcurrency);
        this.rsHoldability = rsHoldability;
    }

    public AbstractJdbc3Statement(AbstractJdbc3Connection connection, String sql, boolean isCallable, int rsType, int rsConcurrency, int rsHoldability) throws SQLException
    {
        super(connection, sql, isCallable, rsType, rsConcurrency);
        this.rsHoldability = rsHoldability;
    }

    /**
     * Moves to this <code>Statement</code> object's next result, deals with
     * any current <code>ResultSet</code> object(s) according  to the instructions
     * specified by the given flag, and returns
     * <code>true</code> if the next result is a <code>ResultSet</code> object.
     *
     * <P>There are no more results when the following is true:
     * <PRE>
     *  <code>(!getMoreResults() && (getUpdateCount() == -1)</code>
     * </PRE>
     *
     * @param current one of the following <code>Statement</code>
     *    constants indicating what should happen to current
     *    <code>ResultSet</code> objects obtained using the method
     *    <code>getResultSet</code:
     *    <code>CLOSE_CURRENT_RESULT</code>,
     *    <code>KEEP_CURRENT_RESULT</code>, or
     *    <code>CLOSE_ALL_RESULTS</code>
     * @return <code>true</code> if the next result is a <code>ResultSet</code>
     *     object; <code>false</code> if it is an update count or there are no
     *     more results
     * @exception SQLException if a database access error occurs
     * @since 1.4
     * @see #execute
     */
    public boolean getMoreResults(int current) throws SQLException
    {
        // CLOSE_CURRENT_RESULT
        if (current == Statement.CLOSE_CURRENT_RESULT && result != null && result.getResultSet() != null)
            result.getResultSet().close();

        // Advance resultset.
        if (result != null)
            result = result.getNext();

        // CLOSE_ALL_RESULTS
        if (current == Statement.CLOSE_ALL_RESULTS)
        {
            // Close preceding resultsets.
            while (firstUnclosedResult != result)
            {
                if (firstUnclosedResult.getResultSet() != null)
                    firstUnclosedResult.getResultSet().close();
                firstUnclosedResult = firstUnclosedResult.getNext();
            }
        }

        // Done.
        return (result != null && result.getResultSet() != null);
    }

    /**
     * Retrieves any auto-generated keys created as a result of executing this
     * <code>Statement</code> object. If this <code>Statement</code> object did
     * not generate any keys, an empty <code>ResultSet</code>
     * object is returned.
     *
     * @return a <code>ResultSet</code> object containing the auto-generated key(s)
     *     generated by the execution of this <code>Statement</code> object
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public ResultSet getGeneratedKeys() throws SQLException
    {
        checkClosed();
        if (generatedKeys == null || generatedKeys.getResultSet() == null)
            return createDriverResultSet(new Field[0], new Vector());

        return generatedKeys.getResultSet();
    }

    /**
     * Executes the given SQL statement and signals the driver with the
     * given flag about whether the
     * auto-generated keys produced by this <code>Statement</code> object
     * should be made available for retrieval.
     *
     * @param sql must be an SQL <code>INSERT</code>, <code>UPDATE</code> or
     *    <code>DELETE</code> statement or an SQL statement that
     *    returns nothing
     * @param autoGeneratedKeys a flag indicating whether auto-generated keys
     *    should be made available for retrieval;
     *     one of the following constants:
     *     <code>Statement.RETURN_GENERATED_KEYS</code>
     *     <code>Statement.NO_GENERATED_KEYS</code>
     * @return either the row count for <code>INSERT</code>, <code>UPDATE</code>
     *     or <code>DELETE</code> statements, or <code>0</code> for SQL
     *     statements that return nothing
     * @exception SQLException if a database access error occurs, the given
     *     SQL statement returns a <code>ResultSet</code> object, or
     *     the given constant is not one of those allowed
     * @since 1.4
     */
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
            return executeUpdate(sql);

        sql = addReturning(connection, sql, new String[]{"*"}, false);
        wantsGeneratedKeysOnce = true;

        return executeUpdate(sql);
    }

    static String addReturning(BaseConnection connection, String sql, String columns[], boolean escape) throws SQLException
    {
        if (!connection.haveMinimumServerVersion("8.2"))
            throw new PSQLException(GT.tr("Returning autogenerated keys is only supported for 8.2 and later servers."), PSQLState.NOT_IMPLEMENTED);

        sql = sql.trim();
        if (sql.endsWith(";"))
            sql = sql.substring(0, sql.length()-1);

        StringBuffer sb = new StringBuffer(sql);
        sb.append(" RETURNING ");
        for (int i=0; i<columns.length; i++) {
            if (i != 0)
                sb.append(", ");
            // If given user provided column names, quote and escape them.
            // This isn't likely to be popular as it enforces case sensitivity,
            // but it does match up with our handling of things like
            // DatabaseMetaData.getColumns and is necessary for the same
            // reasons.
            if (escape)
                Utils.appendEscapedIdentifier(sb, columns[i]);
            else
                sb.append(columns[i]);
        }

        return sb.toString();
    }

    /**
     * Executes the given SQL statement and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval.  The driver will ignore the array if the SQL statement
     * is not an <code>INSERT</code> statement.
     *
     * @param sql an SQL <code>INSERT</code>, <code>UPDATE</code> or
     *    <code>DELETE</code> statement or an SQL statement that returns nothing,
     *    such as an SQL DDL statement
     * @param columnIndexes an array of column indexes indicating the columns
     *    that should be returned from the inserted row
     * @return either the row count for <code>INSERT</code>, <code>UPDATE</code>,
     *     or <code>DELETE</code> statements, or 0 for SQL statements
     *     that return nothing
     * @exception SQLException if a database access error occurs or the SQL
     *     statement returns a <code>ResultSet</code> object
     * @since 1.4
     */
    public int executeUpdate(String sql, int columnIndexes[]) throws SQLException
    {
        if (columnIndexes == null || columnIndexes.length == 0)
            return executeUpdate(sql);

        throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."), PSQLState.NOT_IMPLEMENTED);
    }

    /**
     * Executes the given SQL statement and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval.  The driver will ignore the array if the SQL statement
     * is not an <code>INSERT</code> statement.
     *
     * @param sql an SQL <code>INSERT</code>, <code>UPDATE</code> or
     *    <code>DELETE</code> statement or an SQL statement that returns nothing
     * @param columnNames an array of the names of the columns that should be
     *    returned from the inserted row
     * @return either the row count for <code>INSERT</code>, <code>UPDATE</code>,
     *     or <code>DELETE</code> statements, or 0 for SQL statements
     *     that return nothing
     * @exception SQLException if a database access error occurs
     *
     * @since 1.4
     */
    public int executeUpdate(String sql, String columnNames[]) throws SQLException
    {
        if (columnNames == null || columnNames.length == 0)
            return executeUpdate(sql);

        sql = addReturning(connection, sql, columnNames, true);
        wantsGeneratedKeysOnce = true;

        return executeUpdate(sql);
    }

    /**
     * Executes the given SQL statement, which may return multiple results,
     * and signals the driver that any
     * auto-generated keys should be made available
     * for retrieval.  The driver will ignore this signal if the SQL statement
     * is not an <code>INSERT</code> statement.
     * <P>
     * In some (uncommon) situations, a single SQL statement may return
     * multiple result sets and/or update counts.  Normally you can ignore
     * this unless you are (1) executing a stored procedure that you know may
     * return multiple results or (2) you are dynamically executing an
     * unknown SQL string.
     * <P>
     * The <code>execute</code> method executes an SQL statement and indicates the
     * form of the first result.  You must then use the methods
     * <code>getResultSet</code> or <code>getUpdateCount</code>
     * to retrieve the result, and <code>getMoreResults</code> to
     * move to any subsequent result(s).
     *
     * @param sql any SQL statement
     * @param autoGeneratedKeys a constant indicating whether auto-generated
     *    keys should be made available for retrieval using the method
     *    <code>getGeneratedKeys</code>; one of the following constants:
     *    <code>Statement.RETURN_GENERATED_KEYS</code> or
     *    <code>Statement.NO_GENERATED_KEYS</code>
     * @return <code>true</code> if the first result is a <code>ResultSet</code>
     *     object; <code>false</code> if it is an update count or there are
     *     no results
     * @exception SQLException if a database access error occurs
     * @see #getResultSet
     * @see #getUpdateCount
     * @see #getMoreResults
     * @see #getGeneratedKeys
     *
     * @since 1.4
     */
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
            return execute(sql);

        sql = addReturning(connection, sql, new String[]{"*"}, false);
        wantsGeneratedKeysOnce = true;

        return execute(sql);
    }

    /**
     * Executes the given SQL statement, which may return multiple results,
     * and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval.  This array contains the indexes of the columns in the
     * target table that contain the auto-generated keys that should be made
     * available. The driver will ignore the array if the given SQL statement
     * is not an <code>INSERT</code> statement.
     * <P>
     * Under some (uncommon) situations, a single SQL statement may return
     * multiple result sets and/or update counts.  Normally you can ignore
     * this unless you are (1) executing a stored procedure that you know may
     * return multiple results or (2) you are dynamically executing an
     * unknown SQL string.
     * <P>
     * The <code>execute</code> method executes an SQL statement and indicates the
     * form of the first result.  You must then use the methods
     * <code>getResultSet</code> or <code>getUpdateCount</code>
     * to retrieve the result, and <code>getMoreResults</code> to
     * move to any subsequent result(s).
     *
     * @param sql any SQL statement
     * @param columnIndexes an array of the indexes of the columns in the
     *    inserted row that should be  made available for retrieval by a
     *    call to the method <code>getGeneratedKeys</code>
     * @return <code>true</code> if the first result is a <code>ResultSet</code>
     *     object; <code>false</code> if it is an update count or there
     *     are no results
     * @exception SQLException if a database access error occurs
     * @see #getResultSet
     * @see #getUpdateCount
     * @see #getMoreResults
     *
     * @since 1.4
     */
    public boolean execute(String sql, int columnIndexes[]) throws SQLException
    {
        if (columnIndexes == null || columnIndexes.length == 0)
            return execute(sql);

        throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."), PSQLState.NOT_IMPLEMENTED);
    }

    /**
     * Executes the given SQL statement, which may return multiple results,
     * and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval. This array contains the names of the columns in the
     * target table that contain the auto-generated keys that should be made
     * available. The driver will ignore the array if the given SQL statement
     * is not an <code>INSERT</code> statement.
     * <P>
     * In some (uncommon) situations, a single SQL statement may return
     * multiple result sets and/or update counts.  Normally you can ignore
     * this unless you are (1) executing a stored procedure that you know may
     * return multiple results or (2) you are dynamically executing an
     * unknown SQL string.
     * <P>
     * The <code>execute</code> method executes an SQL statement and indicates the
     * form of the first result.  You must then use the methods
     * <code>getResultSet</code> or <code>getUpdateCount</code>
     * to retrieve the result, and <code>getMoreResults</code> to
     * move to any subsequent result(s).
     *
     * @param sql any SQL statement
     * @param columnNames an array of the names of the columns in the inserted
     *    row that should be made available for retrieval by a call to the
     *    method <code>getGeneratedKeys</code>
     * @return <code>true</code> if the next result is a <code>ResultSet</code>
     *     object; <code>false</code> if it is an update count or there
     *     are no more results
     * @exception SQLException if a database access error occurs
     * @see #getResultSet
     * @see #getUpdateCount
     * @see #getMoreResults
     * @see #getGeneratedKeys
     *
     * @since 1.4
     */
    public boolean execute(String sql, String columnNames[]) throws SQLException
    {
        if (columnNames == null || columnNames.length == 0)
            return execute(sql);

        sql = addReturning(connection, sql, columnNames, true);
        wantsGeneratedKeysOnce = true;

        return execute(sql);
    }

    /**
      * Retrieves the result set holdability for <code>ResultSet</code> objects
      * generated by this <code>Statement</code> object.
      *
      * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
      *   <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
      * @exception SQLException if a database access error occurs
      *
      * @since 1.4
      */
    public int getResultSetHoldability() throws SQLException
    {
        return rsHoldability;
    }

    /**
     * Sets the designated parameter to the given <code>java.net.URL</code> value.
     * The driver converts this to an SQL <code>DATALINK</code> value
     * when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the <code>java.net.URL</code> object to be set
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public void setURL(int parameterIndex, java.net.URL x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setURL(int,URL)");
    }

    /**
     * Retrieves the number, types and properties of this
     * <code>PreparedStatement</code> object's parameters.
     *
     * @return a <code>ParameterMetaData</code> object that contains information
     *     about the number, types and properties of this
     *     <code>PreparedStatement</code> object's parameters
     * @exception SQLException if a database access error occurs
     * @see ParameterMetaData
     * @since 1.4
     */
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        int flags = QueryExecutor.QUERY_ONESHOT | QueryExecutor.QUERY_DESCRIBE_ONLY | QueryExecutor.QUERY_SUPPRESS_BEGIN;
        StatementResultHandler handler = new StatementResultHandler();
        connection.getQueryExecutor().execute(preparedQuery, preparedParameters, handler, 0, 0, flags);

        int oids[] = preparedParameters.getTypeOIDs();
        if (oids != null)
            return createParameterMetaData(connection, oids);

        return null;

    }

    public abstract ParameterMetaData createParameterMetaData(BaseConnection conn, int oids[]) throws SQLException;

    /**
     * Registers the OUT parameter named
     * <code>parameterName</code> to the JDBC type
     * <code>sqlType</code>.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, <code>sqlType</code>
     * should be <code>java.sql.Types.OTHER</code>.  The method
     * {@link #getObject} retrieves the value.
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by <code>java.sql.Types</code>.
     * If the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>, the version of
     * <code>registerOutParameter</code> that accepts a scale value
     * should be used.
     * @exception SQLException if a database access error occurs
     * @since 1.4
     * @see Types
     */
    public void registerOutParameter(String parameterName, int sqlType)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "registerOutParameter(String,int)");
    }

    /**
     * Registers the parameter named
     * <code>parameterName</code> to be of JDBC type
     * <code>sqlType</code>.  This method must be called
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * This version of <code>registerOutParameter</code> should be
     * used when the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>.
     * @param parameterName the name of the parameter
     * @param sqlType SQL type code defined by <code>java.sql.Types</code>.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception SQLException if a database access error occurs
     * @since 1.4
     * @see Types
     */
    public void registerOutParameter(String parameterName, int sqlType, int scale)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "registerOutParameter(String,int,int)");
    }

    /**
     * Registers the designated output parameter.  This version of
     * the method <code>registerOutParameter</code>
     * should be used for a user-named or REF output parameter.  Examples
     * of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *
     * Before executing a stored procedure call, you must explicitly
     * call <code>registerOutParameter</code> to register the type from
     * <code>java.sql.Types</code> for each
     * OUT parameter.  For a user-named parameter the fully-qualified SQL
     * type name of the parameter should also be given, while a REF
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it. To be portable,
     * however, applications should always provide these values for
     * user-named and REF parameters.
     *
     * Although it is intended for user-named and REF parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-named or REF type, the
     * typeName parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the <code>getXXX</code> method whose Java type XXX corresponds to the
     * parameter's registered SQL type.
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception SQLException if a database access error occurs
     * @see Types
     * @since 1.4
     */
    public void registerOutParameter (String parameterName, int sqlType, String typeName)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "registerOutParameter(String,int,String)");
    }

    /**
     * Retrieves the value of the designated JDBC <code>DATALINK</code> parameter as a
     * <code>java.net.URL</code> object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @return a <code>java.net.URL</code> object that represents the
     *     JDBC <code>DATALINK</code> value used as the designated
     *     parameter
     * @exception SQLException if a database access error occurs,
     *     or if the URL being returned is
     *     not a valid URL on the Java platform
     * @see #setURL
     * @since 1.4
     */
    public java.net.URL getURL(int parameterIndex) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getURL(String)");
    }

    /**
     * Sets the designated parameter to the given <code>java.net.URL</code> object.
     * The driver converts this to an SQL <code>DATALINK</code> value when
     * it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param val the parameter value
     * @exception SQLException if a database access error occurs,
     *     or if a URL is malformed
     * @see #getURL
     * @since 1.4
     */
    public void setURL(String parameterName, java.net.URL val) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setURL(String,URL)");
    }

    /**
     * Sets the designated parameter to SQL <code>NULL</code>.
     *
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * @param parameterName the name of the parameter
     * @param sqlType the SQL type code defined in <code>java.sql.Types</code>
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public void setNull(String parameterName, int sqlType) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setNull(String,int)");
    }

    /**
     * Sets the designated parameter to the given Java <code>boolean</code> value.
     * The driver converts this
     * to an SQL <code>BIT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getBoolean
     * @since 1.4
     */
    public void setBoolean(String parameterName, boolean x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setBoolean(String,boolean)");
    }

    /**
     * Sets the designated parameter to the given Java <code>byte</code> value.
     * The driver converts this
     * to an SQL <code>TINYINT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getByte
     * @since 1.4
     */
    public void setByte(String parameterName, byte x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setByte(String,byte)");
    }

    /**
     * Sets the designated parameter to the given Java <code>short</code> value.
     * The driver converts this
     * to an SQL <code>SMALLINT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getShort
     * @since 1.4
     */
    public void setShort(String parameterName, short x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setShort(String,short)");
    }

    /**
     * Sets the designated parameter to the given Java <code>int</code> value.
     * The driver converts this
     * to an SQL <code>INTEGER</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getInt
     * @since 1.4
     */
    public void setInt(String parameterName, int x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setInt(String,int)");
    }

    /**
     * Sets the designated parameter to the given Java <code>long</code> value.
     * The driver converts this
     * to an SQL <code>BIGINT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getLong
     * @since 1.4
     */
    public void setLong(String parameterName, long x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setLong(String,long)");
    }

    /**
     * Sets the designated parameter to the given Java <code>float</code> value.
     * The driver converts this
     * to an SQL <code>FLOAT</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getFloat
     * @since 1.4
     */
    public void setFloat(String parameterName, float x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setFloat(String,float)");
    }

    /**
     * Sets the designated parameter to the given Java <code>double</code> value.
     * The driver converts this
     * to an SQL <code>DOUBLE</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getDouble
     * @since 1.4
     */
    public void setDouble(String parameterName, double x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setDouble(String,double)");
    }

    /**
     * Sets the designated parameter to the given
     * <code>java.math.BigDecimal</code> value.
     * The driver converts this to an SQL <code>NUMERIC</code> value when
     * it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getBigDecimal
     * @since 1.4
     */
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setBigDecimal(String,BigDecimal)");
    }

    /**
     * Sets the designated parameter to the given Java <code>String</code> value.
     * The driver converts this
     * to an SQL <code>VARCHAR</code> or <code>LONGVARCHAR</code> value
     * (depending on the argument's
     * size relative to the driver's limits on <code>VARCHAR</code> values)
     * when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getString
     * @since 1.4
     */
    public void setString(String parameterName, String x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setString(String,String)");
    }

    /**
     * Sets the designated parameter to the given Java array of bytes.
     * The driver converts this to an SQL <code>VARBINARY</code> or
     * <code>LONGVARBINARY</code> (depending on the argument's size relative 
     * to the driver's limits on <code>VARBINARY</code> values) when it sends 
     * it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getBytes
     * @since 1.4
     */
    public void setBytes(String parameterName, byte x[]) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setBytes(String,byte)");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value.
     * The driver converts this
     * to an SQL <code>DATE</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getDate
     * @since 1.4
     */
    public void setDate(String parameterName, java.sql.Date x)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setDate(String,Date)");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value.
     * The driver converts this
     * to an SQL <code>TIME</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getTime
     * @since 1.4
     */
    public void setTime(String parameterName, java.sql.Time x)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setTime(String,Time)");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value.
     * The driver
     * converts this to an SQL <code>TIMESTAMP</code> value when it sends it to the
     * database.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs
     * @see #getTimestamp
     * @since 1.4
     */
    public void setTimestamp(String parameterName, java.sql.Timestamp x)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setTimestamp(String,Timestamp)");
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached. The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public void setAsciiStream(String parameterName, java.io.InputStream x, int length)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setAsciiStream(String,InputStream,int)");
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public void setBinaryStream(String parameterName, java.io.InputStream x,
                                int length) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setBinaryStream(String,InputStream,int)");
    }

    /**
     * Sets the value of the designated parameter with the given object. The second
     * argument must be an object type; for integral values, the
     * <code>java.lang</code> equivalent objects should be used.
     *
     * <p>The given Java object will be converted to the given targetSqlType
     * before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the
     * interface <code>SQLData</code>),
     * the JDBC driver should call the method <code>SQLData.writeSQL</code> to write it
     * to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>, <code>Struct</code>,
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * Note that this method may be used to pass datatabase-
     * specific abstract data types.
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     * sent to the database. The scale argument may further qualify this type.
     * @param scale for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
     *   this is the number of digits after the decimal point.  For all other
     *   types, this value will be ignored.
     * @exception SQLException if a database access error occurs
     * @see Types
     * @see #getObject
     * @since 1.4
     */
    public void setObject(String parameterName, Object x, int targetSqlType, int scale)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setObject(String,Object,int,int)");
    }

    /**
     * Sets the value of the designated parameter with the given object.
     * This method is like the method <code>setObject</code>
     * above, except that it assumes a scale of zero.
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     *      sent to the database
     * @exception SQLException if a database access error occurs
     * @see #getObject
     * @since 1.4
     */
    public void setObject(String parameterName, Object x, int targetSqlType)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setObject(String,Object,int)");
    }

    /**
     * Sets the value of the designated parameter with the given object.
     * The second parameter must be of type <code>Object</code>; therefore, the
     * <code>java.lang</code> equivalent objects should be used for built-in types.
     *
     * <p>The JDBC specification specifies a standard mapping from
     * Java <code>Object</code> types to SQL types.  The given argument
     * will be converted to the corresponding SQL type before being
     * sent to the database.
     *
     * <p>Note that this method may be used to pass datatabase-
     * specific abstract data types, by using a driver-specific Java
     * type.
     *
     * If the object is of a class implementing the interface <code>SQLData</code>,
     * the JDBC driver should call the method <code>SQLData.writeSQL</code>
     * to write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>, <code>Struct</code>,
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * This method throws an exception if there is an ambiguity, for example, if the
     * object is of a class implementing more than one of the interfaces named above.
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @exception SQLException if a database access error occurs or if the given
     *     <code>Object</code> parameter is ambiguous
     * @see #getObject
     * @since 1.4
     */
    public void setObject(String parameterName, Object x) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setObject(String,Object)");
    }


    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached. The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param reader the <code>java.io.Reader</code> object that
     *    contains the UNICODE data used as the designated parameter
     * @param length the number of characters in the stream
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public void setCharacterStream(String parameterName,
                                   java.io.Reader reader,
                                   int length) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setCharacterStream(String,Reader,int)");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>DATE</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the date
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *     to construct the date
     * @exception SQLException if a database access error occurs
     * @see #getDate
     * @since 1.4
     */
    public void setDate(String parameterName, java.sql.Date x, Calendar cal)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setDate(String,Date,Calendar)");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIME</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the time
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *     to construct the time
     * @exception SQLException if a database access error occurs
     * @see #getTime
     * @since 1.4
     */
    public void setTime(String parameterName, java.sql.Time x, Calendar cal)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setTime(String,Time,Calendar)");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIMESTAMP</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the timestamp
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *     to construct the timestamp
     * @exception SQLException if a database access error occurs
     * @see #getTimestamp
     * @since 1.4
     */
    public void setTimestamp(String parameterName, java.sql.Timestamp x, Calendar cal)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setTimestamp(String,Timestamp,Calendar)");
    }

    /**
     * Sets the designated parameter to SQL <code>NULL</code>.
     * This version of the method <code>setNull</code> should
     * be used for user-defined types and REF type parameters. Examples
     * of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *
     * <P><B>Note:</B> To be portable, applications must give the
     * SQL type code and the fully-qualified SQL type name when specifying
     * a NULL user-defined or REF parameter.  In the case of a user-defined type
     * the name is the type name of the parameter itself.  For a REF
     * parameter, the name is the type name of the referenced type.  If
     * a JDBC driver does not need the type code or type name information,
     * it may ignore it.
     *
     * Although it is intended for user-defined and Ref parameters,
     * this method may be used to set a null parameter of any JDBC type.
     * If the parameter does not have a user-defined or REF type, the given
     * typeName is ignored.
     *
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from <code>java.sql.Types</code>
     * @param typeName the fully-qualified name of an SQL user-defined type;
     *    ignored if the parameter is not a user-defined type or
     *    SQL <code>REF</code> value
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public void setNull (String parameterName, int sqlType, String typeName)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "setNull(String,int,String)");
    }

    /**
     * Retrieves the value of a JDBC <code>CHAR</code>, <code>VARCHAR</code>,
     * or <code>LONGVARCHAR</code> parameter as a <code>String</code> in
     * the Java programming language.
     * <p>
     * For the fixed-length type JDBC <code>CHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the JDBC
     * <code>CHAR</code> value had in the
     * database, including any padding added by the database.
     * @param parameterName the name of the parameter
     * @return the parameter value. If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setString
     * @since 1.4
     */
    public String getString(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getString(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>BIT</code> parameter as a
     * <code>boolean</code> in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>false</code>.
     * @exception SQLException if a database access error occurs
     * @see #setBoolean
     * @since 1.4
     */
    public boolean getBoolean(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getBoolean(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>TINYINT</code> parameter as a <code>byte</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs
     * @see #setByte
     * @since 1.4
     */
    public byte getByte(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getByte(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>SMALLINT</code> parameter as a <code>short</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs
     * @see #setShort
     * @since 1.4
     */
    public short getShort(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getShort(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>INTEGER</code> parameter as an <code>int</code>
     * in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *     the result is <code>0</code>.
     * @exception SQLException if a database access error occurs
     * @see #setInt
     * @since 1.4
     */
    public int getInt(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getInt(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>BIGINT</code> parameter as a <code>long</code>
     * in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *     the result is <code>0</code>.
     * @exception SQLException if a database access error occurs
     * @see #setLong
     * @since 1.4
     */
    public long getLong(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getLong(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>FLOAT</code> parameter as a <code>float</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *     the result is <code>0</code>.
     * @exception SQLException if a database access error occurs
     * @see #setFloat
     * @since 1.4
     */
    public float getFloat(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getFloat(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>DOUBLE</code> parameter as a <code>double</code>
     * in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *     the result is <code>0</code>.
     * @exception SQLException if a database access error occurs
     * @see #setDouble
     * @since 1.4
     */
    public double getDouble(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getDouble(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>BINARY</code> or <code>VARBINARY</code>
     * parameter as an array of <code>byte</code> values in the Java
     * programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result is
     * <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setBytes
     * @since 1.4
     */
    public byte[] getBytes(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getBytes(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>DATE</code> parameter as a
     * <code>java.sql.Date</code> object.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setDate
     * @since 1.4
     */
    public java.sql.Date getDate(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getDate(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>TIME</code> parameter as a
     * <code>java.sql.Time</code> object.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setTime
     * @since 1.4
     */
    public java.sql.Time getTime(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getTime(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
     * <code>java.sql.Timestamp</code> object.
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setTimestamp
     * @since 1.4
     */
    public java.sql.Timestamp getTimestamp(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getTimestamp(String)");
    }

    /**
     * Retrieves the value of a parameter as an <code>Object</code> in the Java
     * programming language. If the value is an SQL <code>NULL</code>, the
     * driver returns a Java <code>null</code>.
     * <p>
     * This method returns a Java object whose type corresponds to the JDBC
     * type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target JDBC
     * type as <code>java.sql.Types.OTHER</code>, this method can be used
     * to read database-specific abstract data types.
     * @param parameterName the name of the parameter
     * @return A <code>java.lang.Object</code> holding the OUT parameter value.
     * @exception SQLException if a database access error occurs
     * @see Types
     * @see #setObject
     * @since 1.4
     */
    public Object getObject(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getObject(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>NUMERIC</code> parameter as a
     * <code>java.math.BigDecimal</code> object with as many digits to the
     * right of the decimal point as the value contains.
     * @param parameterName the name of the parameter
     * @return the parameter value in full precision.  If the value is
     * SQL <code>NULL</code>, the result is <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setBigDecimal
     * @since 1.4
     */
    public BigDecimal getBigDecimal(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getBigDecimal(String)");
    }

    /**
     * Returns an object representing the value of OUT parameter
     * <code>i</code> and uses <code>map</code> for the custom
     * mapping of the parameter value.
     * <p>
     * This method returns a Java object whose type corresponds to the
     * JDBC type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target
     * JDBC type as <code>java.sql.Types.OTHER</code>, this method can
     * be used to read database-specific abstract data types.
     * @param parameterName the name of the parameter
     * @param map the mapping from SQL type names to Java classes
     * @return a <code>java.lang.Object</code> holding the OUT parameter value
     * @exception SQLException if a database access error occurs
     * @see #setObject
     * @since 1.4
     */
    public Object getObjectImpl (String parameterName, java.util.Map map) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getObject(String,Map)");
    }

    /**
     * Retrieves the value of a JDBC <code>REF(&lt;structured-type&gt;)</code>
     * parameter as a {@link Ref} object in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Ref</code> object in the
     *     Java programming language.  If the value was SQL <code>NULL</code>,
     *     the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public Ref getRef (String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getRef(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>BLOB</code> parameter as a
     * {@link Blob} object in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Blob</code> object in the
     *     Java programming language.  If the value was SQL <code>NULL</code>,
     *     the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public Blob getBlob (String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getBlob(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>CLOB</code> parameter as a
     * <code>Clob</code> object in the Java programming language.
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Clob</code> object in the
     *     Java programming language.  If the value was SQL <code>NULL</code>,
     *     the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public Clob getClob (String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getClob(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>ARRAY</code> parameter as an
     * {@link Array} object in the Java programming language.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as an <code>Array</code> object in
     *     Java programming language.  If the value was SQL <code>NULL</code>,
     *     the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs
     * @since 1.4
     */
    public Array getArray (String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getArray(String)");
    }

    /**
     * Retrieves the value of a JDBC <code>DATE</code> parameter as a
     * <code>java.sql.Date</code> object, using
     * the given <code>Calendar</code> object
     * to construct the date.
     * With a <code>Calendar</code> object, the driver
     * can calculate the date taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *     to construct the date
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     * the result is <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setDate
     * @since 1.4
     */
    public java.sql.Date getDate(String parameterName, Calendar cal)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getDate(String,Calendar)");
    }

    /**
     * Retrieves the value of a JDBC <code>TIME</code> parameter as a
     * <code>java.sql.Time</code> object, using
     * the given <code>Calendar</code> object
     * to construct the time.
     * With a <code>Calendar</code> object, the driver
     * can calculate the time taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *     to construct the time
     * @return the parameter value; if the value is SQL <code>NULL</code>, the result is
     * <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setTime
     * @since 1.4
     */
    public java.sql.Time getTime(String parameterName, Calendar cal)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getTime(String,Calendar)");
    }

    /**
     * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
     * <code>java.sql.Timestamp</code> object, using
     * the given <code>Calendar</code> object to construct
     * the <code>Timestamp</code> object.
     * With a <code>Calendar</code> object, the driver
     * can calculate the timestamp taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *     to construct the timestamp
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result is
     * <code>null</code>.
     * @exception SQLException if a database access error occurs
     * @see #setTimestamp
     * @since 1.4
     */
    public java.sql.Timestamp getTimestamp(String parameterName, Calendar cal)
    throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getTimestamp(String,Calendar)");
    }

    /**
     * Retrieves the value of a JDBC <code>DATALINK</code> parameter as a
     * <code>java.net.URL</code> object.
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>java.net.URL</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs,
     *     or if there is a problem with the URL
     * @see #setURL
     * @since 1.4
     */
    public java.net.URL getURL(String parameterName) throws SQLException
    {
        throw org.postgresql.driver.Driver.notImplemented(this.getClass(), "getURL(String)");
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException
    {
        if (targetSqlType == Types.BOOLEAN)
        {
            targetSqlType = Types.BIT;
        }
        super.setObject(parameterIndex, x, targetSqlType, scale);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        if (sqlType == Types.BOOLEAN)
        {
            sqlType = Types.BIT;
        }
        super.setNull(parameterIndex, sqlType);
    }

    

    protected boolean wantsHoldableResultSet() {
        return rsHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }
    
    public void registerOutParameter( int parameterIndex, int sqlType ) throws SQLException
    {
        // if this isn't 8.1 or we are using protocol version 2 then we don't
        // register the parameter
        switch( sqlType )
        {
        		case Types.BOOLEAN:
        		    	sqlType = Types.BIT;
        			break;
    			default:
        		
        }
        super.registerOutParameter(parameterIndex, sqlType, !adjustIndex );
    }
    public void registerOutParameter(int parameterIndex, int sqlType,
            int scale) throws SQLException
    {
        // ignore scale for now
        registerOutParameter(parameterIndex, sqlType );
    }
}
