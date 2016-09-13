/*****************************************************************************
 * Copyright (C) 2008 EnterpriseDB Corporation.
 * Copyright (c) 2016 Regents of the University of Minnesota
 *
 * This file is part of the Minnesota Population Center's Terra Populus project.
 * For copyright and licensing information, see the NOTICE and LICENSE files
 * in this project's top-level directory, and also online at:
 * https://github.com/mnpopcenter/stado
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses or write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *
 * You can contact EnterpriseDB, Inc. via its website at
 * http://www.enterprisedb.com
 *
 ****************************************************************************/
package org.postgresql.stado.engine.copy;

import com.edb.PGConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * Wrapper for Edb connection
 * Delegate method calls to Edb CopyManager's functions
 */
public class EdbCopyManager extends CopyManager {
    private PGConnection connection;

    /**
     * Create new instance of CopyManager
     * @param connection
     *          the connection
     */
    EdbCopyManager(PGConnection connection) {
        this.connection = connection;
        // TODO check driver version to verify if copy is supported
    }

    /**
     * Delegate the call to Edb CopyManager
     * @param command
     *          must be a COPY FROM STDIN
     * @param in
     *          the data source
     * @return number of rows affected
     * @throws SQLException
     *          if SQL error occurs
     * @throws IOException
     *          problem of reading from the stream
     */
    public long copyIn(String command, InputStream in) throws SQLException, IOException {
        return connection.getCopyAPI().copyIn(command, in);
    }

    /**
     * Delegate the call to Edb CopyManager
     * @param command
     *          must be a COPY FROM STDIN
     * @return an instance of EdbCopyIn
     * @throws SQLException
     *          if SQL error occurs
     */
    public CopyIn copyIn(String command) throws SQLException {
        return new EdbCopyIn(connection.getCopyAPI().copyIn(command));
    }

    /**
     * Delegate the call to Edb CopyManager
     * @param command
     *          must be a COPY TO STDOUT
     * @param out
     *          the destination
     * @return number of rows returned
     * @throws SQLException
     *          if SQL error occurs
     * @throws IOException
     *          problem of writing to the stream
     */
    public long copyOut(String command, OutputStream out) throws SQLException, IOException {
        return connection.getCopyAPI().copyOut(command, out);
    }

    /**
     * Delegate the call to Edb CopyManager
     * @param command
     *          must be a COPY TO STDOUT
     * @return an instance of EdbCopyOut
     * @throws SQLException
     *          if SQL error occurs
     */
    public CopyOut copyOut(String command) throws SQLException {
        return new EdbCopyOut(connection.getCopyAPI().copyOut(command));
    }
}
