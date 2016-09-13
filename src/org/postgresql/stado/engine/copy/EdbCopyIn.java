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

import java.sql.SQLException;

/**
 * This is a wrapper class for CopyIn interface of the Edb driver
 * Delegates respective method calls
 */
public class EdbCopyIn implements CopyIn {
    private com.edb.copy.CopyIn copyIn;

    /**
     * Constructs new instance of EdbCopyIn
     * @param copyIn
     *          Edb driver's CopyIn object
     */
    EdbCopyIn(com.edb.copy.CopyIn copyIn) {
        this.copyIn = copyIn;
    }

    /**
     * Delegate the call to Edb CopyIn
     * @param buffer
     *          byte array containing data
     * @param offset
     *          where the data begin
     * @param length
     *          number of data bytes
     * @throws SQLException
     *          if SQL error occurs
     */
    public void writeToCopy(byte[] buffer, int offset, int length) throws SQLException {
        copyIn.writeToCopy(buffer, offset, length);
    }

    /**
     * Delegate the call to Edb CopyIn
     * @return number of rows affected
     * @throws SQLException
     *          if SQL error occurs
     */
    public long endCopy() throws SQLException {
        return copyIn.endCopy();
    }

    /**
     * Delegate the call to Edb CopyIn
     * @throws SQLException
     *          if SQL error occurs
     */
    public void cancelCopy() throws SQLException {
        copyIn.cancelCopy();
    }
}
