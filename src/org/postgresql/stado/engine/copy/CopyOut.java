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
package org.postgresql.stado.engine.copy;

import java.sql.SQLException;

/**
 * Objects implementing this interface are used to control running COPY TO command
 */
public interface CopyOut {

    /**
     * Returns number of fields in the data set
     * @return field count
     */
    int getFieldCount();

    /**
     * Get one row of data from the COPY command
     * @return byte array containing the data
     * @throws SQLException
     *          if SQL error occurs
     */
    byte[] readFromCopy() throws SQLException;

    /**
     * Cancel the COPY command
     * @throws SQLException
     *          if SQL error occurs
     */
    void cancelCopy() throws SQLException;
}
