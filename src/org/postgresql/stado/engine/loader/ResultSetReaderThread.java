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
package org.postgresql.stado.engine.loader;

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.exception.XDBDataReaderException;


/**
 * Purpose of Data Reader is to split data source on rows
 * ResultSetReaderThread assumes the data source is a java.sql.ResultSet
 *
 * @author amart
 */
public class ResultSetReaderThread implements Callable<Boolean> {

    private DataReaderAndProcessorBuffer<String[]> loadBuffer;

    private int columns;

    private ResultSet dataSourceRS;

    private int[] groupHashList;
    
    //sray: toRemove
    static int threadIdGen = 0;
    private int threadId;
    
    /**
     * Creates a new instance of ResultSetReaderThread
     * @param rs
     * @param buffer
     * @param groupHashList List of expression strings to use to create
     *        a hashable String on. Used for GROUP BY processing
     * @throws XDBDataReaderException
     */
    public ResultSetReaderThread(ResultSet rs,
            DataReaderAndProcessorBuffer<String[]> buffer,
            int[] groupHashList)
            throws XDBDataReaderException {
        try {
            dataSourceRS = rs;
            columns = dataSourceRS.getMetaData().getColumnCount();
            loadBuffer = buffer;
            this.groupHashList = groupHashList;
            
            //sray: toRemove
            threadId = ++threadIdGen;
        } catch (Exception ex) {
            throw new XDBDataReaderException(ex);
        }
    }

    /**
     * Closes data source.
     *
     */
    public void close() {
        try {
            if (dataSourceRS != null) {
                dataSourceRS.close();
            }
        } catch (Exception ex) {
            // Ignore exception message.
        }
    }

    /**
     * Read current row from the data source into string array
     * @return
     * @throws XDBDataReaderException
     */
    private String[] readLineFromResultSet() throws XDBDataReaderException {
        try {
            if (!dataSourceRS.next()) {
                return null;
            }
            String[] colsValue = new String[columns];
            for (int i = 0; i < columns; i++) {
                colsValue[i] = dataSourceRS.getString(i + 1);
            }
            return colsValue;
        } catch (Exception ex) {
            throw new XDBDataReaderException(ex);
        }
    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    public Boolean call() throws XDBDataReaderException {
        String[] rowColsValue;
        try {
            while ((rowColsValue = readLineFromResultSet()) != null) {
                //loadBuffer.putRowValue(rowColsValue, getGroupByHashString()); 
                loadBuffer.putRowValue(rowColsValue, getGroupByHashString(),threadId); //sray: toRemove
            }
            return true;
        } catch (Exception ex) {
        	System.err.println("ResultSetReaderThread in call()-catch: threadId="+ threadId + " throwing exception\n");
            throw new XDBDataReaderException(ex);
   
        } finally {
            close();
            if (loadBuffer != null) {
            	if (Props.XDB_TO_DEBUG) System.out.println("ResultSetReaderThread in call()-finally: threadId="+ threadId + " marking buffer finished\n");
                //loadBuffer.markFinished();
            	loadBuffer.markFinished(threadId); //sray: toRemove
            }
        }
    }

    /**
     * @return a String to use for group by hashing purposes
     */
    private String getGroupByHashString() throws java.sql.SQLException {

        if (groupHashList == null || dataSourceRS.isAfterLast()) {
            return null;
        }

        StringBuffer sbHash = new StringBuffer();
        for (int element : groupHashList) {
            sbHash.append(dataSourceRS.getString(element));
        }
        return sbHash.toString();
    }
}
