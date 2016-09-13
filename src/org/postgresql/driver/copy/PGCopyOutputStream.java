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
package org.postgresql.driver.copy;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

import org.postgresql.driver.PGConnection;
import org.postgresql.driver.util.GT;

/**
 * OutputStream for buffered input into a PostgreSQL COPY FROM STDIN operation
 */
public class PGCopyOutputStream extends OutputStream implements CopyIn {
    private CopyIn op;
    private final byte[] copyBuffer;
    private final byte[] singleByteBuffer = new byte[1];
    private int at = 0;

    /**
     * Uses given connection for specified COPY FROM STDIN operation
     * @param connection database connection to use for copying (protocol version 3 required)
     * @param sql COPY FROM STDIN statement
     * @throws SQLException if initializing the operation fails
     */
    public PGCopyOutputStream(PGConnection connection, String sql) throws SQLException {
        this(connection, sql, CopyManager.DEFAULT_BUFFER_SIZE);
    }

    /**
     * Uses given connection for specified COPY FROM STDIN operation
     * @param connection database connection to use for copying (protocol version 3 required)
     * @param sql COPY FROM STDIN statement
     * @param bufferSize try to send this many bytes at a time
     * @throws SQLException if initializing the operation fails
     */
    public PGCopyOutputStream(PGConnection connection, String sql, int bufferSize) throws SQLException {
        this(connection.getCopyAPI().copyIn(sql), bufferSize);
    }

    /**
     * Use given CopyIn operation for writing
     * @param op COPY FROM STDIN operation
     */
    public PGCopyOutputStream(CopyIn op) {
        this(op, CopyManager.DEFAULT_BUFFER_SIZE);
    }

    /**
     * Use given CopyIn operation for writing
     * @param op COPY FROM STDIN operation
     * @param bufferSize try to send this many bytes at a time
     */
    public PGCopyOutputStream(CopyIn op, int bufferSize) {
        this.op = op;
        copyBuffer = new byte[bufferSize];
    }

    public void write(int b) throws IOException {
        checkClosed();
        if(b<0 || b>255)
            throw new IOException(GT.tr("Cannot write to copy a byte of value {0}", new Integer(b)));
        singleByteBuffer[0] = (byte)b;
        write(singleByteBuffer, 0, 1);
    }

    public void write(byte[] buf) throws IOException {
        write(buf, 0, buf.length);
    }

    public void write(byte[] buf, int off, int siz) throws IOException {
        checkClosed();
        try {
            writeToCopy(buf, off, siz);
        } catch(SQLException se) {
            IOException ioe = new IOException("Write to copy failed.");
            ioe.initCause(se);
            throw ioe;
        }
    }

    private void checkClosed() throws IOException {
        if (op == null) {
            throw new IOException(GT.tr("This copy stream is closed."));
        }
    }

    public void close() throws IOException {
        // Don't complain about a double close.
        if (op == null)
            return;

        try{
            endCopy();
        } catch(SQLException se) {
            IOException ioe = new IOException("Ending write to copy failed.");
            ioe.initCause(se);
            throw ioe;
        }
        op = null;
    }

    public void flush() throws IOException {
        try {
            op.writeToCopy(copyBuffer, 0, at);
            at = 0;
            op.flushCopy();
        } catch (SQLException e) {
            IOException ioe = new IOException("Unable to flush stream");
            ioe.initCause(e);
            throw ioe;
        }
    }

    public void writeToCopy(byte[] buf, int off, int siz) throws SQLException {
        if(at > 0 && siz > copyBuffer.length - at) { // would not fit into rest of our buf, so flush buf
            op.writeToCopy(copyBuffer, 0, at);
            at = 0;
        }
        if(siz > copyBuffer.length) { // would still not fit into buf, so just pass it through
            op.writeToCopy(buf, off, siz);
        } else { // fits into our buf, so save it there
            System.arraycopy(buf, off, copyBuffer, at, siz);
            at += siz;
        }
    }

    public int getFormat() {
        return op.getFormat();
    }

    public int getFieldFormat(int field) {
        return op.getFieldFormat(field);
    }

    public void cancelCopy() throws SQLException {
        op.cancelCopy();
    }

    public int getFieldCount() {
        return op.getFieldCount();
    }

    public boolean isActive() {
        return op.isActive();
    }

    public void flushCopy() throws SQLException {
        op.flushCopy();
    }

    public long endCopy() throws SQLException {
        if(at > 0) {
            op.writeToCopy(copyBuffer, 0, at);
        }
        op.endCopy();
        return getHandledRowCount();
    }

    public long getHandledRowCount() {
        return op.getHandledRowCount();
    }

}
