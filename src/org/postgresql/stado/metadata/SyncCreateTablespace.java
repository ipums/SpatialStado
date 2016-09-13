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
/**
 * 
 */
package org.postgresql.stado.metadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlCreateTableSpace;


/**
 * 
 * 
 */
public class SyncCreateTablespace implements IMetaDataUpdate {
    private static final XLogger logger = XLogger
            .getLogger(SyncCreateTablespace.class);

    private XDBSessionContext client;

    private SqlCreateTableSpace parent;

    private int tablespaceID;

    /**
     * 
     */
    public SyncCreateTablespace(SqlCreateTableSpace parent) {
        this.parent = parent;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#execute(org.postgresql.stado.server.XDBSessionContext)
     */
    public void execute(XDBSessionContext client) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {

            this.client = client;

            MetaData meta = MetaData.getMetaData();

            String commandStr = "INSERT INTO xsystablespaces"
                    + " (tablespacename, ownerid) VALUES ("
                    + "'" + parent.getName() + "', "
                    + client.getCurrentUser().getUserID() + ")";
            ResultSet keys = MetaData.getMetaData().executeUpdateReturning(commandStr);
            if (keys.next()) {
            	tablespaceID = keys.getInt(1);
            } else {
            	throw new Exception("Error creating tablespace");
            }

            // Insert locations
            int locationID;
            ResultSet rs = meta
                    .executeQuery("SELECT max(tablespacelocid) FROM xsystablespacelocs");
            try {
                rs.next();
                locationID = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }

            commandStr = "INSERT INTO xsystablespacelocs"
                    + " (tablespacelocid, tablespaceid, filepath, nodeid)"
                    + " VALUES (?, ?, ?, ?)";
            PreparedStatement ps = meta.prepareStatement(commandStr);
            // Same for all locations
            ps.setInt(2, tablespaceID);
            for (Iterator it = parent.getLocations().entrySet().iterator(); it
                    .hasNext();) {
                Map.Entry entry = (Map.Entry) it.next();
                ps.setInt(1, locationID++);
                ps.setString(3, (String) entry.getValue());
                ps.setInt(4, ((DBNode) entry.getKey()).getNodeId());
                if (ps.executeUpdate() != 1) {
                    XDBServerException ex = new XDBServerException(
                            "Failed to insert row into \"xsystablespacelocs\"");
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.IMetaDataUpdate#refresh()
     */
    public void refresh() throws Exception {
        HashMap locations = new HashMap();
        for (Iterator it = parent.getLocations().entrySet().iterator(); it
                .hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            locations.put(new Integer(((DBNode) entry.getKey()).getNodeId()),
                    entry.getValue());
        }
        SysTablespace tablespace = new SysTablespace(tablespaceID, parent
                .getName(), client.getCurrentUser().getUserID(), locations);
        MetaData.getMetaData().addTablespace(tablespace);
    }

}
