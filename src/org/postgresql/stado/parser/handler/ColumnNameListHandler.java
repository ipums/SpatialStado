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

import java.util.ArrayList;
import java.util.List;

import org.postgresql.stado.parser.core.syntaxtree.Identifier;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;


/**
 * This class is responsible to handle any list which contains a list of
 * columnNames. It captures the visit functions for a ColumnNameList.
 */
public class ColumnNameListHandler extends ObjectDepthFirst {

    /**
     * This will contain the List of Name that this handler had to handle.
     */
    List<String> columnNameList = new ArrayList<String>();

    private IdentifierHandler ih = new IdentifierHandler();

    /**
     * A get function for accessing the information of this class
     *
     * @return This contains a list of String which will have the column names
     */
    public List<String> getColumnNameList() {
        return columnNameList;
    }

    /**
     * Constructor
     */
    public ColumnNameListHandler() {
    }

    /**
     * Grammar production: f0 -> <IDENTIFIER_NAME> | UnreservedWords(prn)
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(Identifier n, Object argu) {
        columnNameList.add((String) n.accept(ih, argu));
        return null;
    }

}
