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
/*
 * TimeType.java
 *
 *
 */

package org.postgresql.stado.engine.datatypes;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;

/**
 *
 *
 */
public class TimeType implements XData {
    private Time val = null;
    private String timezone = null;

    protected TimeType() {
    }

    /** Creates a new instance of TimeType */
    public TimeType(Time t, String timezone) {
        this.val = t;
        this.timezone = timezone == null ? "" : timezone;
    }

    public int getJavaType() {
        return Types.TIME;
    }

    /** get the current data type value as string */
    @Override
    public String toString() {
        if (val == null) {
            return null;
        }
        return val.toString() + timezone;
    }

    /** is this data value null? */
    public boolean isValueNull() {
        return val == null;
    }

    public void setJavaType(int type) {
    }

    /** indicate whether the current data value is null */
    public void setNull(boolean isNull) {
    }

    /** returns the java object implemented */
    public Object getObject() throws SQLException {
        return timezone == null || timezone.length() == 0 ? val : this;
    }

    public void setObject(Object o) throws SQLException {
    }

    /** convert the value to string with the specified charset encoding */
    public String toString(String encoding)
            throws java.io.UnsupportedEncodingException {
        if (val == null) {
            return null;
        }
        return new String(val.toString().getBytes(encoding));
    }

}
