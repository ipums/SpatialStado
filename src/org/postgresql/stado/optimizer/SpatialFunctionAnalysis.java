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
 * SpatailFunctionAnalysis Class is responsible for doing the checks for the all
 * the supported functions
 * 
 */
public class SpatialFunctionAnalysis {
	public static final int MAX_GEOMETRY_TEXT_LEN = 1024;
	public static final int MAX_BOX2D_TEXT_LEN = 1024;
	public static final int MAX_BOX3D_TEXT_LEN = 1024;
	public static final int MAX_BOX3D_EXTENT_TEXT_LEN = 1024;
	

	/**
	 * Input:GEOMETRY, VARCHAR Output: VARCHAR
	 */
	public static ExpressionType analyzeST_AsText(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as VARCHAR
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, INTEGER, VARCHAR Output: GEOMETRY
	 */
	public static ExpressionType analyzeST_Transform(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, GEOMETRY, VARCHAR Output: FLOAT
	 */
	public static ExpressionType analyzeST_Distance(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as FLOAT
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, GEOMETRY, DOUBLE, VARCHAR Output: BOOLEAN
	 */
	public static ExpressionType analyzeST_DWithin(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: FLOAT
	 */
	public static ExpressionType analyzeST_Length(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as FLOAT
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: FLOAT
	 */
	public static ExpressionType analyzeST_Area(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as FLOAT
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: BYTEA
	 */
	public static ExpressionType analyzeST_AsBinary(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as BYTEA
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BLOB_TYPE, -1, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: GEOMETRY
	 */
	public static ExpressionType analyzeST_Boundary(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, FLOAT, TEXT Output: GEOMETRY
	 */
	public static ExpressionType analyzeST_Buffer(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: GEOMETRY
	 */
	public static ExpressionType analyzeST_Centroid(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ADDMEASURE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ADDPOINT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_AFFINE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_AREA2D(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, VARCHAR Output: BYTEA
	 */
	public static ExpressionType analyzeST_ASEWKB(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BLOB_TYPE, -1, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: TEXT
	 */
	public static ExpressionType analyzeST_ASEWKT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ASGEOJSON(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ASGML(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ASHEXEWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ASKML(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ASSVG(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_AZIMUTH(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_BDMPOLYFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_BDPOLYFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_BUILDAREA(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_BYTEA(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CHIP_IN(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CHIP_OUT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CLOSESTPOINT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_COLLECTIONEXTRACT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_COMBINE_BBOX(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_COMPRESSION(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CONTAINS(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CONTAINSPROPERLY(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CONVEXHULL(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_COORDDIM(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_COVEREDBY(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_COVERS(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CROSSES(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_CURVETOLINE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_DATATYPE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_DFULLYWITHIN(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_DIFFERENCE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_DIMENSION(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_DISJOINT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_DISTANCE_SPHERE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_DISTANCE_SPHEROID(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ENDPOINT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ENVELOPE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_EQUALS(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_EXTERIORRING(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FACTOR(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FIND_EXTENT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FORCERHR(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FORCE_2D(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FORCE_3D(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FORCE_3DM(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FORCE_3DZ(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FORCE_4D(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_FORCE_COLLECTION(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOGFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOGFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOGRAPHYFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOHASH(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMCOLLFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMCOLLFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMFROMEWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMFROMEWKT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMFROMGML(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMFROMKML(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GMLTOSQL(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_HASARC(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_HAUSDORFFDISTANCE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_HEIGHT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_INTERIORRINGN(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_INTERSECTION(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_INTERSECTS(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ISCLOSED(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ISEMPTY(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ISRING(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ISSIMPLE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ISVALID(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ISVALIDREASON(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LENGTH2D(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LENGTH2D_SPHEROID(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LENGTH3D(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LENGTH3D_SPHEROID(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LENGTH_SPHEROID(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINECROSSINGDIRECTION(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINEFROMMULTIPOINT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINEFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINEFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINEMERGE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINESTRINGFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINETOCURVE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINE_INTERPOLATE_POINT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, GEOMETRY, VARCHAR Output: FLOAT
	 */
	public static ExpressionType analyzeST_LINE_LOCATE_POINT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LINE_SUBSTRING(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LOCATEBETWEENELEVATIONS(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LOCATE_ALONG_MEASURE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LOCATE_BETWEEN_MEASURES(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_LONGESTLINE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_M(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MAKEENVELOPE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MAKEPOINT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MAKEPOINTM(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MAKEPOLYGON(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MAXDISTANCE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MEMCOLLECT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: INTEGER
	 */
	public static ExpressionType analyzeST_MEM_SIZE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MINIMUMBOUNDINGCIRCLE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MLINEFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MLINEFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MPOINTFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MPOINTFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MPOLYFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MPOLYFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MULTI(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MULTILINEFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MULTILINESTRINGFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MULTIPOINTFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MULTIPOINTFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MULTIPOLYFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_MULTIPOLYGONFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_NDIMS(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: INTEGER
	 */
	public static ExpressionType analyzeST_NPOINTS(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: INTEGER
	 */
	public static ExpressionType analyzeST_NRINGS(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: INTEGER
	 */
	public static ExpressionType analyzeST_NUMGEOMETRIES(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_NUMINTERIORRING(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.INT_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_NUMINTERIORRINGS(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_NUMPOINTS(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, GEOMETRY Output: BOOLEAN
	 */	
	public static ExpressionType analyzeST_ORDERINGEQUALS(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_OVERLAPS(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_PERIMETER(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_PERIMETER2D(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_PERIMETER3D(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POINT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POINTFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POINTFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POINTN(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POINTONSURFACE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POINT_INSIDE_CIRCLE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POLYFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POLYFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POLYGON(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POLYGONFROMTEXT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POLYGONFROMWKB(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POSTGIS_GIST_JOINSEL(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_POSTGIS_GIST_SEL(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_RELATE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_REMOVEPOINT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_REVERSE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ROTATE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ROTATEX(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ROTATEY(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ROTATEZ(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SCALE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SEGMENTIZE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SETFACTOR(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, INTEGER, GEOMETRY Output: GEOMETRY
	 */	
	public static ExpressionType analyzeST_SETPOINT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE, MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, INTEGER, GEOMETRY Output: GEOMETRY
	 */	
	public static ExpressionType analyzeST_SETSRID(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SHIFT_LONGITUDE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SHORTESTLINE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SIMPLIFY(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SIMPLIFYPRESERVETOPOLOGY(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SNAPTOGRID(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SRID(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_STARTPOINT(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SUMMARY(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SYMDIFFERENCE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_SYMMETRICDIFFERENCE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_TEXT(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_TOUCHES(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_TRANSLATE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_TRANSSCALE(
			SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_WIDTH(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_WITHIN(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_WKBTOSQL(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_WKTTOSQL(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE,
				MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_X(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_Y(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_Z(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.FLOAT_TYPE, 32, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_ZMFLAG(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {
		ExpressionType exprT = new ExpressionType();
        exprT.setExpressionType(ExpressionType.SMALLINT_TYPE, 0, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: BOX2D
	 */
	public static ExpressionType analyzeST_BOX2D(SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as BOX2D
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOX2D_TYPE, MAX_BOX2D_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: BOX3D
	 */
	public static ExpressionType analyzeST_BOX3D(SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as BOX3D
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOX3D_TYPE, MAX_BOX3D_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:BOX3D Output: GEOMETRY
	 */
	public static ExpressionType analyzeST_GEOMETRY(SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE, MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	public static ExpressionType analyzeST_GEOMETRYN(SqlExpression functionExpr, SysDatabase database)
		throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE, MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: GEOMETRY
	 */
	public static ExpressionType analyzePOSTGIS_DROPBBOX(SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE, MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: BOX3D
	 */
	public static ExpressionType analyzeST_EXTENT3D(SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as BOX3D
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOX3D_TYPE, MAX_BOX3D_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: BOX3D_EXTENT
	 */
	public static ExpressionType analyzeST_EXTENT(SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as BOX3D
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.BOX3DEXTENT_TYPE, MAX_BOX3D_EXTENT_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY Output: GEOMETRY
	 */
	public static ExpressionType analyzeST_COLLECT(SqlExpression functionExpr, SysDatabase database)
			throws ColumnNotFoundException {

		// set the return type as GEOMETRY
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.GEOMETRY_TYPE, MAX_GEOMETRY_TEXT_LEN, 0, 0);
		return exprT;
	}

	/**
	 * Input:GEOMETRY, VARCHAR Output: VARCHAR
	 */
	public static ExpressionType analyzeST_GEOMETRYTYPE(SqlExpression functionExpr,
			SysDatabase database) throws ColumnNotFoundException {

		// set the return type as VARCHAR
		ExpressionType exprT = new ExpressionType();
		exprT.setExpressionType(ExpressionType.VARCHAR_TYPE, 4000, 0, 0);
		return exprT;
	}

}
