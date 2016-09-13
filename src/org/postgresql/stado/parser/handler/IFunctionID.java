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

/**
 * This interface holds ID's for all the Funtions that we support. Please change
 * the MAXID in this file so that we dont assign the same ID to two function.
 */
public interface IFunctionID {
    /**
     * Please update this variable as you bump the numbers up.
     */
    public static int MAXID = 137;

    /**
     * Date Time Function.
     */
    public static int YEAR_ID = 11;

    public static int MONTH_ID = 15;

    public static int MINUTE_ID = 16;

    public static int SECOND_ID = 18;

    public static int USER_ID = 19;

    public static int SUBSTRING_ID = 20;

    public static int TIMESTAMP_ID = 21;

    public static int TRUNC_ID = 23;

    public static int BINARY_ID = 24;

    public static int ADDDATE_ID = 27;

    public static int ADDTIME_ID = 28;

    public static int DATE_ID = 29;

    public static int DATEDIFF_ID = 30;

    public static int DAY_ID = 31;

    public static int DAYNAME_ID = 32;

    public static int DAYOFMONTH_ID = 33;

    public static int DAYOFWEEK_ID = 34;

    public static int DAYOFYEAR_ID = 35;

    public static int MONTHNAME_ID = 36;

    public static int SUBDATE_ID = 37;

    public static int SUBTIME_ID = 38;

    public static int TIME_ID = 39;

    public static int WEEKOFYEAR_ID = 40;

    public static int HOUR_ID = 42;

    // Arthematic Functions
    public static int ABS_ID = 0;

    public static int CEIL_ID = 43;

    public static int EXP_ID = 44;

    public static int FLOOR_ID = 45;

    public static int LN_ID = 46;

    public static int LOG_ID = 47;

    public static int PI_ID = 48;

    public static int POWER_ID = 49;

    public static int SIGN_ID = 50;

    public static int ASIN_ID = 51;

    public static int ATAN_ID = 52;

    public static int COS_ID = 53;

    public static int COT_ID = 54;

    public static int DEGREES_ID = 55;

    public static int RADIANS_ID = 56;

    public static int SIN_ID = 57;

    public static int TAN_ID = 58;

    public static int ROUND_ID = 59;

    public static int ACOS_ID = 87;

    public static int LOG10_ID = 88;

    public static int MOD_ID = 89;

    public static int SQRT_ID = 90;

    public static int COSH_ID = 91;

    public static int FLOAT_ID = 93;

    public static int GREATEST_ID = 94;

    public static int LEAST_ID = 95;

    public static int ATAN2_ID = 96;

    public static int ATN2_ID = 97;

    // String Functions
    public static int ASCII_ID = 60;

    public static int INDEX_ID = 61;

    public static int LEFT_ID = 62;

    public static int LENGTH_ID = 63;

    public static int LOWER_ID = 64;

    public static int LPAD_ID = 65;

    public static int LTRIM_ID = 66;

    public static int REPLACE_ID = 67;

    public static int RIGHT_ID = 68;

    public static int RPAD_ID = 69;

    public static int RTRIM_ID = 70;

    public static int SUBSTR_ID = 71;

    public static int TRIM_ID = 72;

    public static int UPPER_ID = 73;

    public static int INSTR_ID = 74;

    public static int SOUNDEX_ID = 98;

    public static int INITCAP_ID = 99;

    public static int LFILL_ID = 100;

    public static int MAPCHAR_ID = 101;

    public static int NUM_ID = 102;

    public static int CONCAT_ID = 104;

    public static int CUSTOM_ID = 105;

    public static int CAST_ID = 106;

    // For Postgres
    public static int TIMEOFDAY_ID = 107;

    public static int ISFINITE_ID = 108;

    public static int EXTRACT_ID = 109;

    public static int DATETRUNC_ID = 110;

    public static int DATEPART_ID = 111;

    public static int AGE_ID = 112;

    public static int BITLENGTH_ID = 118;

    public static int CHARLENGTH_ID = 119;

    public static int CONVERT_ID = 120;

    public static int OCTETLENGTH_ID = 121;

    public static int OVERLAY_ID = 122;

    public static int POSITION_ID = 123;

    public static int TO_HEX_ID = 124;

    public static int QUOTE_LITERAL_ID = 125;

    public static int QUOTE_IDENT_ID = 126;

    public static int PG_CLIENT_ENCODING_ID = 127;

    public static int MD5_ID = 128;

    public static int CHR_ID = 129;

    public static int TRANSLATE_ID = 130;

    public static int TO_ASCII_ID = 131;

    public static int STRPOS_ID = 132;

    public static int SPLIT_PART_ID = 133;

    public static int REPEAT_ID = 134;

    public static int ENCODE_ID = 135;

    public static int DECODE_ID = 136;

    public static int BTRIM_ID = 137;

    public static int WIDTH_BUCKET_ID = 138;

    public static int SETSEED_ID = 139;

    public static int RANDOM_ID = 140;

    public static int CBRT_ID = 141;
    public static int GETBIT_ID = 142 ;
    public static int CLOCK_TIMESTAMP_ID = 143 ;
    public static int STATEMENT_TIMESTAMP_ID = 144 ;
    public static int TRANSACTION_TIMESTAMP_ID = 145 ;
    public static int GETBYTE_ID = 146 ;
    public static int TODATE_ID = 147 ;
    public static int NULLIF_ID = 148 ;
    public static int SETBIT_ID = 149 ;
    public static int SETBYTE_ID = 150 ;
    public static int TOCHAR_ID = 151 ;
    public static int TONUMBER_ID = 152 ;
    public static int TOTIMESTAMP_ID = 153 ;
    public static int ADDMONTHS_ID = 154 ;
    public static int JUSTIFYDAYS_ID = 155 ;
    public static int JUSTIFYHOURS_ID = 156 ;
    public static int JUSTIFYINTERVAL_ID = 157 ;
    public static int LASTDAY_ID = 158 ;
    public static int MONTHSBETWEEN_ID = 159 ;
    public static int NEXTDAY_ID = 160 ;
    public static int CURRENTDATABASE_ID = 161 ;
    public static int CURRENTSCHEMA_ID = 162 ;
    public static int REGEXPREPLACE_ID = 185;
    public static int COALESCE_ID = 188;
    public static int CURRENTDATE_ID = 189;
    public static int CURRENTTIME_ID = 190;
    public static int CURRENTTIMESTAMP_ID = 191;
    public static int CURRENTUSER_ID = 192;

    // Aggreagate Functions
    public static int AVG_ID = 75;

    public static int COUNT_ID = 76;

    public static int COUNT_STAR_ID = 77;

    public static int STDEV_ID = 78;

    public static int VARIANCE_ID = 79;

    public static int MAX_ID = 80;

    public static int MIN_ID = 81;

    public static int SUM_ID = 82;

    public static int BITAND_ID = 164;

    public static int BITOR_ID = 165;

    public static int BOOLAND_ID = 166;

    public static int BOOLOR_ID = 167;

    public static int EVERY_ID = 168;

    // Statistical Aggreagate Functions
    public static int CORR_ID = 169;

    public static int COVARPOP_ID = 170;

    public static int COVARSAMP_ID = 171;

    public static int REGRAVX_ID = 172;

    public static int REGRAVY_ID = 173;

    public static int REGRCOUNT_ID = 174;

    public static int REGRINTERCEPT_ID = 175;

    public static int REGRR2_ID = 176;

    public static int REGRSLOPE_ID = 177;

    public static int REGRSXX_ID = 178;

    public static int REGRSXY_ID = 179;

    public static int REGRSYY_ID = 180;

    public static int STDEVPOP_ID = 181;

    public static int STDEVSAMP_ID = 182;

    public static int VARIANCEPOP_ID = 183;

    public static int VARIANCESAMP_ID = 184;
    
    // inet and cidr functions
    public static int ABBREV_ID = 193;
    
    public static int BROADCAST_ID = 194;
    
    public static int FAMILY_ID = 195;
    
    public static int HOST_ID = 196;
    
    public static int HOSTMASK_ID = 197;
    
    public static int MASKLEN_ID = 198;
    
    public static int NETMASK_ID = 199;
    
    public static int NETWORK_ID = 200;
    
    public static int SET_MASKLEN_ID = 201;
    
    public static int TEXT_ID = 202;
    
    // Misc Functions

    public static int VERSION_ID = 84;

    public static int VALUE_ID = 85;

    public static int CASE_ID = 86;

	// Spatial Functions
	public static int ST_ASTEXT_ID = 300;

	public static int ST_TRANSFORM_ID = 301;

	public static int ST_DISTANCE_ID = 302;

	public static int ST_DWITHIN_ID = 303;

	public static int ST_LENGTH_ID = 304;

	public static int ST_AREA_ID = 305;

	public static int ST_ASBINARY_ID = 306;

	public static int ST_BOUNDARY_ID = 307;

	public static int ST_BUFFER_ID = 308;

	public static int ST_CENTROID_ID = 309;


	public static int ST_ADDMEASURE_ID = 310;
	public static int ST_ADDPOINT_ID = 311;
	public static int ST_AFFINE_ID = 312;
	public static int ST_AREA2D_ID = 313;
	public static int ST_ASEWKB_ID = 314;
	public static int ST_ASEWKT_ID = 315;
	public static int ST_ASGEOJSON_ID = 316;
	public static int ST_ASGML_ID = 317;
	public static int ST_ASHEXEWKB_ID = 318;
	public static int ST_ASKML_ID = 319;
	public static int ST_ASSVG_ID = 320;
	public static int ST_AZIMUTH_ID = 321;
	public static int ST_BDMPOLYFROMTEXT_ID = 322;
	public static int ST_BDPOLYFROMTEXT_ID = 323;
	public static int ST_BUILDAREA_ID = 324;
	public static int ST_BYTEA_ID = 325;
	public static int ST_CHIP_IN_ID = 326;
	public static int ST_CHIP_OUT_ID = 327;
	public static int ST_CLOSESTPOINT_ID = 328;
	public static int ST_COLLECTIONEXTRACT_ID = 329;
	public static int ST_COMBINE_BBOX_ID = 330;
	public static int ST_COMPRESSION_ID = 331;
	public static int ST_CONTAINS_ID = 332;
	public static int ST_CONTAINSPROPERLY_ID = 333;
	public static int ST_CONVEXHULL_ID = 334;
	public static int ST_COORDDIM_ID = 335;
	public static int ST_COVEREDBY_ID = 336;
	public static int ST_COVERS_ID = 337;
	public static int ST_CROSSES_ID = 338;
	public static int ST_CURVETOLINE_ID = 339;
	public static int ST_DATATYPE_ID = 340;
	public static int ST_DFULLYWITHIN_ID = 341;
	public static int ST_DIFFERENCE_ID = 342;
	public static int ST_DIMENSION_ID = 343;
	public static int ST_DISJOINT_ID = 344;
	public static int ST_DISTANCE_SPHERE_ID = 345;
	public static int ST_DISTANCE_SPHEROID_ID = 346;
	public static int ST_ENDPOINT_ID = 347;
	public static int ST_ENVELOPE_ID = 348;
	public static int ST_EQUALS_ID = 349;
	public static int ST_EXTERIORRING_ID = 350;
	public static int ST_FACTOR_ID = 351;
	public static int ST_FIND_EXTENT_ID = 352;
	public static int ST_FORCERHR_ID = 353;
	public static int ST_FORCE_2D_ID = 354;
	public static int ST_FORCE_3D_ID = 355;
	public static int ST_FORCE_3DM_ID = 356;
	public static int ST_FORCE_3DZ_ID = 357;
	public static int ST_FORCE_4D_ID = 358;
	public static int ST_FORCE_COLLECTION_ID = 359;
	public static int ST_GEOGFROMTEXT_ID = 360;
	public static int ST_GEOGFROMWKB_ID = 361;
	public static int ST_GEOGRAPHYFROMTEXT_ID = 362;
	public static int ST_GEOHASH_ID = 363;
	public static int ST_GEOMCOLLFROMTEXT_ID = 364;
	public static int ST_GEOMCOLLFROMWKB_ID = 365;
	public static int ST_GEOMFROMEWKB_ID = 366;
	public static int ST_GEOMFROMEWKT_ID = 367;
	public static int ST_GEOMFROMGML_ID = 368;
	public static int ST_GEOMFROMKML_ID = 369;
	public static int ST_GEOMFROMTEXT_ID = 370;
	public static int ST_GEOMFROMWKB_ID = 371;
	public static int ST_GMLTOSQL_ID = 372;
	public static int ST_HASARC_ID = 373;
	public static int ST_HAUSDORFFDISTANCE_ID = 374;
	public static int ST_HEIGHT_ID = 375;
	public static int ST_INTERIORRINGN_ID = 376;
	public static int ST_INTERSECTION_ID = 377;
	public static int ST_INTERSECTS_ID = 378;
	public static int ST_ISCLOSED_ID = 379;
	public static int ST_ISEMPTY_ID = 380;
	public static int ST_ISRING_ID = 381;
	public static int ST_ISSIMPLE_ID = 382;
	public static int ST_ISVALID_ID = 383;
	public static int ST_ISVALIDREASON_ID = 384;
	public static int ST_LENGTH2D_ID = 385;
	public static int ST_LENGTH2D_SPHEROID_ID = 386;
	public static int ST_LENGTH3D_ID = 387;
	public static int ST_LENGTH3D_SPHEROID_ID = 388;
	public static int ST_LENGTH_SPHEROID_ID = 389;
	public static int ST_LINECROSSINGDIRECTION_ID = 390;
	public static int ST_LINEFROMMULTIPOINT_ID = 391;
	public static int ST_LINEFROMTEXT_ID = 392;
	public static int ST_LINEFROMWKB_ID = 393;
	public static int ST_LINEMERGE_ID = 394;
	public static int ST_LINESTRINGFROMWKB_ID = 395;
	public static int ST_LINETOCURVE_ID = 396;
	public static int ST_LINE_INTERPOLATE_POINT_ID = 397;
	public static int ST_LINE_LOCATE_POINT_ID = 398;
	public static int ST_LINE_SUBSTRING_ID = 399;
	public static int ST_LOCATEBETWEENELEVATIONS_ID = 400;
	public static int ST_LOCATE_ALONG_MEASURE_ID = 401;
	public static int ST_LOCATE_BETWEEN_MEASURES_ID = 402;
	public static int ST_LONGESTLINE_ID = 403;
	public static int ST_M_ID = 404;
	public static int ST_MAKEENVELOPE_ID = 405;
	public static int ST_MAKEPOINT_ID = 406;
	public static int ST_MAKEPOINTM_ID = 407;
	public static int ST_MAKEPOLYGON_ID = 408;
	public static int ST_MAXDISTANCE_ID = 409;
	public static int ST_MEMCOLLECT_ID = 410;
	public static int ST_MEM_SIZE_ID = 411;
	public static int ST_MINIMUMBOUNDINGCIRCLE_ID = 412;
	public static int ST_MLINEFROMTEXT_ID = 413;
	public static int ST_MLINEFROMWKB_ID = 414;
	public static int ST_MPOINTFROMTEXT_ID = 415;
	public static int ST_MPOINTFROMWKB_ID = 416;
	public static int ST_MPOLYFROMTEXT_ID = 417;
	public static int ST_MPOLYFROMWKB_ID = 418;
	public static int ST_MULTI_ID = 419;
	public static int ST_MULTILINEFROMWKB_ID = 420;
	public static int ST_MULTILINESTRINGFROMTEXT_ID = 421;
	public static int ST_MULTIPOINTFROMTEXT_ID = 422;
	public static int ST_MULTIPOINTFROMWKB_ID = 423;
	public static int ST_MULTIPOLYFROMWKB_ID = 424;
	public static int ST_MULTIPOLYGONFROMTEXT_ID = 425;
	public static int ST_NDIMS_ID = 426;
	public static int ST_NPOINTS_ID = 427;
	public static int ST_NRINGS_ID = 428;
	public static int ST_NUMGEOMETRIES_ID = 429;
	public static int ST_NUMINTERIORRING_ID = 430;
	public static int ST_NUMINTERIORRINGS_ID = 431;
	public static int ST_NUMPOINTS_ID = 432;
	public static int ST_ORDERINGEQUALS_ID = 433;
	public static int ST_OVERLAPS_ID = 434;
	public static int ST_PERIMETER_ID = 435;
	public static int ST_PERIMETER2D_ID = 436;
	public static int ST_PERIMETER3D_ID = 437;
	public static int ST_POINT_ID = 438;
	public static int ST_POINTFROMTEXT_ID = 439;
	public static int ST_POINTFROMWKB_ID = 440;
	public static int ST_POINTN_ID = 441;
	public static int ST_POINTONSURFACE_ID = 442;
	public static int ST_POINT_INSIDE_CIRCLE_ID = 443;
	public static int ST_POLYFROMTEXT_ID = 444;
	public static int ST_POLYFROMWKB_ID = 445;
	public static int ST_POLYGON_ID = 446;
	public static int ST_POLYGONFROMTEXT_ID = 447;
	public static int ST_POLYGONFROMWKB_ID = 448;
	public static int ST_POSTGIS_GIST_JOINSEL_ID = 449;
	public static int ST_POSTGIS_GIST_SEL_ID = 450;
	public static int ST_RELATE_ID = 451;
	public static int ST_REMOVEPOINT_ID = 452;
	public static int ST_REVERSE_ID = 453;
	public static int ST_ROTATE_ID = 454;
	public static int ST_ROTATEX_ID = 455;
	public static int ST_ROTATEY_ID = 456;
	public static int ST_ROTATEZ_ID = 457;
	public static int ST_SCALE_ID = 458;
	public static int ST_SEGMENTIZE_ID = 459;
	public static int ST_SETFACTOR_ID = 460;
	public static int ST_SETPOINT_ID = 461;
	public static int ST_SETSRID_ID = 462;
	public static int ST_SHIFT_LONGITUDE_ID = 463;
	public static int ST_SHORTESTLINE_ID = 464;
	public static int ST_SIMPLIFY_ID = 465;
	public static int ST_SIMPLIFYPRESERVETOPOLOGY_ID = 466;
	public static int ST_SNAPTOGRID_ID = 467;
	public static int ST_SRID_ID = 468;
	public static int ST_STARTPOINT_ID = 469;
	public static int ST_SUMMARY_ID = 470;
	public static int ST_SYMDIFFERENCE_ID = 471;
	public static int ST_SYMMETRICDIFFERENCE_ID = 472;
	public static int ST_TEXT_ID = 473;
	public static int ST_TOUCHES_ID = 474;
	public static int ST_TRANSLATE_ID = 475;
	public static int ST_TRANSSCALE_ID = 476;
	public static int ST_WIDTH_ID = 477;
	public static int ST_WITHIN_ID = 478;
	public static int ST_WKBTOSQL_ID = 479;
	public static int ST_WKTTOSQL_ID = 480;
	public static int ST_X_ID = 481;
	public static int ST_Y_ID = 482;
	public static int ST_Z_ID = 483;
	public static int ST_ZMFLAG_ID = 484;

	public static int ST_BOX2D_ID = 485;
	public static int ST_BOX3D_ID = 486;
	public static int ST_GEOMETRY_ID = 487;
	public static int POSTGIS_DROPBBOX_ID = 488;
	public static int ST_EXTENT_ID = 489;
	public static int ST_EXTENT3D_ID = 490;
	public static int ST_COLLECT_ID = 491;
	public static int ST_GEOMETRYTYPE_ID = 492;
	public static int ST_GEOMETRYN_ID = 493;
	public static int ST_COLLECT_AGG_ID = 494;
    
}
