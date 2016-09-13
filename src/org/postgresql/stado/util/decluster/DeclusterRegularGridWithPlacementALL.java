/*
 * Copyright (c) 2016 Regents of the University of Minnesota
 *
 * This file is part of the Minnesota Population Center's Terra Populus project.
 * For copyright and licensing information, see the NOTICE and LICENSE files
 * in this project's top-level directory, and also online at:
 * https://github.com/mnpopcenter/stado
 */
package org.postgresql.stado.util.decluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.ResourceBundle;
import java.util.StringTokenizer;


import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;


public class DeclusterRegularGridWithPlacementALL {


	static final int DECIMAL_PRECISION  = 6;
	static final String SHARDID_COLUMN_NAME = "shardid";
	
	
	// texas 
	//static final String[] tables = {"arealm_merge", "edges_merge",  "areawater_merge"};
	//static  final String SPLIT_ITERATE_PRIMARY_TABLE_NAME = "edges_merge";   
	//static  	  String SPLIT_ITERATE_SECONDARY_TABLE_NAME = "arealm_merge";	
	
	// california
	static final String[] tables = {"arealm_merge_ca", "edges_merge_ca", "areawater_merge_ca"};

	static  final String SPLIT_ITERATE_PRIMARY_TABLE_NAME = "edges_merge_ca";   
	static  	  String SPLIT_ITERATE_SECONDARY_TABLE_NAME = "arealm_merge_ca";	
	//static  final String SPLIT_ITERATE_PRIMARY_TABLE_NAME = "areawater_merge_ca";
	//static  String SPLIT_ITERATE_SECONDARY_TABLE_NAME = "areawater_merge_ca";	
	
	static  final String SPLIT_ITERATE_TABLE_NAME = "edges_merge_ca";
	// static  final String SPLIT_ITERATE_TABLE_NAME = "areawater_merge_ca";
	
	//us
	//static final String[] tables = {"arealm_merge_us", "edges_merge_us",  "areawater_merge_us"};
	//// static  final String SPLIT_ITERATE_PRIMARY_TABLE_NAME = "edges_merge_us";
	//// static  String SPLIT_ITERATE_SECONDARY_TABLE_NAME = "arealm_merge_us";	
	//static  final String SPLIT_ITERATE_PRIMARY_TABLE_NAME = "areawater_merge_us";
	//static  	  String SPLIT_ITERATE_SECONDARY_TABLE_NAME = "areawater_merge_us";	

	//static  final String SPLIT_ITERATE_TABLE_NAME = "edges_merge_us";
	//static  final String SPLIT_ITERATE_TABLE_NAME = "areawater_merge_us";
		
	static final int SRID= 4326; //4326; 32633	
	static final int LIMIT = 20000;

	
	// 1000 partitions	
	//static final int MAX_RECORDS_PER_TILE = 	1000; // 1000 for edges, 100 for areawater_us, 15 for areawater_ca
	//static int MAX_NUMBER_BUCKETS = 996; //996 
	//static final int MAX_ITERATIONS=5;
	
	static int incremTileCounter =0;
	
	static HashMap<String, Envelope> MBR_HS = null;
	static WKBReader wkbreader = new WKBReader();
	static Envelope globalMBR = null;
	static TileList globalTileList = null;
	
	
	static LinkedHashMap<String, Integer> serverDescs = null; 

	static Connection mConn = null;
	
	static int mGridDimension = -1;
	static int mTableId = -1;
	static String mPartId = "_new";
	static String mSERVER_DESC_FILE_NAME="";
	static String mPATH="./";
	
	static String mDestDbTableName = null;
	static String mDestJdbcURL = null;
	static String mSrcDbTableName = null;
	
	private static String stmtTerminator = "";
	//Global MBR for table edges_merge :Env[-106.645646 : -93.508039, 25.837164 : 36.500704]
	//Global MBR for table arealm_merge : X=-93.607151 Y=36.489345 ,x=-106.605494 y=25.895032999999998
	                     
	/**
	 *  Example: 
	 *  cd /home/suprio/eclipse/workspace/NebulaDB/bin
	 *  java -Xmx2000m -cp .:../lib/postgresql-8.4-703.jdbc4.jar:../lib/jdbm-2.1.jar  edu.toronto.cs.nebuladb.decluster.DeclusterRegularGridWithPlacementALL  -d 32 -t 0  -s _shall  -f SERVER_DESC -p /home/suprio/eclipse/workspace/NebulaDB/ -T arealm_merge_ca_shall -J jdbc:postgresql://10.70.20.5:5432/dbgeo?user=dbuser&password=DBUserPassword
	 *
	 *  Normal: based on the line table:  -t 0  -s _part  -f SERVER_DESC -p /home/suprio/eclipse/workspace/NebulaDB/
	 *  Exception: based on the areawater_merge_us table: -t 2  -s _shard  -f SERVER_DESC -p /home/suprio/eclipse/workspace/NebulaDB/
	 */
	public static void main(String args[]) {   
		
		if (args.length < 8) {
			System.out.println("Command: DeclusterRegularGridWithPlacementALL -d <grid dimension d x d>  -t <table id>   -s <partition suffix>   -f <server desc. file>  -p <home dir path with / at end>  -T  <dest table name>  -J <dest JDBC URL>");
			System.out.println("Available table ids:-");
			System.out.println("	arealm_merge:    0 ");
			System.out.println("	edges_merge:     1 ");
			System.out.println("	areawater_merge: 2 ");
			
			System.out.println(args.length +"   "+ args[0]);
			return;
		}
		else {
			int tableIdIndex=-1;
			int partitionSuffixIndex=-1;
			int serverDescFileIndex=-1;
			int homeDirPathIndex = -1;
			int gridDimensionIndex = -1;
			int destDbTableNameIndex = -1;
			int destJdbcURLIndex = -1;
			
			for (int i=0;i<args.length;i++) {
				if (args[i].equals("-d")) {
					gridDimensionIndex = i+1;
				}
				else if (args[i].equals("-t")) {
					tableIdIndex = i+1;
				}
				else if (args[i].equals("-s")) {
					partitionSuffixIndex = i+1;
				}
				else if (args[i].equals("-f")) {
					serverDescFileIndex = i+1;
				}
				else if (args[i].equals("-p")) {
					homeDirPathIndex = i+1;
				}
				else if (args[i].equals("-T")) {
					destDbTableNameIndex = i+1;
				}
				else if (args[i].equals("-J")) {
					destJdbcURLIndex = i+1;
				}
			}
			
			try {
		
				mGridDimension = Integer.parseInt(args[gridDimensionIndex]);
				
				mTableId =  Integer.parseInt(args[tableIdIndex]);
				mSrcDbTableName = tables[mTableId];
					
				//tables
				if (!(mTableId >= 0 && mTableId < tables.length)) {
					System.out.println("Table id out of range");
					System.out.println("Available table ids:-");
					System.out.println("	arealm_merge:    0 ");
					System.out.println("	edges_merge:     1 ");
					System.out.println("	areawater_merge: 2 ");
					return;	
				}
				
				mPartId = args[partitionSuffixIndex];
				
				mSERVER_DESC_FILE_NAME = args[serverDescFileIndex];				
				mPATH = args[homeDirPathIndex];
				
				File tryF = new File(mPATH+mSERVER_DESC_FILE_NAME);
				if (!tryF.exists()) {
					System.out.println("File " +mPATH+mSERVER_DESC_FILE_NAME+ " does not exist");
					return;
				}
				
				mDestDbTableName = args[destDbTableNameIndex];
				mDestJdbcURL = args[destJdbcURLIndex];
				
			}
			catch (Exception e) {
				e.printStackTrace();
				
				return;
			}
				
			System.out.println("Partitioning "+ tables[mTableId] + " with partition suffix="+mPartId+ " using server description file= "+mPATH+mSERVER_DESC_FILE_NAME+ " and home dir path=" +  mPATH);
			
			MBR_HS = new HashMap<String, Envelope>();
			(new DeclusterRegularGridWithPlacementALL()).decluster();
		}
	}
	
	/*
	 * Source: http://stackoverflow.com/questions/14845937/java-how-to-set-precision-for-double-value
	 */
	double truncate(double number, int precision) {
	    double prec = Math.pow(10, precision);
	    int integerPart = (int) number;
	    double fractionalPart = number - integerPart;
	    fractionalPart *= prec;
	    int fractPart = (int) fractionalPart;
	    fractionalPart = (double) (integerPart) + (double) (fractPart)/prec;
	    return fractionalPart;
	}
	
	double truncateBigDecimal(double number, int precision) {
		BigDecimal bd1 = new BigDecimal(number);
        BigDecimal bd2 = bd1.setScale(precision , BigDecimal.ROUND_CEILING); 
        return bd2.doubleValue();
	}
	
	public void decluster() {
		
		long startTime = System.currentTimeMillis();
		
		//first  create global MBR
		initTableMBRs();
		for (int t=0; t<tables.length;t++) {
			createGlobalMBR(tables[t]);
		}
		initServerDescriptions();	
		
		//then split the tiles and impose Hilbert order
		splitIntoRegularGridTiles();
		
		// next, proportional range partition: initPartitionMBRs and distribute the partition based on 
		//initPartitionMBRs(); // don't call if splitIntoRegularGridTiles() is invoked before
		
		proportionallyAssociateTiles2Servers(); // this is needed
		
		
		parallelDistributeAllTiles2Servers4TableWithBucketid(tables[mTableId]);
		
		if (mConn != null)
			try {
				mConn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		long endTime = System.currentTimeMillis();
		System.out.println("Time taken to decluster (sec):" + (endTime-startTime)/1000);
	}
	

	
	void splitIntoRegularGridTiles() {
		if (globalTileList == null) {
			globalTileList = new TileList();

		}
		//globalTileList.init(); // no need to init
		
		double globalXLen = globalMBR.getMaxX()  - globalMBR.getMinX();
		double globalYLen = globalMBR.getMaxY()  - globalMBR.getMinY();
			
		double unitXLen =  globalXLen/mGridDimension;
		double unitYLen =  globalYLen/mGridDimension;
		
		System.out.println(": "+ globalMBR.getMinX() + " , "+ globalMBR.getMaxX()  + " ; "+ globalMBR.getMinY() + ", "+ globalMBR.getMaxY());
		
		int totalGridCells=0;
		
		int numRows =0, numCols=0;
		
	
		// row
		for (double y=globalMBR.getMinY();y<=globalMBR.getMaxY() ;y=y+unitYLen) {
			y = truncateBigDecimal(y,DECIMAL_PRECISION);
			
			// column
			numCols =0;
			for (double x=globalMBR.getMinX();x<globalMBR.getMaxX();x=x+unitXLen) {
				x = truncateBigDecimal(x,DECIMAL_PRECISION);
				incremTileCounter++;
				Tile tile = new Tile();
				tile.setTileId(incremTileCounter);
				tile.setLevel(0);
				Envelope tileEnv = new Envelope(x, x+unitXLen, y, y+unitYLen);
				tile.setEnvelope(tileEnv);
				tile.setEltCount(getTileEltCount(tileEnv,mSrcDbTableName));
								
				tile.setOrderStr("1");
				tile.setOrientation(0);
				//globalTileList.insert(incremTileCounter,tile);
				globalTileList.add(tile);
				totalGridCells++;
				
				numCols++;
			}
			numRows++;
		}

		System.out.println("Total number of grid cells:" + totalGridCells + "  Num of cols:" + numCols + "  Num of rows:" + numRows);
		
		//globalTileList.generatePartitionMBRFile();  
		//globalTileList.printList2File();
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	int getTileEltCount(Envelope env, String tableName)  {
		int cnt =0;
		try {
			if (mConn == null) {
				if (serverDescs != null) {
					Iterator it = serverDescs.keySet().iterator();
					if (it.hasNext()) { //take the first server from serverDescs and connect to it
						String serverIP = (String) it.next();
						mConn = getConnection4Server(serverIP);
					}
				}
			}
			Statement stmt = mConn.createStatement();
			
			double ht = env.getHeight();
			double wd = env.getWidth();
			double maxX = env.getMaxX();
			double maxY =  env.getMaxY();
			double minX = env.getMinX();
			double minY =  env.getMinY();
			
			double x = minX;
			double y = minY;
			String poly = x +" "+ y + ", ";
			
			x = maxX;
			y = minY;
			poly += x +" "+ y + ", ";
			
			x = maxX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = minY;
			poly += x +" "+ y ;
			
			String sqlStmt = "select count(*) from " + tableName+ " where ST_Intersects(geom, ST_PolygonFromText('POLYGON((" + poly + "))',"+SRID+"))";
			//sqlStmt.replaceAll(",", "////");
			
			ResultSet rs =stmt.executeQuery(sqlStmt);
			while (rs.next()) { 
				cnt = rs.getInt(1);
				//System.out.println("table has records " + cnt + " ---> sql="+ sqlStmt);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return cnt;
	}
	
	int getTileEltCount(Connection conn, Envelope env, String tableName)  {
		int cnt =0;
		try {
			if (conn == null) {
				System.err.println("getTileEltCount: null connection");
				return -1;
			}
			
			Statement stmt = conn.createStatement();
			
			double ht = env.getHeight();
			double wd = env.getWidth();
			double maxX = env.getMaxX();
			double maxY =  env.getMaxY();
			double minX = env.getMinX();
			double minY =  env.getMinY();
			
			double x = minX;
			double y = minY;
			String poly = x +" "+ y + ", ";
			
			x = maxX;
			y = minY;
			poly += x +" "+ y + ", ";
			
			x = maxX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = minY;
			poly += x +" "+ y ;
			
			String sqlStmt = "select count(*) from " + tableName+ " where ST_Intersects(geom, ST_PolygonFromText('POLYGON((" + poly + "))',"+SRID+"))";
			//sqlStmt.replaceAll(",", "////");
			
			ResultSet rs =stmt.executeQuery(sqlStmt);
			while (rs.next()) { 
				cnt = rs.getInt(1);
				//System.out.println("table has records " + cnt + " ---> sql="+ sqlStmt);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return cnt;
	}
	
	ResultSet getTileElementsFromTable(String tableName,Envelope env, int curr, int limit)  {
		int cnt =0;
		try {
			if (mConn == null) {
				if (serverDescs != null) {
					Iterator it = serverDescs.keySet().iterator();
					if (it.hasNext()) { //take the first server from serverDescs and connect to it
						String serverIP = (String) it.next();
						mConn = getConnection4Server(serverIP);
					}
				}
			}
			
			Statement stmt = mConn.createStatement();
			
			double ht = env.getHeight();
			double wd = env.getWidth();
			double maxX = env.getMaxX();
			double maxY =  env.getMaxY();
			double minX = env.getMinX();
			double minY =  env.getMinY();
			
			double x = minX;
			double y = minY;
			String poly = x +" "+ y + ", ";
			
			x = maxX;
			y = minY;
			poly += x +" "+ y + ", ";
			
			x = maxX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = minY;
			poly += x +" "+ y ;
			
			String sqlStmt = "select * from "+tableName+" where ST_Intersects(geom, ST_PolygonFromText('POLYGON((" + poly + "))',"+SRID+")) limit "+ limit+ " offset " + curr;
			System.out.println(sqlStmt);
			
			ResultSet rs =stmt.executeQuery(sqlStmt);
			//if (rs.next()) { 
				return rs;
			//}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	ResultSet getTilePKeysFromTable(Connection conn, String tableName,Envelope env, int curr, int limit)  {
		int cnt =0;
		try {
			if (conn == null) {
				System.err.println("getTileElementsFromTable: null connection");
				return null;
			}
			
			Statement stmt = conn.createStatement();
			
			double ht = env.getHeight();
			double wd = env.getWidth();
			double maxX = env.getMaxX();
			double maxY =  env.getMaxY();
			double minX = env.getMinX();
			double minY =  env.getMinY();
			
			double x = minX;
			double y = minY;
			String poly = x +" "+ y + ", ";
			
			x = maxX;
			y = minY;
			poly += x +" "+ y + ", ";
			
			x = maxX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = minY;
			poly += x +" "+ y ;
			
			String sqlStmt = "select gid from "+tableName+" where ST_Intersects(geom, ST_PolygonFromText('POLYGON((" + poly + "))',"+SRID+")) limit "+ limit+ " offset " + curr;
		    //System.out.println(sqlStmt);
			
			ResultSet rs =stmt.executeQuery(sqlStmt);
			//if (rs.next()) { 
				return rs;
			//}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	ResultSet getAllRecordFromTableWithinTile(Connection conn, String srcDbTable, String[] columnName, Envelope env, int curr, int limit)  {
		int cnt =0;
		try {
			if (conn == null) {
				System.err.println("getAllRecordFromTableWithinTile: null connection");
				return null;
			}
			
			Statement stmt = conn.createStatement();
			
			// read and parse a line - insert/update to db table
            String srcSqlStmt = "SELECT ";
            for (int j = 0; j < columnName.length; j++) {
            	if (j>0)
            		srcSqlStmt +=",";
            	if (columnName[j].equalsIgnoreCase("geom"))
            		srcSqlStmt +=  "ST_ASTEXT(" + columnName[j] + ") as geom ";
            	else	
            		srcSqlStmt +=  columnName[j];
            }
            srcSqlStmt+=" FROM "+srcDbTable;
           
			
			double ht = env.getHeight();
			double wd = env.getWidth();
			double maxX = env.getMaxX();
			double maxY =  env.getMaxY();
			double minX = env.getMinX();
			double minY =  env.getMinY();
			
			double x = minX;
			double y = minY;
			String poly = x +" "+ y + ", ";
			
			x = maxX;
			y = minY;
			poly += x +" "+ y + ", ";
			
			x = maxX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = maxY;
			poly += x +" "+ y + ", ";
			
			x = minX;
			y = minY;
			poly += x +" "+ y ;
			
			srcSqlStmt += " where ST_Intersects(geom, ST_PolygonFromText('POLYGON((" + poly + "))',"+SRID+")) limit "+ limit+ " offset " + curr;
		    //System.out.println(srcSqlStmt);
			
			ResultSet rs =stmt.executeQuery(srcSqlStmt); 
			return rs;
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	void initTableMBRs() {
		
		File fl = new File (mPATH+"Table_MBRs");
	
		try {
			BufferedReader  fi = new BufferedReader(new FileReader(fl));
			String line ="";
			while ((line = fi.readLine()) != null) {
				System.out.println("line =" + line);
				StringTokenizer st = new StringTokenizer(line,":");
				String tableName = st.nextToken().trim();
				String rest = st.nextToken().trim();
				
				st = new StringTokenizer(rest,";");
				String Xs = st.nextToken().trim();
				String Ys = st.nextToken().trim();
				
				st = new StringTokenizer(Xs,",");
				double minX = Double.parseDouble(st.nextToken().trim());
				double maxX = Double.parseDouble(st.nextToken().trim());
				
				st = new StringTokenizer(Ys,",");
				double minY = Double.parseDouble(st.nextToken().trim());
				double maxY = Double.parseDouble(st.nextToken().trim());
				
				Envelope env = new Envelope(minX,maxX,minY,maxY);
				MBR_HS.put(tableName, env);
				System.out.println("Initialized MBR for table " + tableName);
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			System.out.println("File not found: "+ mPATH+"Table_MBRs");
			e1.printStackTrace();
		}
		
	}
	
	void initPartitionMBRs() {
		
		globalTileList = new TileList();
		
		File fl = new File (mPATH+"Partition_MBRs");
		System.out.println("\n\n #####  init Partition MBRs from "+ fl.getAbsolutePath());
		
Envelope totalEnv = new Envelope();
int totalEltCount=0;
		try {
			BufferedReader  fi = new BufferedReader(new FileReader(fl));
			String line ="";
			while ((line = fi.readLine()) != null) {
				//System.out.println("line =" + line);
				StringTokenizer st = new StringTokenizer(line,":");
				String orderStr = st.nextToken().trim();
				String tileId = st.nextToken().trim();
				String eltCount = st.nextToken().trim();
				String rest = st.nextToken().trim();
totalEltCount+=Integer.parseInt(eltCount);
				st = new StringTokenizer(rest,";");
				String Xs = st.nextToken().trim();
				String Ys = st.nextToken().trim();
				
				st = new StringTokenizer(Xs,",");
				double minX = Double.parseDouble(st.nextToken().trim());
				double maxX = Double.parseDouble(st.nextToken().trim());
				
				st = new StringTokenizer(Ys,",");
				double minY = Double.parseDouble(st.nextToken().trim());
				double maxY = Double.parseDouble(st.nextToken().trim());
				
				Envelope env = new Envelope();
				
				Tile tile = new Tile();
				tile.setOrderStr(orderStr);
				tile.setTileId(Integer.parseInt(tileId));
				tile.setEltCount(Integer.parseInt(eltCount));
				Envelope tileEnv = new Envelope(minX, maxX, minY, maxY);
				tile.setEnvelope(tileEnv);
totalEnv.expandToInclude(tileEnv);	
				globalTileList.add(tile);
				// System.out.println("Initialized Tile with orderStr " + orderStr + " totalEltCount:" + totalEltCount);
			} // end while
			

System.out.println(" totalEltCount:" + totalEltCount + " ::: total MBR:  X="  + totalEnv.getMaxX() + " Y=" + totalEnv.getMaxY() + " ,x=" + totalEnv.getMinX() + " y=" + totalEnv.getMinY());
String verifyURL = "http://www.openstreetmap.org/?minlon="+totalEnv.getMinX()+"&minlat="+totalEnv.getMinY()+"&maxlon="+totalEnv.getMaxX()+"&maxlat="+totalEnv.getMaxY()+"&box=yes";
System.out.println("verifyURL:"+ verifyURL);

		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
		//globalTileList.printList();
	}


	
	void initServerDescriptions() {
		
		serverDescs = new LinkedHashMap(); 
		File fl = new File (mPATH+mSERVER_DESC_FILE_NAME);
		
		try {
			BufferedReader  fi = new BufferedReader(new FileReader(fl));
			String line ="";
			while ((line = fi.readLine()) != null) {
				System.out.println("line =" + line);
				StringTokenizer st = new StringTokenizer(line,":");
				String serverIP = st.nextToken().trim();
				String capacityRatio = st.nextToken().trim();
				serverDescs.put(serverIP, Integer.parseInt(capacityRatio));
			}
		
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	void proportionallyAssociateTiles2Servers() {
		int totalProportion = 0;
		if (serverDescs != null) {
			Iterator it = serverDescs.keySet().iterator();
			while (it.hasNext()) {
				String key = (String) it.next();
				int proportion = serverDescs.get(key);
				totalProportion += proportion;
			}
			
		}
		long numTiles2Assigned=0;
		long totalTileCount = globalTileList.getTotalTileCount();
		Iterator gtIt = globalTileList.getIterator();
		
		if (serverDescs != null) {
			Iterator it = serverDescs.keySet().iterator();
			while (it.hasNext()) {
				String serverIP = (String) it.next();
				int proportion = serverDescs.get(serverIP);
				//long numTiles2Associate = Math.round ( ((double) totalTileCount * proportion/(double)totalProportion ) );
				long numTiles2Associate = (long) ( ((double) totalTileCount * proportion/(double)totalProportion ) );
				//CERE
				numTiles2Assigned = numTiles2Assigned + numTiles2Associate;
				if (!it.hasNext() && numTiles2Assigned < totalTileCount) {
					numTiles2Associate += totalTileCount - numTiles2Assigned;
				}
				else if (!it.hasNext() && numTiles2Assigned > totalTileCount) {
					numTiles2Associate -=  numTiles2Assigned - totalTileCount;
				}
				
				System.out.println("***** Associating  " + numTiles2Associate +  " tiles out of " + totalTileCount + " to " + serverIP);

				for (int tileIterator =0; tileIterator < numTiles2Associate; tileIterator++) {
					try {
						Tile tile = (Tile) gtIt.next();
						tile.setDestServerIP(serverIP);						
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					//System.out.println("Associated orderStr " + tile.getOrderStr() + " to " + serverIP );
				}
			} // end while
		}
	}
	
	
	void proportionallyDistributeTiles2Servers4Table(String tableName) {
		
		long incrRecrdCtr =0;
		long incrExcpRecrdCtr =0;
		try {
			if (mConn == null) {
				if (serverDescs != null) {
					Iterator it = serverDescs.keySet().iterator();
					if (it.hasNext()) { //take the first server from serverDescs and connect to it
						String serverIP = (String) it.next();
						mConn = getConnection4Server(serverIP);
					}
				}
			}
			
			Statement stmt = mConn.createStatement();
			
			String sqlStmt = "select * from "+ tableName +" limit 1";
			
			ResultSet rs =stmt.executeQuery(sqlStmt);
			ResultSetMetaData rsMetadata = rs.getMetaData();
			int columnCnt = rsMetadata.getColumnCount();
			
			String insertStmt = " insert into "+ tableName + mPartId + " values (";
			for (int y=0;y<columnCnt;y++) {
				insertStmt += "?";
				if (y<columnCnt-1) insertStmt += ",";
			}
			insertStmt += ")";
			//System.out.println("insertStmt " + insertStmt);
			
			// do for each server 
			if (serverDescs != null) {
				Iterator it = serverDescs.keySet().iterator();
				while (it.hasNext()) {
					long serverIncrRecrdCtr =0;
					long serverExcpIncrRecrdCtr =0;
					
					String serverIP = (String) it.next();
					Connection serverConn = getConnection4Server(serverIP);
					//HashSet<Integer> primaryKeyHS = new HashSet();
					long primaryKey = -1;
					// do for assigned tiles
					Iterator<Tile> gtIt = globalTileList.getIterator();
					while (gtIt.hasNext()) {
						Tile tile = (Tile) gtIt.next();
						if (tile.getDestServerIP().equals(serverIP)) {
							Envelope env = tile.getEnvelope();
							
							/////////////////////
							int limit = LIMIT;
							int curr = 0;
							int maxrec = getTileEltCount(env, tableName);
							
							incrRecrdCtr +=maxrec;
							serverIncrRecrdCtr +=maxrec;
							//System.out.println(" incrRecrdCtr="+ incrRecrdCtr + " serverIncrRecrdCtr="+  serverIncrRecrdCtr + " for "+serverIP+" == element count in envelope "+ env.toString() +  " :" + maxrec);
							
							long tileInsertRecordCount=0;
							while (curr  <=  maxrec) {
							
							  ////////////////////
							  long totalMem = Runtime.getRuntime().totalMemory();
							  long freeMem = Runtime.getRuntime().freeMemory();
							  float percentThold = 0.05F;
							  
							  while (freeMem < totalMem*percentThold) {
								  System.out.println(" totalMem=" + totalMem + " freeMem="+ freeMem );
								  try {
									System.gc();
									Thread.sleep(1000);  
									totalMem = Runtime.getRuntime().totalMemory();
									freeMem = Runtime.getRuntime().freeMemory();
								  }
								  catch (Exception e) {
									  e.printStackTrace();
								  }
							  }
							  
							  ResultSet trs = getTileElementsFromTable(tableName,env, curr, limit);
							  long insertRecordCount=0;
							  
							  if (trs != null) {
								PreparedStatement serverPrepStmt = serverConn.prepareStatement(insertStmt);
								
								while (trs.next()) {

									for (int columnIndex=1;columnIndex<=columnCnt;columnIndex++) {										
										int columnType = rsMetadata.getColumnType(columnIndex);
//System.out.println("column name=" + rsMetadata.getColumnName(columnIndex));
										switch (columnType)   {
									      case java.sql.Types.BIT :
									      	{
									    	  byte data= trs.getByte(columnIndex);
									    	  serverPrepStmt.setByte(columnIndex, data);
									      	}
									      	break;
									      case -2: // mysql
									      case java.sql.Types.BLOB :
									      	{
									    	  Blob data= trs.getBlob(columnIndex);
									    	  serverPrepStmt.setBlob(columnIndex, data);
									      	}
									      	break;
									      case 1111: // postgis	  
									      	{
									    	  Object data= trs.getObject(columnIndex);
									    	  serverPrepStmt.setObject(columnIndex, data);
									      	}
									      	break;
									      case java.sql.Types.BOOLEAN :
									      	{
									    	  Blob data= trs.getBlob(columnIndex);
									    	  serverPrepStmt.setBlob(columnIndex, data);
									      	}
									      	break;
									      case java.sql.Types.CHAR :
									      case java.sql.Types.VARCHAR :
									      	{
									    	  String data= trs.getString(columnIndex);
									    	  serverPrepStmt.setString(columnIndex, data);
									      	}
									      	break;
									      case java.sql.Types.CLOB :
									      	{
									    	  Clob data= trs.getClob(columnIndex);
									    	  serverPrepStmt.setClob(columnIndex, data);
									      	}
									      	break;
									      case java.sql.Types.DATE :
									      	{
									    	  Date data= trs.getDate(columnIndex);
									    	  serverPrepStmt.setDate(columnIndex, data);
									      	}
									      	break;
									      case java.sql.Types.DECIMAL :
									      case java.sql.Types.DOUBLE :
									      case java.sql.Types.FLOAT :
									      	{
									    	  double data= trs.getDouble(columnIndex);
									    	  serverPrepStmt.setDouble(columnIndex, data);
									      	}
									      	break;
									      case java.sql.Types.INTEGER :
									      case java.sql.Types.TINYINT :
									      case java.sql.Types.SMALLINT :
									      	{
									    	  int data= trs.getInt(columnIndex);
									    	  serverPrepStmt.setInt(columnIndex, data);
									    	  
									    	  if (rsMetadata.getColumnName(columnIndex).equalsIgnoreCase("gid")) {
									    		  primaryKey = data;
									    	  }
									      	}
									      	break;
									      case java.sql.Types.BIGINT :
									      	{
									    	  long data= trs.getLong(columnIndex);
									    	  serverPrepStmt.setLong(columnIndex, data);
									    	  
									    	  if (rsMetadata.getColumnName(columnIndex).equalsIgnoreCase("gid")) {
									    		  primaryKey = data;
									    	  }
									      	}
									      	break;
									      case java.sql.Types.TIME :
									     	{
									    	  Time data= trs.getTime(columnIndex);
									    	  serverPrepStmt.setTime(columnIndex, data);
									      	}
									      	break;
									      case java.sql.Types.TIMESTAMP :
									      	{
									      	  Timestamp data= trs.getTimestamp(columnIndex);
									    	  serverPrepStmt.setTimestamp(columnIndex, data);
									      	}
									      	break;
									     
									    }
									}
									//System.out.println(primaryKey);
									try {
										insertRecordCount++;
										tileInsertRecordCount++;
										serverPrepStmt.executeUpdate();
									}
									//catch (com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException cve) {
										
									//}
									catch (Exception e) {
										
										incrExcpRecrdCtr++;
										serverExcpIncrRecrdCtr++;
										e.printStackTrace();
									}
									
									// out of memory 
									/*
									if (primaryKey != -1) {
										if (!primaryKeyHS.contains(primaryKey)) {
											serverPrepStmt.executeUpdate();
											primaryKeyHS.add(primaryKey);
										}
										primaryKey = -1;
									}
									*/
								} // end while trs.next()
								trs.close();
								
								
							} // trs != null
							System.out.println("******** insertRecorCount=" + insertRecordCount + " curr=" + curr + " limit=" + limit);   
							curr += limit;
						  } // curr < maxrec	
						  System.out.println("******** tileInsertRecordCount=" + tileInsertRecordCount);	
						}
					}

					System.out.println("******** incrExcpRecrdCtr="+ incrExcpRecrdCtr + " serverExcpIncrRecrdCtr="+ serverExcpIncrRecrdCtr);
				}
			}
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	
	void parallelDistributeAllTiles2Servers4TableWithBucketid(String tableName) {
		
		ArrayList<DeclusterThread> dtAL= new ArrayList();
		
	
		if (serverDescs != null) {
			Iterator it = serverDescs.keySet().iterator();
			while (it.hasNext()) {
				String serverIP = (String) it.next();
				DeclusterThread dt= new DeclusterThread(serverIP, tableName);
				dt.start();	
				dtAL.add(dt);
			}
			
			try {
				Iterator<DeclusterThread> alit = dtAL.iterator();
				while (alit.hasNext()) {
					DeclusterThread dt= alit.next();
					dt.join();
					System.out.println("waiting for thread " + dt.getId());
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}
	
	
	public void createGlobalMBR(String table) {
		
		Envelope val = MBR_HS.get(table);
		if (val != null) {
			Envelope env = MBR_HS.get(table);
			if (globalMBR == null) 
				globalMBR = env;
			else 
				globalMBR.expandToInclude(env);
			
			System.out.println("Global MBR for table "+ table + " : minX="  + globalMBR.getMinX() + " maxX=" + globalMBR.getMaxX() + " ,minY=" + globalMBR.getMinY() + " maxY=" + globalMBR.getMaxY());
			System.out.println(table + ": "+ globalMBR.getMinX() + " , "+ globalMBR.getMaxX()  + " ; "+ globalMBR.getMinY() + ", "+ globalMBR.getMaxY());
		}
		else {
			try {
				if (mConn == null) {
					if (serverDescs != null) {
						Iterator it = serverDescs.keySet().iterator();
						if (it.hasNext()) { //take the first server from serverDescs and connect to it
							String serverIP = (String) it.next();
							mConn = getConnection4Server(serverIP);
						}
					}
				}
				Statement stmt = mConn.createStatement();
				
				int limit = LIMIT;
				int curr = 1;
				int maxrec =0;
				
				ResultSet rs =stmt.executeQuery("select count(*) from " + table );
				while (rs.next()) { 
					maxrec = rs.getInt(1);
					System.out.println("\n Table "  + table + " has records " + maxrec);
				}
				
				int c=0;
				while (curr  <=  maxrec) {
		
		System.out.println("::: query: " + "select asBinary(geom) from " + table + " limit "+limit+" offset "+curr);
		
					rs =stmt.executeQuery("select asBinary(geom) from " + table + " limit "+limit+" offset "+curr);
					while (rs.next()) {
						//String shapeTxt = rs.getString(1);
						//System.out.println(shapeTxt);
						
						byte[] shapeBin = rs.getBytes(1);
						try {
							//Geometry shape = getGeometry(GeomFromText(shapeTxt, 0));
							
							Geometry shape = getGeometry(shapeBin);
							
							Envelope env = shape.getEnvelopeInternal();
							if (globalMBR == null) 
								globalMBR = env;
							else {
								globalMBR.expandToInclude(env);
								c++;
							}
							
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} // end while
					curr += limit; 
				} // end while
				System.out.println("# of global MBR envelope expansions: " + c);
				System.out.println("Global MBR for table "+ table + " : minX="  + globalMBR.getMinX() + " maxX=" + globalMBR.getMaxX() + " ,minY=" + globalMBR.getMinY() + " maxY=" + globalMBR.getMaxY());
				System.out.println(table + ": "+ globalMBR.getMinX() + " , "+ globalMBR.getMaxX()  + " ; "+ globalMBR.getMinY() + ", "+ globalMBR.getMaxY());
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} // end else
		
		//System.out.println("::::: number of records in the top level ::: "+getTileEltCount(globalMBR)+"\n\n");
	}
	
	
	public static Geometry getGeometry(byte arg0[])
		throws Exception {
		return wkbreader.read(arg0);
	}
	
	public static byte[] GeomFromText(String arg0, int arg1)
	throws ParseException, IOException {
		Geometry geom = (new WKTReader()).read(arg0);
		return setWKBGeometry(geom, arg1);
	}
	
	public static byte[] setWKBGeometry(Geometry arg0, int arg1)
	throws IOException {
		Geometry geom = arg0;
		geom.setSRID(arg1);
		WKBWriter wkbWriter = new WKBWriter(3, 2);
		return wkbWriter.write(geom);
	}
	
	/** Return MySQL driver. */
	  public String getDriver()  {
	    return "org.postgresql.Driver";
	  }
	  
	  public String getDBConnectionProperties() {
		 return  "connection_postgres_spatial";
	  }
	  
	 
	  /** Gets a database connection from server IP. */
	  public Connection getConnection4Server(String serverIP)
	      throws SQLException   {
		  String connectionUrl="";
		  String login= "";
		  String password ="";
		  
		  try    {
			  serverIP =serverIP.replace('.', '_');
			  ResourceBundle  rb = ResourceBundle.getBundle(getDBConnectionProperties());
			  connectionUrl= rb.getString("url_"+serverIP);
			  login= rb.getString("user");
			  password = rb.getString("password");
			  
		      Class.forName(getDriver());
		    }
		    catch (Exception e)   {
		      e.printStackTrace();
		    }
		    
		    // Connect to database.
		    System.out.println("Connecting to database: url=" + connectionUrl + " user=" + login+ " password="+ password);
		    Connection conn = DriverManager.getConnection(connectionUrl, login, password);
		    return conn; 
	  }
	  
	  
	  /** Gets a database connection from JDBC URK */
	  public Connection getConnectionFromURL(String jdbcURL)
	      throws SQLException   {
		  Connection conn = null;
		  try   {
			  conn = DriverManager.getConnection(jdbcURL);
		    }
		    catch (Exception e)  {
		      e.printStackTrace();
		    }
		    
		    // Connect to database.
		    System.out.println("Connecting to database: url=" + jdbcURL);
		   
		    return conn; 
	  }
	  
	  /**
	     * check whether the data type needs to be quoted
	     *
	     * @return boolean
	     * @param type
	     */
	  private boolean quoteDataType(int type) {
		  boolean quote;
		  switch (type) {
		     case java.sql.Types.CHAR:
	        case java.sql.Types.VARCHAR:
	        case java.sql.Types.DATE:
	        case java.sql.Types.TIME:
	        case java.sql.Types.TIMESTAMP:
	            quote = true;
	            break;
	        default:
	            quote = false;
		  }
		  return quote;
	  }
	  
	  /**
	     * protect data with quotes
	     *
	     * @return Stirng
	     * @param include
	     */
	    private String quote(String include) {
	    	if (include != null) {
	            return "'" + include + "'";
	        }

	        return include;
	    }
	  ////////////////////////// START INNER CLASSES ////////////////////////////////////////////////////////////////////////
	  
		public class Tile {
			private int tileId;
			//private int bucketId;
			private int level;
			private int eltCount;
			private String orderStr;
			private  int orientation;
			private String destServerIP;
					
			private Envelope env;
			
			public String getDestServerIP() {
				return destServerIP;
			}
			public void setDestServerIP(String IP) {
				this.destServerIP = IP;
			}
			
			public String getOrderStr() {
				return orderStr;
			}
			public void setOrderStr(String orderStr) {
				this.orderStr = orderStr;
			}
			
			public int getOrientation() {
				return orientation;
			}
			public void setOrientation(int orientation) {
				this.orientation = orientation;
			}
			
			
			public int getTileId() {
				return tileId;
			}
			public void setTileId(int tileId) {
				this.tileId = tileId;
			}
			
			public int getLevel() {
				return level;
			}
			public void setLevel(int level) {
				this.level = level;
			}
			public int getEltCount() {
				return eltCount;
			}
			public void setEltCount(int eltCount) {
				this.eltCount = eltCount;
			}
			public Envelope getEnvelope() {
				return env;
			}
			public void setEnvelope(Envelope env) {
				this.env = env;
			}
		}
		
		
		
		public class TileList {
			LinkedList<Tile> dq=  null;
			
			public TileList() {
				dq= new LinkedList<Tile>();
			}
			
			void init() {
				if (globalMBR != null) {
					Tile tile = new Tile();
					tile.setTileId(0);
					tile.setLevel(0);
					tile.setEltCount(1);
					tile.setEnvelope(globalMBR);
					dq.add(tile);
				}
			}
			
			void remove(Tile tile) {
				dq.remove(tile);
			}
			
			void add(Tile tile) {
				dq.add(tile);
			}
			
			/*
			void insert(int index, Tile tile) {
				dq.add(index, tile);
			}
			*/
			
			void insertAll(int index, ArrayList<Tile> tileAL) {
				dq.addAll(index, tileAL);
			}
			
			int getIndex(Tile tile) {
				return dq.indexOf(tile);
			}
			
			int getTotalTileCount() {
				return dq.size();
			}
			
			Tile getTileAt(int index) {
				return dq.get(index);
			}
			
			Tile getLargestTile() {
				Iterator<Tile> it = dq.iterator();
				Tile largestTile = null;
				int maxEltCount =-1;
				
				while (it.hasNext()) {
					Tile tile= it.next();
					int eltCnt = tile.getEltCount();
					if (maxEltCount < eltCnt) {
						maxEltCount = eltCnt ;
						largestTile = tile;
					}
				}
				return largestTile;
			}
			
			Iterator getIterator() {
				return  dq.iterator();
			}
			
			long getTotalNumRecordsInAllTiles() {
				long totalRecordsInAllTiles=0;
				Iterator<Tile> it = dq.iterator();
				
			    while (it.hasNext()) {
			    	Tile tile= it.next();
			    	totalRecordsInAllTiles += tile.getEltCount();
			    }
			    return totalRecordsInAllTiles;
			}
			
			void generatePartitionMBRFile() {
				int totalEltCount=0;
				int totalTileCoint=0;
				Iterator<Tile> it = dq.iterator();
				
				File partitionMBRFile = new File ("Partition_MBRs_" + System.currentTimeMillis());
				try {
					PrintWriter pr = new PrintWriter (new FileWriter(partitionMBRFile  ));
					pr.flush();
					while (it.hasNext()) {
				    	Tile tile= it.next();
				    	Envelope env = tile.getEnvelope();
				    	totalEltCount+=tile.getEltCount();
				    	totalTileCoint++;
	System.out.println(tile.getOrderStr() +":"+ tile.getTileId() + ":" + tile.getEltCount() + ":"+ env.getMinX()+","+env.getMaxX()+";"+env.getMinY()+","+env.getMaxY());
				    	pr.println(tile.getOrderStr() +":"+ tile.getTileId() + ":" + tile.getEltCount() + ":"+ env.getMinX()+","+env.getMaxX()+";"+env.getMinY()+","+env.getMaxY());
					}
					pr.flush();
					pr.close();
					System.out.println("total elt count=" + totalEltCount+ "total tile count="+ totalTileCoint+" \n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			void printList() {
				Iterator<Tile> it = dq.iterator();
				
			    while (it.hasNext()) {
			    	Tile tile= it.next();
			    	System.out.println("Order="+  tile.getOrderStr() +"  Tile id=" + tile.getTileId() + " Element count=" + tile.getEltCount() + " MBR="+ tile.getEnvelope().toString());
			    }
			}

			void printList2File() {
				
			   // System.out.println("Order="+  tile.getOrderStr() +"  Tile id=" + tile.getTileId() + " Element count=" + tile.getEltCount() + " MBR="+ tile.getEnvelope().toString());
			  
			    long totalRecordsInAllTiles =0;
			    BufferedWriter dataFileWriter = null;
			    
			    try {
					//String fileName = "Partition_MBRs_"+ System.currentTimeMillis()	;
			    	
			    	String fileName = "Partition_MBRs";
					File outFile = new File(fileName);
					System.out.println("#####  Writing into "+ outFile.getAbsolutePath());
					dataFileWriter = new BufferedWriter(new FileWriter(outFile));
					
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
			    Iterator<Tile> it = dq.iterator();
				
			    while (it.hasNext()) {
			    	Tile tile= it.next();
				    String dataTxtLine= tile.getOrderStr()+":"+tile.getTileId()+":"+ tile.getEltCount() +":"+tile.getEnvelope().getMinX()+","+tile.getEnvelope().getMaxX()+";"+tile.getEnvelope().getMinY()+","+tile.getEnvelope().getMaxY();
			//System.out.println(dataTxtLine);
					totalRecordsInAllTiles += tile.getEltCount();
					try {
						dataFileWriter.write(dataTxtLine);
						dataFileWriter.newLine();
						dataFileWriter.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    }
			    
			    try {
					dataFileWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println( "\n $$$$$ totalRecordsInAllTiles:" + totalRecordsInAllTiles);
			} 
		}

		/////////////////////////////////// MAIN INNER CLASS /////////////////////////////////////////////////////////////////
		class DeclusterThread extends Thread {
			  
			  String mServerIP;
			  String tableName;
				  
			  DeclusterThread(String sIP, String tab) {
				  mServerIP = sIP;
				  tableName = tab;
			  }
			  
			  public void run() {
				  parallelDistributeAllTiles2Servers4TableWithTileid();
			  }
			  
			  void parallelDistributeAllTiles2Servers4TableWithTileid() {
					
					long insertRecrdCtr = 0;
					int tilecount4serverIP=0;
					try { 
						Connection selectSrcConn = getConnection4Server(mServerIP); // select from many pointed to by SERVER_DESC
						Statement srcStmt = selectSrcConn.createStatement();
						Connection insertDestConn = getConnectionFromURL(mDestJdbcURL); // insert only into one database - pointed to by url_original in connection_postgres_spatial.properties
						Statement destDbStatement = insertDestConn.createStatement();									
						
						// read src table metadata and populate columnName 
			            String srcSqlStmt = "select * from "+mSrcDbTableName+" limit 1 ";  
						System.out.println(srcSqlStmt);	
						
						ResultSet srs =srcStmt.executeQuery(srcSqlStmt);
						ResultSetMetaData rsMetadata = srs.getMetaData();
						int columnCnt = rsMetadata.getColumnCount();
						String columnName[] = new String[columnCnt];
						int i = 0;
						while (srs.next()) {
							for (int columnIndex=1;columnIndex<=columnCnt;columnIndex++) {										
								int columnType = rsMetadata.getColumnType(columnIndex);
								columnName[i++] = rsMetadata.getColumnName(columnIndex);
								switch (columnType)   {
								  case java.sql.Types.BIT :
							      	{
							      		
							      	}
							      	break;
							      case -2: // mysql
							      case java.sql.Types.BLOB :
							      	{
							    	 
							      	}
							      	break;
							      case 1111: // postgis	  
							      	{
							    	 
							      	}
							      	break;
							      case java.sql.Types.BOOLEAN :
							      	{
							    	  
							      	}
							      	break;
							      case java.sql.Types.CHAR :
							      case java.sql.Types.VARCHAR :
							      	{
							    	 
							      	}
							      	break;
							      case java.sql.Types.CLOB :
							      	{
							    	  
							      	}
							      	break;
							      case java.sql.Types.DATE :
							      	{
							    	 
							      	}
							      	break;
							      case java.sql.Types.DECIMAL :
							      case java.sql.Types.DOUBLE :
							      case java.sql.Types.FLOAT :
							      	{
							    	 
							      	}
							      	break;
							      case java.sql.Types.INTEGER :
							      case java.sql.Types.TINYINT :
							      case java.sql.Types.SMALLINT :
							      	{
							    	 				    	  
							    	  //if (rsMetadata.getColumnName(columnIndex).equalsIgnoreCase("gid")) {
							    	  //	primaryKey = data;
							    	  //}
							      	}
							      	break;
							      case java.sql.Types.BIGINT :
							      	{
							    					    	  
							    	  //if (rsMetadata.getColumnName(columnIndex).equalsIgnoreCase("gid")) {
							    	  //	  primaryKey = data;
							    	  //}
							      	}
							      	break;
							      case java.sql.Types.TIME :
							     	{
							    	
							      	}
							      	break;
							      case java.sql.Types.TIMESTAMP :
							      	{
							 
							      	}
							      	break;    
							    }
							} // end for
						} // end while
						srs.close();
			           
			           
			            String baseInsert = "INSERT INTO \"" + mDestDbTableName + "\" (";
			            for (int j = 0; j < columnName.length - 1; j++) {
			                baseInsert += "\"" + columnName[j] + "\", ";
			            }
			            baseInsert += "\"" + columnName[columnName.length - 1] + "\", ";
			            baseInsert += "\"" + SHARDID_COLUMN_NAME + "\" ) ";
			            baseInsert += " VALUES (";

																			
						int primaryKey = -1;
						// do for assigned tiles
						Iterator<Tile> gtIt = globalTileList.getIterator();
						while (gtIt.hasNext()) {
							Tile tile = (Tile) gtIt.next();
							if (tile == null)
								continue;
							if (tile.getDestServerIP()==null) {
								System.err.println("tile.getDestServerIP()==null ; number of elements= " + tile.getEltCount());
								continue;
							}
							if (tile.getDestServerIP().equals(mServerIP)) {
								Envelope env = tile.getEnvelope();
										
								int tileId = tile.getTileId();
								int limit = LIMIT;
								int curr = 0;
								
								int maxrec = tile.getEltCount();
								
								while (curr  <=  maxrec) {
										
									long totalMem = Runtime.getRuntime().totalMemory();
									long freeMem = Runtime.getRuntime().freeMemory();
									float percentThold = 0.05F;
										  
									while (freeMem < totalMem*percentThold) {
										System.out.println(" totalMem=" + totalMem + " freeMem="+ freeMem );
										try {
											System.gc();
											Thread.sleep(1000);  
											totalMem = Runtime.getRuntime().totalMemory();
											freeMem = Runtime.getRuntime().freeMemory();
										}
										catch (Exception e) {
											e.printStackTrace();
										}
									}
										  
									srs = getAllRecordFromTableWithinTile(selectSrcConn,mSrcDbTableName, columnName, env, curr, limit);
								
									if (srs != null) {
										rsMetadata = srs.getMetaData();
										columnCnt = rsMetadata.getColumnCount();	
										while (srs.next()) {
											StringBuffer sbQuery = new StringBuffer(baseInsert);
											try {

							                	for (int columnIndex=1;columnIndex<=columnCnt;columnIndex++) {										
							                		int columnType = rsMetadata.getColumnType(columnIndex);
							                		//System.out.println(rsMetadata.getColumnName(columnIndex));
							        				String columnData;	
							                		switch (columnType)   {
												      case java.sql.Types.BIT :
												      	{
												      		//byte data= srs.getByte(columnIndex);
												      		//serverPrepStmt.setByte(columnIndex, data);
												      		columnData= String.valueOf(srs.getByte(columnIndex));
												      		if (columnData == null) {
												      			sbQuery.append("null");
												      		} else if (quoteDataType(columnType)) {
												      			sbQuery.append(quote(columnData));
												      		} else {
												      			sbQuery.append(columnData);
												      		}		
												      	}
												      	break;
												      case -2: // mysql
												      case java.sql.Types.BLOB :
												      	{
												    	    //Blob data= trs.getBlob(columnIndex);
												    	    //serverPrepStmt.setBlob(columnIndex, data);
												      		columnData= String.valueOf(srs.getBlob(columnIndex));
												      		if (columnData == null) {
												      			sbQuery.append("null");
												      		} else if (quoteDataType(columnType)) {
												      			sbQuery.append(quote(columnData));
												      		} else {
												      			sbQuery.append(columnData);
												      		}	
												      	}
												      	break;
												      case 1111: // postgis	  
												      	{
												    	  //Object data= trs.getObject(columnIndex);
												    	  //serverPrepStmt.setObject(columnIndex, data);
												      	  columnData= String.valueOf(srs.getObject(columnIndex));
												      	  if (columnData == null) {
												      		  sbQuery.append("null");
												      	  } else if (quoteDataType(columnType)) {
												      		  sbQuery.append(quote(columnData));
												      	  } else {
												      		  sbQuery.append(columnData);
												      	  }	
												      	}
												      	break;
												      case java.sql.Types.BOOLEAN :
												      	{
												    	  //Blob data= trs.getBlob(columnIndex);
												    	  //serverPrepStmt.setBlob(columnIndex, data);
												      	  columnData= String.valueOf(srs.getBoolean(columnIndex));
												      	  if (columnData == null) {
												      		  sbQuery.append("null");
												      	  } else if (quoteDataType(columnType)) {
												      		  sbQuery.append(quote(columnData));
												      	  } else {
												      		  sbQuery.append(columnData);
												      	  }
												      	}
												      	break;
												      case java.sql.Types.CHAR :
												      case java.sql.Types.VARCHAR :
												      	{
												      		//String data= trs.getString(columnIndex);
												      		//serverPrepStmt.setString(columnIndex, data);
												      		columnData= srs.getString(columnIndex);
												      		if (columnData != null && columnData.indexOf("'") != -1)
												      			columnData = columnData.replaceAll("'", "''");
												      		if (rsMetadata.getColumnName(columnIndex).equalsIgnoreCase("geom")) {
												      			sbQuery.append(" ST_GeomFromText(").append(quote(columnData)).append(",").append(SRID).append(")");
												      		}
												      		else {
													      		if (columnData == null) {
													      			sbQuery.append("null");
													      		} else if (quoteDataType(columnType)) {
													      			sbQuery.append(quote(columnData));
													      		} else {
													      			sbQuery.append(columnData);
													      		}	
												      		}
												      	}
												      	break;
												      case java.sql.Types.CLOB :
												      	{
												    	  //Clob data= trs.getClob(columnIndex);
												    	  //serverPrepStmt.setClob(columnIndex, data);
												      	  columnData= srs.getClob(columnIndex).toString();
												      	  if (columnData == null) {
												      		  sbQuery.append("null");
												      	  } else if (quoteDataType(columnType)) {
												      		  sbQuery.append(quote(columnData));
												      	  } else {
												      		  sbQuery.append(columnData);
												      	  }		
												      	}
												      	break;
												      case java.sql.Types.DATE :
												      	{
												    	  //Date data= trs.getDate(columnIndex);
												    	  //serverPrepStmt.setDate(columnIndex, data);
												      	  columnData= String.valueOf(srs.getDate(columnIndex));
												    	  if (columnData == null) {
												    		  sbQuery.append("null");
												    	  } else if (quoteDataType(columnType)) {
												    		  sbQuery.append(quote(columnData));
												    	  } else {
												    		  sbQuery.append(columnData);
												    	  }	
												      	}
												      	break;
												      case java.sql.Types.DECIMAL :
												      case java.sql.Types.DOUBLE :
												      case java.sql.Types.FLOAT :
												      	{
												    	  //double data= trs.getDouble(columnIndex);
												    	  //serverPrepStmt.setDouble(columnIndex, data);
												    	  columnData= String.valueOf(srs.getDouble(columnIndex));
												    	  if (columnData == null) {
												    		  sbQuery.append("null");
												    	  } else if (quoteDataType(columnType)) {
												    		  sbQuery.append(quote(columnData));
												    	  } else {
												    		  sbQuery.append(columnData);
												    	  }
												      	}
												      	break;
												      case java.sql.Types.INTEGER :
												      case java.sql.Types.TINYINT :
												      case java.sql.Types.SMALLINT :
												      	{
												      		//int data= trs.getInt(columnIndex);
												      		//serverPrepStmt.setInt(columnIndex, data);
												      		//if (rsMetadata.getColumnName(columnIndex).equalsIgnoreCase("gid")) {
												      		//  primaryKey = data;
												      		//}
												      		columnData= String.valueOf(srs.getInt(columnIndex));
												      		if (columnData == null) {
										                        sbQuery.append("null");
										                    } else if (quoteDataType(columnType)) {
										                        sbQuery.append(quote(columnData));
										                    } else {
										                        sbQuery.append(columnData);
										                    }		
												      	}
												      	break;
												      case java.sql.Types.BIGINT :
												      	{
												      		//long data= srs.getLong(columnIndex);
												      		//serverPrepStmt.setLong(columnIndex, data);    	  
												      		//if (rsMetadata.getColumnName(columnIndex).equalsIgnoreCase("gid")) {
												      		//  primaryKey = data;
												      		//}
												      		columnData= String.valueOf(srs.getLong(columnIndex));
												      		if (columnData == null) {
										                        sbQuery.append("null");
										                    } else if (quoteDataType(columnType)) {
										                        sbQuery.append(quote(columnData));
										                    } else {
										                        sbQuery.append(columnData);
										                    }	
												      	}
												      	break;
												      case java.sql.Types.TIME :
												     	{
												    	   //Time data= srs.getTime(columnIndex);
												    	   //serverPrepStmt.setTime(columnIndex, data);
												     		columnData= srs.getTime(columnIndex).toString();	
												      		if (columnData == null) {
										                        sbQuery.append("null");
										                    } else if (quoteDataType(columnType)) {
										                        sbQuery.append(quote(columnData));
										                    } else {
										                        sbQuery.append(columnData);
										                    }		
												      	}
												      	break;
												      case java.sql.Types.TIMESTAMP :
												      	{
												      	  //Timestamp data= srs.getTimestamp(columnIndex);
												    	  //serverPrepStmt.setTimestamp(columnIndex, data);
												      		columnData= srs.getTimestamp(columnIndex).toString();	
												      		if (columnData == null) {
										                        sbQuery.append("null");
										                    } else if (quoteDataType(columnType)) {
										                        sbQuery.append(quote(columnData));
										                    } else {
										                        sbQuery.append(columnData);
										                    }	
												      	}
												      	break;
												     
												    }
							                		sbQuery.append(", ");
							                	} // end for all fields
							                	
							                	//sbQuery.setLength(sbQuery.length() - 2);
							                	sbQuery.append(String.valueOf(tileId));
							                	sbQuery.append(")");
							                	sbQuery.append(stmtTerminator);
							                	String query = sbQuery.toString();
							                	
							                	destDbStatement.execute(query);
							                    
							                	insertRecrdCtr++;
							                   												
											}
											catch (Exception e) {
												//if (!(e instanceof java.sql.SQLException))		
													e.printStackTrace();
											}	
										}											
																	
									}  // trs != null
									srs.close();
									
									curr += limit;
								} // while curr  <=  maxrec
								tilecount4serverIP++;
							} // end if server matches			
						} // while has more tiles
						
						srcStmt.close();
						selectSrcConn.close();
						destDbStatement.close();
						insertDestConn.close();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					System.out.println(" From server"+mServerIP + "records inserted ="+ insertRecrdCtr);
				}
		  } // end class
		
		/////////// END INNER CLASSES///////////////////////////////////////////////////////////////////////////////////////////////////

}
