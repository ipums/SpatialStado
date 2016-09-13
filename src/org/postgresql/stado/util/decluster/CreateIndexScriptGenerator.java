/*
 * Copyright (c) 2016 Regents of the University of Minnesota
 *
 * This file is part of the Minnesota Population Center's Terra Populus project.
 * For copyright and licensing information, see the NOTICE and LICENSE files
 * in this project's top-level directory, and also online at:
 * https://github.com/mnpopcenter/stado
 */
package org.postgresql.stado.util.decluster;

public class CreateIndexScriptGenerator {

	// california
	static final String[] TABLES = {"arealm_merge_ca_shall", "edges_merge_ca_shall", "areawater_merge_ca_shall"};
	
	static final int NUMBER_OF_SHARDS = 1024;
	
	static final String QUERY = "CREATE INDEX #@#_%@%_goem_idx ON #@#_%@% USING gist (geom);";
	
	static final String TABLE_NAME_REPL_SUBSTR = "#@#";
	static final String TABLE_ID_REPL_SUBSTR = "%@%";
	
	public static void main(String args[]) {
		
		if (args.length != 1) {
			System.err.println("ERROR: CreateIndexScriptGenerator <TABLE ID: 0/1/2>");
			return;
		}
		
		int tableId = Integer.parseInt( args[0].trim());
		for (int i=0;i<=NUMBER_OF_SHARDS;i++) {
			String toPrint = QUERY.replaceAll(TABLE_NAME_REPL_SUBSTR, TABLES[tableId]);
			if (i==0) {
				String replStr = "_" + TABLE_ID_REPL_SUBSTR;
				toPrint =  toPrint.replaceAll(replStr,"");
			}
			else 
				toPrint =  toPrint.replaceAll(TABLE_ID_REPL_SUBSTR,String.valueOf(i));
			System.out.println(toPrint);
		}
			
	}
}
