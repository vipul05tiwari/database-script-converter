/*
 * The information contained in this document is subject to change without notice.
 * 
 * Developer MAKES NO WARRANTY OF ANY KIND WITH REGARD TO
 * THIS MATERIAL, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Except to
 * correct same after receipt of reasonable notice, GoldenSource Corporation 
 * shall not be liable for errors contained herein or for incidental and/or 
 * consequential damages in connection with the furnishing, performance, 
 * or use of this material.
 * 
 * This document contains proprietary and confidential information that is protected by copyright.
 * 
 * The names of other organizations and products referenced herein are the trademarks or service
 * marks (as applicable) of their respective owners. Unless otherwise stated herein, no association
 * with any other organization or product referenced herein is intended or should be inferred.
 * 
 * 
 */

package com.script.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

/**
 * {@link RawInsertScriptPopulator} reads sql file and populate {@link RawInsertScriptEntity}.
 *
 * @author vipul
 * @see 
 * @Date 17-Apr-2015
 *
 */
public class RawInsertScriptPopulator 
{
	final static Logger logger = Logger.getLogger(RawInsertScriptPopulator.class);

	private static final String SPLIT_BY_VALUE_ERROR_MSG = "Insert query must be divide in two parts when split using \"value\" keyword.";

	private static final String WHITE_SPACE = "\\s+";
	
	private static String VALUES = "(\\))(\\s+)(?i)(VALUES)(\\s+)(\\()";
	
	/**
	 * Read sql file and create Map of table name as key and {@link RawInsertScriptEntity} as value.
	 * 
	 * @param sqlFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Map<String, RawInsertScriptEntity> populate(File sqlFile) throws FileNotFoundException, IOException
	{
		Map<String , RawInsertScriptEntity> tableAndRowInsertScriptMap = new LinkedHashMap<String, RawInsertScriptEntity>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(sqlFile)))
		{
			String insertScript = null;
			
			while ((insertScript = br.readLine()) != null) 
		    {
				if(insertScript.startsWith("INSERT"))
				{
					String[] splitByValue = insertScript.split(VALUES);
					
					Validate.isTrue(splitByValue.length == 2, SPLIT_BY_VALUE_ERROR_MSG +"\n"+ "Error while processing : " +insertScript);
					
					String tableColumnName = splitByValue[0];
					String values =	splitByValue[1];
					
					String[] columnNames = extractDataBetweenRoundBrackets(tableColumnName+")");
					
					String[] columnValues = extractDataBetweenRoundBrackets("("+values);
					
					String tableName = extractTableName(tableColumnName);
					
					RawInsertScriptEntity rawInsertScriptEntity = tableAndRowInsertScriptMap.get(tableName);
					
					if(rawInsertScriptEntity == null)
					{
						rawInsertScriptEntity = new RawInsertScriptEntity(tableName, columnNames);
						tableAndRowInsertScriptMap.put(tableName, rawInsertScriptEntity);
					}
					
					rawInsertScriptEntity.addValue(columnNames, columnValues);
				}
		    }
		}
		
		return tableAndRowInsertScriptMap;
	}


	private String extractTableName(String tableColumnName) 
	{
		return tableColumnName.split(WHITE_SPACE)[1].trim();
	}

	private String[] extractDataBetweenRoundBrackets(String stringData) 
	{
		int indexOfOpenBracket = stringData.indexOf("(");
		int indexOfCloseBracket = stringData.lastIndexOf(")");
	     
	    String valueWithOutParanthesis = stringData.substring(indexOfOpenBracket+1, indexOfCloseBracket);   
	    
	    //splitting a comma-separated string but ignoring commas in quotes
	    return valueWithOutParanthesis.split(",(?=([^\']*\'[^\']*\')*[^\']*$)");
	}
}
