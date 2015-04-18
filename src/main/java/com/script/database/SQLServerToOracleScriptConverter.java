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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * {@link SQLServerToOracleScriptConverter} will convert sql dml script to oracle.
 *
 * @author vipul
 * @see 
 * @Date 17-Apr-2015
 *
 */
public class SQLServerToOracleScriptConverter implements ScriptConverter
{

	@Override
	public void convert(Map<String, RawInsertScriptEntity> rawInsertScriptDetail, String fileName) throws Exception 
	{
		String outputFileName = ScriptGeneratorLauncher.workDir+File.separator+DatabaseEnum.Oracle+File.separator+fileName;
		
		try(BufferedWriter oracleBufferWritter = new BufferedWriter(new FileWriter(outputFileName)))
		{
			for (RawInsertScriptEntity rawInsertScriptEntity : rawInsertScriptDetail.values()) 
			{
				List<Map<String, String>> allColumnValues = rawInsertScriptEntity.getAllColumnValues();
				
				List<String> columnNameList = rawInsertScriptEntity.getColumnNameList();
				
				String columnNames = StringUtils.join(columnNameList,", ").toUpperCase();
				
				for (Map<String, String> columnValue : allColumnValues) 
				{
										
					StringBuilder sqlServerQuery = new StringBuilder("INSERT ");
					sqlServerQuery.append(" INTO ");
					sqlServerQuery.append(rawInsertScriptEntity.getTableName().toUpperCase());
					sqlServerQuery.append(" ( ");
					sqlServerQuery.append(columnNames);
					sqlServerQuery.append(" ) VALUES ");
					
					columnValue = convertDateValue(new LinkedHashMap<>(columnValue));
					
					Collection<String> values = columnValue.values();
					
					sqlServerQuery.append("("+StringUtils.join(values,", ")+");");
					
					oracleBufferWritter.write(sqlServerQuery.toString());
					oracleBufferWritter.newLine();
					
				}
				
				oracleBufferWritter.newLine();
				oracleBufferWritter.newLine();
			}
			
			oracleBufferWritter.newLine();
			oracleBufferWritter.write("commit;");
		}
		
	}

	private Map<String, String> convertDateValue(Map<String, String> valueMap) throws Exception
	{
		for (Entry<String, String> valueMapEntry : valueMap.entrySet()) 
		{	
			//get the value between single quotes
			Pattern pattern = Pattern.compile("(?:^|\\s)'([^']*?)'(?:$|\\s)");
			Matcher matcher = pattern.matcher(valueMapEntry.getValue());
			
			if(matcher.find() && DateUtils.isDate(matcher.group(1)))
			{
				String formattedDate = DateUtils.getFormattedDate(DatabaseEnum.Oracle,matcher.group(1));
				valueMap.put(valueMapEntry.getKey(), formattedDate);
			}
		}
		return valueMap;
	}

}
