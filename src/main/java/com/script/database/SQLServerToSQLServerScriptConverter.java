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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * {@link SQLServerToSQLServerScriptConverter} will convert sql dml script to sql server.
 *
 * @author vipul
 * @see 
 * @Date 17-Apr-2015
 *
 */
public class SQLServerToSQLServerScriptConverter implements ScriptConverter
{
	
	@Override
	public void convert(Map<String, RawInsertScriptEntity> rawInsertScriptDetail, String fileName) throws Exception 
	{
		String outputFileName = ScriptGeneratorLauncher.workDir+File.separator+DatabaseEnum.SQLServer+File.separator+fileName;
		
		try(BufferedWriter sqlServerBufferWriter = new BufferedWriter(new FileWriter(outputFileName)))
		{
			for (RawInsertScriptEntity rawInsertScriptEntity : rawInsertScriptDetail.values()) 
			{
				sqlServerBufferWriter.write("SET IDENTITY_INSERT "+rawInsertScriptEntity.getTableName()+" ON");
				sqlServerBufferWriter.newLine();
				List<List<Map<String, String>>> allColumnValuesInBatch = rawInsertScriptEntity.getAllColumnValueInBatch(valuePerInsert);
				
				for (List<Map<String, String>> batchColumnValue : allColumnValuesInBatch) 
				{
					boolean isFirstIterartion = true;
					
					StringBuilder sqlServerQuery = null;
					
					int totalRecordCount = 0;
					
					while(totalRecordCount < batchColumnValue.size())
					{
						if(isFirstIterartion)
						{
							sqlServerQuery = new StringBuilder("INSERT INTO "+rawInsertScriptEntity.getTableName()+" ( ");
							
							List<String> columnNameList = rawInsertScriptEntity.getColumnNameList();
							
							sqlServerQuery.append(StringUtils.join(columnNameList,", ")+" ) VALUES ");
							
							isFirstIterartion = false;
						}
						
						Map<String, String> valueMap = batchColumnValue.get(totalRecordCount);
						
						Collection<String> values = valueMap.values();
						
						sqlServerQuery.append("("+StringUtils.join(values,", ")+")");
						
						totalRecordCount++;
						
						if(totalRecordCount != batchColumnValue.size())
						{
							sqlServerQuery.append(", ");
						}
					}
					
					sqlServerQuery.append(";");
					sqlServerBufferWriter.write(sqlServerQuery.toString());
					sqlServerBufferWriter.newLine();
				}
				
				sqlServerBufferWriter.write("SET IDENTITY_INSERT "+rawInsertScriptEntity.getTableName()+" OFF");
				sqlServerBufferWriter.newLine();
				sqlServerBufferWriter.newLine();
			}
			
			sqlServerBufferWriter.newLine();
			sqlServerBufferWriter.write("commit;");
		}
	}

}
