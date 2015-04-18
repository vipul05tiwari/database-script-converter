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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * {@link RawInsertScriptEntity} holds raw sql server insert script.
 *
 * @author vipul
 * @see 
 * @Date 17-Apr-2015
 *
 */
public class RawInsertScriptEntity 
{
	private final String tableName;
	
	private final List<String> columnNameList = new ArrayList<>();
	
	private final List<Map<String, String>> allColumnValues = new ArrayList<Map<String,String>>();
	
	public RawInsertScriptEntity(final String tableName, final String [] columnNames)
	{
		Validate.isTrue(! StringUtils.isEmpty(tableName), "table name should not be empty.");
		Validate.isTrue(columnNames.length > 0, "At least on column should exist.");
		
		this.tableName = tableName;
		
		int columnCount = columnNames.length;
		
		for (int counter = 0; counter < columnCount; counter++) 
		{
			columnNameList.add(columnNames[counter].trim());	
		}
	}
	
	
	public String getTableName() 
	{
		return tableName;
	}

	public List<String> getColumnNameList() 
	{
		return columnNameList;
	}

	public List<Map<String, String>> getAllColumnValues() 
	{
		return allColumnValues;
	}
	
	public List<List<Map<String, String>>> getAllColumnValueInBatch(final int L) 
	{
	    List<List<Map<String, String>>> parts = new ArrayList<List<Map<String, String>>>();
	    final int N = allColumnValues.size();
	    for (int i = 0; i < N; i += L) 
	    {
	    	List<Map<String, String>> subList = allColumnValues.subList(i, Math.min(N, i+L));
	    	parts.add(Collections.unmodifiableList(new ArrayList<Map<String, String>>(subList)));
	    }
	    return Collections.unmodifiableList(parts);
	}

	public void addValue(String[] columnNames, String[] columnValues) 
	{
		Validate.isTrue(columnNames.length == columnValues.length, "Number of column name and value should be equal. Column -->>" + Arrays.toString(columnNames) +" Values -->> "+ Arrays.toString(columnValues));
		
		int columnCount = columnNames.length;
		
		Map<String, String> columnValueMap = new LinkedHashMap<String, String>();
		
		for (int counter = 0; counter < columnCount; counter++) 
		{
			Validate.isTrue(columnNameList.get(counter).equals(columnNames[counter].trim()), "Order of column is diffrent. '"+columnNameList.get(counter) +"' and '"+ columnNames[counter]+"'");
			
			columnValueMap.put(columnNames[counter].trim(), columnValues[counter].trim());
		}
		
		allColumnValues.add(Collections.unmodifiableMap(columnValueMap));
	}
	
	@Override
	public String toString() {
		return "RawInsertScriptEntity [tableName=" + tableName
				+ ", columnNameList=" + columnNameList + ", allColumnValues="
				+ allColumnValues + "]";
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RawInsertScriptEntity other = (RawInsertScriptEntity) obj;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

}
