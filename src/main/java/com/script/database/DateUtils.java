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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.Validate;

/**
 * {@link DateUtils} is utility used to convert date specific column value.
 *
 * @author vipul
 * @see 
 * @Date 18-Apr-2015
 *
 */
public class DateUtils 
{
	/**
	 * Use to check whether given column value is date or not.
	 * 
	 * @param value
	 * @return true, when value is valid as per <b>yyyy-MM-d HH:mm:ss.SS</b> date format.
	 */
	public static boolean isDate(final String value) 
	{
		DateFormat format = new SimpleDateFormat("yyyy-MM-d HH:mm:ss.SS", Locale.ENGLISH);
		try 
		{
			@SuppressWarnings("unused")
			Date date = format.parse(value);
			return true;
		} 
		catch (ParseException e) 
		{
			return false;
		}
	}
	
	/**
	 * Converts the sql specific date to specified database format.
	 * @param dataBaseType
	 * @param stringDate
	 * @return
	 * @throws Exception
	 */
	public static String getFormattedDate(final DatabaseEnum dataBaseType,final String stringDate) throws Exception 
	{
		String[] stringDateTime = stringDate.split(" ");
		
		String result = "";
		
		if(dataBaseType == DatabaseEnum.Oracle )
		{
			Validate.isTrue(stringDateTime.length == 2, stringDate+" is not splitted in two parts using \" \"");
			
			String date = stringDateTime[0];
			
			String[] stringDateArray = date.split("-");
			
			String formattedString = stringDateArray[1]+"/"+stringDateArray[2]+"/"+stringDateArray[0];
			
			String time = stringDateTime[1]+"000";
			
			result = "TO_TIMESTAMP('"+formattedString+" "+time+"','fmMMfm/fmDDfm/YYYY fmHH24fm:MI:SS.FF')";
		}
		
		if(dataBaseType == DatabaseEnum.MySQL)
		{
			result = "'"+stringDate.substring(0, stringDate.indexOf("."))+"'";
		}
				
		return result;
	}
}
