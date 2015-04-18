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

import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * {@link ScriptGeneratorTask} whose instances are intended to be executed by a thread.
 *
 * @author vipul
 * @see 
 * @Date 17-Apr-2015
 *
 */
public class ScriptGeneratorTask implements Runnable
{

	private ScriptConverter scriptConverter;
	
	private Map<String, RawInsertScriptEntity> rawInsertScriptDetail;
	
	private String fileName;
	
	final static Logger logger = Logger.getLogger(ScriptGeneratorLauncher.class);
	
	public ScriptGeneratorTask(ScriptConverter scriptConverter, Map<String, RawInsertScriptEntity> rawInsertScriptDetail, String fileName)
	{
		this.scriptConverter = scriptConverter;
		this.rawInsertScriptDetail = rawInsertScriptDetail;
		this.fileName = fileName;
	}
	
	@Override
	public void run() 
	{
		try 
		{
			scriptConverter.convert(rawInsertScriptDetail, fileName);
		} 
		catch (Exception ex) 
		{
			logger.error(ExceptionUtils.getStackTrace(ex));
		}
	}

}
