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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;


/**
 * {@link ScriptGeneratorLauncher} launches utility to convert sql-server script to optimized script for Oracle, MySql and SQL-Server database.
 *
 * @author vipul
 * @see 
 * @Date 17-Apr-2015
 *
 */
public class ScriptGeneratorLauncher 
{

	final static Logger logger = Logger.getLogger(ScriptGeneratorLauncher.class);
	
	public static File workDir;
	
	private ScriptGeneratorTaskExecutor scriptGeneratorTaskExecutor = new ScriptGeneratorTaskExecutor();
	
	public static void main(String[] args) throws IOException 
	{
		logger.info("ScriptGeneratorLauncher started ...");
		
		logger.info("Log file is generated at "+new java.io.File( "." ).getCanonicalPath()+"/log/Logfile.log");
		
		try
		{
			if(args.length == 1)
			{
				String launchParameter = args[0];
				String[] split = launchParameter.split("=");
				workDir = new File(split[1]);
				logger.info("Working dir is "+workDir);
				
				checkWorkingDirectoryHaveSQLFile();
				
				initializeOutputDir();
				
				ScriptGeneratorLauncher generatorLauncher = new ScriptGeneratorLauncher();
				generatorLauncher.launchScriptGenerator();

			}
			else
			{
				throw new IllegalArgumentException("Illegal Argument : \"workdir\" is mandatory parameter");
			}
		}
		catch(Exception ex)
		{
			logger.error(ExceptionUtils.getStackTrace(ex));
			throw ex;
		}
		
	}

	private void launchScriptGenerator() throws FileNotFoundException, IOException 
	{
		Set<File> sqlFileInWorkingDir = getSQLFileInWorkingDir();
		for (File sqlFile : sqlFileInWorkingDir) 
		{
			logger.info("Processing file "+ sqlFile.getName());
			RawInsertScriptPopulator rawInsertScriptPopulator = new RawInsertScriptPopulator();
			Map<String, RawInsertScriptEntity> rawInsertScriptDetail = rawInsertScriptPopulator.populate(sqlFile);
			
			ScriptGeneratorTask mysqlScriptGeneratorTask = new ScriptGeneratorTask(new SQLServerToMysqlScriptConverter(), rawInsertScriptDetail, sqlFile.getName());
			ScriptGeneratorTask oracleScriptGeneratorTask = new ScriptGeneratorTask(new SQLServerToOracleScriptConverter(), rawInsertScriptDetail, sqlFile.getName());
			//ScriptGeneratorTask sqlServerScriptGeneratorTask = new ScriptGeneratorTask(new SQLServerToSQLServerScriptConverter(), rawInsertScriptDetail, sqlFile.getName());
			
			scriptGeneratorTaskExecutor.executeTask(mysqlScriptGeneratorTask);
			scriptGeneratorTaskExecutor.executeTask(oracleScriptGeneratorTask);
			//scriptGeneratorTaskExecutor.executeTask(sqlServerScriptGeneratorTask);
		}
		
		scriptGeneratorTaskExecutor.shutdownTask();
		
	}

	private static void initializeOutputDir() throws IOException 
	{
		DatabaseEnum[] databaseValues = DatabaseEnum.values();
		for (DatabaseEnum databaseEnum : databaseValues) 
		{
			File file = new File(workDir, databaseEnum.name());
			FileUtils.deleteQuietly(file);
			file.mkdir();
			logger.info("deleted and created new "+ file.getAbsolutePath());
		}
		
	}

	private static boolean checkWorkingDirectoryHaveSQLFile() 
	{
		if(! workDir.exists())
		{
			throw new IllegalArgumentException(workDir.getAbsolutePath() + " does not exist.");
		}
		
		if(! workDir.isDirectory())
		{
			throw new IllegalArgumentException(workDir.getAbsolutePath() + " is not a directory.");
		}
		
		Set<File> sqlFileInWorkingDir = getSQLFileInWorkingDir();
		
		if(sqlFileInWorkingDir.isEmpty())
		{
			throw new IllegalArgumentException(workDir.getAbsolutePath() + " does not contain any .sql file.");
		}
		
		return true;
	}
	
	private static Set<File> getSQLFileInWorkingDir()
	{
		File[] files = workDir.listFiles(new FilenameFilter() 
		{
		    public boolean accept(File dir, String name) 
		    {
		        return name.toLowerCase().endsWith(".sql");
		    }
		});
		
		return new HashSet<File>(Arrays.asList(files));
	}
}
