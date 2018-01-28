package com.nano.mongo_ras.tools;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches property file for use in application life cycle.
 * 
 * @author segz
 *
 */
public class FileCache {
	
	private static ConcurrentHashMap<String, FileManager> pties = new ConcurrentHashMap<String, FileManager>();

	/**
	 * Retrieves the property object by name
	 *
	 * @param file the file
	 * @return the property file
	 */
	public static FileManager getPropertyFile(String file) {
		FileManager pf = pties.get(file);
		if(pf == null){
			pf = new FileManager(file);
			pf = pties.putIfAbsent(file, pf);
		}
		return pf;
	}

	/**
	 * Retrieves the default property object
	 *
	 * @return the property file
	 */
	public static FileManager getPropertyFile() {
		String file = System.getProperty("jboss.home.dir") + "/app.properties";
		FileManager pf = pties.get(file);
		if(pf == null){
			pf = new FileManager(file);
			pf = pties.putIfAbsent(file, pf);
		}
		return pf;
	}

}
