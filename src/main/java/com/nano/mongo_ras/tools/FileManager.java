package com.nano.mongo_ras.tools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.jboss.logging.Logger;

public class FileManager {
	
	private Logger log = Logger.getLogger(getClass());

	private Properties properties ;

	private String filename ;

	protected FileManager propsManager ;

	public FileManager() {
		// TODO Auto-generated constructor stub
		this(System.getProperty("jboss.home.dir") + "/app.properties");
	}

	public FileManager(String filename) {
		// TODO Auto-generated constructor stub
		properties = new Properties();

		this.filename = filename ;
		loadProperties();
	}

	/**
	 * Load properties.
	 *
	 * @return the properties
	 */
	public Properties loadProperties() {
		try {
			FileInputStream fis = new FileInputStream(filename);
			properties.load(fis);

			fis.close();
		} catch (FileNotFoundException e) {
			createProperties();
			log.error(e.getMessage());
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		return properties;
	}

	/**
	 * Creates a blank properties file.
	 */
	private void createProperties() {
		try {
			FileOutputStream fos = new FileOutputStream(filename);
			properties.store(fos, "Auto generated document");
			fos.flush();
			fos.close();
		} catch (Exception e) {
			log.error("Exception while updating properties file: ", e);
		}
	}

	/**
	 *
	 * @param propertyMap - map to add to property file
	 */
	public void saveProperties(HashMap<String, String> propertyMap) {

		Set<String> keys = propertyMap.keySet();
		Iterator<String> itr = keys.iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			properties.setProperty(key, propertyMap.get(key));
		}
		createProperties();
	}

	/**
	 * deletes an entry with the specified key.
	 *
	 * @param key reference item to remove
	 */
	public synchronized void removeProperty(String key) {
		properties.remove(key);
		createProperties();
	}

	/**
	 * Get the string property but also writes property to file
	 * if property doesn't exist in file.
	 * 
	 * @param property - property name
	 * @param defaultVal - property default value
	 * @return property
	 */
	public String getProperty(String property, 
			String defaultVal){

		HashMap<String, String> propertyMap = null;

		String props = properties.getProperty(property, "");
		if (props == null || props.isEmpty()){
			propertyMap = new HashMap<String, String>();
			propertyMap.put(property, defaultVal);
		}else
			return props;

		if (propertyMap != null)
			saveProperties(propertyMap);

		return defaultVal;
	}

	/**
	 * Gets property from file and casts to integer.
	 * Writes property to file if it doesn't already exist.
	 *
	 * @param key reference key
	 * @param defaultVal value if property is not set
	 * @return integer property
	 */
	public Integer getInt(String key, Integer defaultVal) {
		return Integer.valueOf(getProperty(key, defaultVal + ""));
	}

	/**
	 * Gets property from file and casts to long.
	 * Writes property to file if it doesn't already exist.
	 *
	 * @param key reference key
	 * @param defaultVal value if property is not set
	 * @return integer property
	 */
	public Long getLong(String key, Long defaultVal) {
		return Long.parseLong(getProperty(key, defaultVal + ""));
	}

	/**
	 * Gets the boolean property for specified key.
	 * Writes property to file if it doesn't already exist.
	 *
	 * @param key the property key
	 * @param defaultVal the default value if nothing is found
	 * @return the bool value for the target key
	 */
	public Boolean getBool(String key, Boolean defaultVal){
		return Boolean.parseBoolean(getProperty(key, defaultVal.toString()));
	}

}