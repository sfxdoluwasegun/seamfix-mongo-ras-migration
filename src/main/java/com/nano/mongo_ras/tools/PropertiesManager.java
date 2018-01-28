package com.nano.mongo_ras.tools;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PropertiesManager extends FileManager {

	private String targetPropertyFilename ;

	public PropertiesManager() {
		// TODO Auto-generated constructor stub

		propsManager = FileCache.getPropertyFile();
	}

	public PropertiesManager(String filename) {
		// TODO Auto-generated constructor stub
		setTargetPropertyFilename(filename);
		propsManager = FileCache.getPropertyFile(getTargetPropertyFilename());
	}

	public String getTargetPropertyFilename() {
		return targetPropertyFilename;
	}

	public void setTargetPropertyFilename(String targetPropertyFilename) {
		this.targetPropertyFilename = targetPropertyFilename;
	}
}
