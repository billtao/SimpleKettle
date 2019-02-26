package com.powerrich;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LoggingPluginInterface;

public class Log4jLogging implements LoggingPluginInterface {
	
	public static final String STRING_PENTAHO_DI_LOGGER_NAME = "org.pentaho.di";
	
	private Logger pentahoLogger;
	
	public Log4jLogging() {
		this.pentahoLogger = Logger.getLogger(STRING_PENTAHO_DI_LOGGER_NAME);
		this.pentahoLogger.setAdditivity(false);
	}

	@Override
	public void eventAdded(KettleLoggingEvent event) {
		switch(event.getLevel()) {
			case ERROR:
				pentahoLogger.log(Level.ERROR, event.getMessage());
				break;
			case MINIMAL:
				pentahoLogger.log(Level.WARN, event.getMessage());
				break;
			case BASIC:
				pentahoLogger.log(Level.INFO, event.getMessage());
				break;
			case DETAILED:
				pentahoLogger.log(Level.DEBUG, event.getMessage());
				break;
			case DEBUG:
				pentahoLogger.log(Level.TRACE, event.getMessage());
				break;
			case ROWLEVEL:
				pentahoLogger.log(Level.ALL, event.getMessage());
				break;
			case NOTHING:
				pentahoLogger.log(Level.OFF, event.getMessage());
				break;
		}
		
	}

	@Override
	public void dispose() {
		KettleLogStore.getAppender().removeLoggingEventListener(this);
	}

	@Override
	public void init() {
		KettleLogStore.getAppender().addLoggingEventListener(this);
	}

}
