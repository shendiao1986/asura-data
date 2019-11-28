package org.asura.data.log;

import java.util.Date;

import org.asura.core.data.DataRecord;
import org.asura.core.data.IEditor;
import org.asura.core.util.DateUtil;
import org.asura.core.util.cache.SimpleCache;
import org.asura.data.mongo.MongoHandler;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoLogFactory {
	private static SimpleCache<String, MLog> cache = new SimpleCache<String, MLog>(20);

	public static MLog getLogger(String name) {
		return getLogger(name, MongoLogConfig.fromConfigFile());
	}

	public static MLog getLogger(String name, MongoLogConfig config) {
		if (cache.get(name + config) != null) {
			return cache.get(name + config);
		} else {
			MongoLogger logger = new MongoLogger(name, config);
			cache.cache(name + config, logger);
			return cache.get(name + config);
		}
	}

	public static MLog getLogger(@SuppressWarnings("rawtypes") Class cls) {
		return getLogger(cls.getName());
	}

	private static class MongoLogger implements MLog {

		private Logger logger = LoggerFactory.getLogger(MongoLogger.class);
		private String name;
		private MongoHandler handler;
		private IEditor logEditor;

		public MongoLogger(String name, MongoLogConfig config) {
			this.name = name;
			handler = new MongoHandler(config.getHost());
			logEditor = handler.getEditor(config.getLogDb(), config.getLogCollection(), new String[] { "id" },
					new String[] { "date", "createDate", "source" });
			logger.info("initialize mongo logger " + config);
		}

		public void debug(Object message) {
			debug(message, null);
		}

		public void debug(Object message, Throwable t) {
			DataRecord record = buildRecord(message, t);
			record.AddField("severity", "debug");
			logEditor.addRecord(record);
		}

		public void info(Object message) {
			info(message, null);
		}

		public void info(Object message, Throwable t) {
			DataRecord record = buildRecord(message, t);
			record.AddField("severity", "info");
			logEditor.addRecord(record);
		}

		public void warn(Object message) {
			warn(message, null);
		}

		public void warn(Object message, Throwable t) {
			DataRecord record = buildRecord(message, t);
			record.AddField("severity", "warn");
			logEditor.addRecord(record);
		}

		public void error(Object message) {
			error(message, null);
		}

		public void error(Object message, Throwable t) {
			DataRecord record = buildRecord(message, t);
			record.AddField("severity", "error");
			logEditor.addRecord(record);
		}

		private DataRecord buildRecord(Object message, Throwable t) {
			DataRecord record = new DataRecord();
			record.AddField("id", ObjectId.get());
			record.AddField("content", message.toString());
			record.AddField("source", name);
			record.AddField("createDate", new Date());
			record.AddField("date", DateUtil.getDateString(new Date()));
			if (t != null) {
				StringBuffer stackTrace = new StringBuffer();
				StackTraceElement[] trace = t.getStackTrace();
				for (StackTraceElement traceElement : trace) {
					stackTrace.append("\tat " + traceElement + "\n");
				}
				record.AddField("cause", stackTrace.toString());
			}

			return record;
		}

	}
}
