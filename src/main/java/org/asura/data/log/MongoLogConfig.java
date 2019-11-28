package org.asura.data.log;

import java.io.InputStream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class MongoLogConfig {

	private String host;
	private String logDb;
	private String logCollection;

	private static XStream xs = new XStream(new DomDriver());

	static {
		xs.alias("mongo-log-info", MongoLogConfig.class);
		xs.aliasField("host", MongoLogConfig.class, "host");
		xs.aliasField("db-name", MongoLogConfig.class, "logDb");
		xs.aliasField("collection-name", MongoLogConfig.class, "logCollection");
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getLogDb() {
		return logDb;
	}

	public void setLogDb(String logDb) {
		this.logDb = logDb;
	}

	public String getLogCollection() {
		return logCollection;
	}

	public void setLogCollection(String logCollection) {
		this.logCollection = logCollection;
	}

	@Override
	public String toString() {
		return "MongoLogConfig [host=" + host + ", logDb=" + logDb + ", logCollection=" + logCollection + "]";
	}

	public static String toXML(MongoLogConfig config) {
		return xs.toXML(config);
	}

	public static MongoLogConfig fromXml(String xml) {
		return ((MongoLogConfig) xs.fromXML(xml));
	}

	public static MongoLogConfig fromConfigFile() {
		return fromConfigFile(MongoLogConfig.class.getClassLoader().getResourceAsStream("mongo-log.xml"));
	}

	public static MongoLogConfig fromConfigFile(InputStream stream) {
		MongoLogConfig ci = null;
		try {
			ci = (MongoLogConfig) xs.fromXML(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ci;
	}

	public static MongoLogConfig fromClassLoaderFile(String fileName) {
		MongoLogConfig ci = null;
		try {
			InputStream stream = MongoLogConfig.class.getClassLoader().getResourceAsStream(fileName);
			ci = (MongoLogConfig) xs.fromXML(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ci;
	}

}
