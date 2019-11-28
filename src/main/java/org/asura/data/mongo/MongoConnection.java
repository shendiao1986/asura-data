package org.asura.data.mongo;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.asura.core.exception.DataException;
import org.asura.core.util.ExceptionUtil;
import org.asura.core.util.StringUtil;
import org.asura.data.mysql.ConnectionInformation;

import com.mongodb.ServerAddress;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class MongoConnection {
	private String servers;

	public MongoConnection() {
	}

	public MongoConnection(String servers) {
		this.servers = servers;
	}

	public String getServers() {
		return this.servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public static MongoConnection fromConfig() {
		return fromConfig("mongo.xml");
	}

	public static MongoConnection fromXml(String xml) {
		return ((MongoConnection) getStream().fromXML(xml));
	}

	private static XStream getStream() {
		XStream xs = new XStream(new DomDriver());
		xs.alias("mongo", MongoConnection.class);
		xs.aliasAttribute(MongoConnection.class, "servers", "servers");

		return xs;
	}

	public static MongoConnection fromConfig(String name) {
		MongoConnection ci = null;
		try {
			InputStream stream = ConnectionInformation.class.getClassLoader().getResourceAsStream(name);
			ci = (MongoConnection) getStream().fromXML(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ci;
	}

	public List<ServerAddress> parse() {
		List<ServerAddress> list = new ArrayList<>();
		String[] ss = StringUtil.split(this.servers, ",");
		for (String s : ss) {
			try {
				if (s.contains(":")) {
					list.add(new ServerAddress(s.split(":")[0].trim(), Integer.parseInt(s.split(":")[1].trim())));
					break;
				}
				list.add(new ServerAddress(s.trim(), 27017));
			} catch (Exception e) {
				throw new DataException(
						"failed to parse connection '" + this.servers + "'\n" + ExceptionUtil.getExceptionContent(e));
			}
		}

		return list;
	}

	public int hashCode() {
		int prime = 31;
		int result = 1;
		result = prime * result + ((this.servers == null) ? 0 : this.servers.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (super.getClass() != obj.getClass())
			return false;
		MongoConnection other = (MongoConnection) obj;
		if (this.servers == null)
			if (other.servers != null)
				return false;
			else if (!(this.servers.equals(other.servers)))
				return false;
		return true;
	}
}
