package org.asura.data.mysql;

import java.io.InputStream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class ConnectionInformation {
	private String host;
	private String port;
	private String userName;
	private String password;
	private String dbName;
	private String driver;
	private String url;

	public String getDriver() {
		return this.driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return this.port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getUserName() {
		return this.userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDbName() {
		return this.dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getUrl() {
		return this.url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int hashCode() {
		int prime = 31;
		int result = 1;
		result = 31 * result + ((this.dbName == null) ? 0 : this.dbName.hashCode());
		result = 31 * result + ((this.host == null) ? 0 : this.host.hashCode());
		result = 31 * result + ((this.password == null) ? 0 : this.password.hashCode());
		result = 31 * result + ((this.port == null) ? 0 : this.port.hashCode());
		result = 31 * result + ((this.userName == null) ? 0 : this.userName.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (super.getClass() != obj.getClass())
			return false;
		ConnectionInformation other = (ConnectionInformation) obj;
		if (this.dbName == null)
			if (other.dbName != null)
				return false;
			else if (!(this.dbName.equals(other.dbName)))
				return false;
		if (this.host == null)
			if (other.host != null)
				return false;
			else if (!(this.host.equals(other.host)))
				return false;
		if (this.password == null)
			if (other.password != null)
				return false;
			else if (!(this.password.equals(other.password)))
				return false;
		if (this.port == null)
			if (other.port != null)
				return false;
			else if (!(this.port.equals(other.port)))
				return false;
		if (this.userName == null)
			if (other.userName != null)
				return false;
			else if (!(this.userName.equals(other.userName)))
				return false;
		return true;
	}

	public String toString() {
		return "h:" + this.host + "port:" + this.port + "user:" + this.userName + "password:" + this.password
				+ "dbName:" + this.dbName;
	}

	public static String toXml(ConnectionInformation information) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);
		return xs.toXML(information);
	}

	public static ConnectionInformation fromXml(String xml) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);
		return ((ConnectionInformation) xs.fromXML(xml));
	}

	public static ConnectionInformation fromConfigFile() {
		return fromConfigFile(ConnectionInformation.class.getClassLoader().getResourceAsStream("connection.xml"));
	}

	public static ConnectionInformation fromConfigFile(InputStream stream) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);

		ConnectionInformation ci = null;
		try {
			ci = (ConnectionInformation) xs.fromXML(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ci;
	}

	public static ConnectionInformation fromClassLoaderFile(String fileName) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);

		ConnectionInformation ci = null;
		try {
			InputStream stream = ConnectionInformation.class.getClassLoader().getResourceAsStream(fileName);
			ci = (ConnectionInformation) xs.fromXML(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ci;
	}

	public static void setXStream(XStream xs) {
		xs.alias("con-info", ConnectionInformation.class);
		xs.aliasField("user", ConnectionInformation.class, "userName");
		xs.aliasField("db-name", ConnectionInformation.class, "dbName");
	}
}
