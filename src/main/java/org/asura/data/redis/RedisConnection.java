package org.asura.data.redis;

import java.io.InputStream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class RedisConnection {

	private String host;
	private int port;
	private int timeOut;
	private int maxTotal;
	private int maxIdle;
	private long maxWaitMillis;
	private boolean testOnBorrow;
	private String password;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public long getMaxWaitMillis() {
		return maxWaitMillis;
	}

	public void setMaxWaitMillis(long maxWaitMillis) {
		this.maxWaitMillis = maxWaitMillis;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + maxIdle;
		result = prime * result + maxTotal;
		result = prime * result + (int) (maxWaitMillis ^ (maxWaitMillis >>> 32));
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + port;
		result = prime * result + (testOnBorrow ? 1231 : 1237);
		result = prime * result + timeOut;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RedisConnection other = (RedisConnection) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (maxIdle != other.maxIdle)
			return false;
		if (maxTotal != other.maxTotal)
			return false;
		if (maxWaitMillis != other.maxWaitMillis)
			return false;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		if (port != other.port)
			return false;
		if (testOnBorrow != other.testOnBorrow)
			return false;
		if (timeOut != other.timeOut)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RedisConnection [host=" + host + ", port=" + port + ", timeOut=" + timeOut + ", maxTotal=" + maxTotal
				+ ", maxIdle=" + maxIdle + ", maxWaitMillis=" + maxWaitMillis + ", testOnBorrow=" + testOnBorrow
				+ ", password=" + password + "]";
	}

	public static void setXStream(XStream xs) {
		xs.alias("redis-info", RedisConnection.class);
	}

	public static String toXml(RedisConnection information) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);
		return xs.toXML(information);
	}

	public static RedisConnection fromXml(String xml) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);
		return ((RedisConnection) xs.fromXML(xml));
	}

	public static RedisConnection fromConfigFile() {
		return fromConfigFile(RedisConnection.class.getClassLoader().getResourceAsStream("rediscon.xml"));
	}

	public static RedisConnection fromConfigFile(InputStream stream) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);

		RedisConnection ci = null;
		try {
			ci = (RedisConnection) xs.fromXML(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ci;
	}

	public static RedisConnection fromClassLoaderFile(String fileName) {
		XStream xs = new XStream(new DomDriver());
		setXStream(xs);

		RedisConnection ci = null;
		try {
			InputStream stream = RedisConnection.class.getClassLoader().getResourceAsStream(fileName);
			ci = (RedisConnection) xs.fromXML(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ci;
	}
}
