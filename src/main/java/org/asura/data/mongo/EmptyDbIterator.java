package org.asura.data.mongo;

import java.util.Iterator;

import org.bson.Document;

public class EmptyDbIterator implements Iterator<Document> {
	public boolean hasNext() {
		return false;
	}

	public Document next() {
		return null;
	}

	public void remove() {
	}
}