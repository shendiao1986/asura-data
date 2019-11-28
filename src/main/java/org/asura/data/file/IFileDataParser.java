package org.asura.data.file;

import org.asura.core.data.DataIterator;
import org.asura.core.exception.DataParserException;

public interface IFileDataParser<T, F> {
	public DataIterator<T> parse(F paramF) throws DataParserException;
}
