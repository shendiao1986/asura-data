package org.asura.data.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.asura.core.data.DataIterator;
import org.asura.core.exception.DataParserException;

public class FilesDataIterator<T> implements DataIterator<T> {
	private IFileDataParser<T, File> parser;
	private IFileFilter filter;
	private IFileHandler handler;
	private Iterator<File> fileIt;
	private File[] files;
	private DataIterator<T> currentIt;

	public FilesDataIterator(String folder) {
		File file = new File(folder);
		this.files = file.listFiles();
	}

	public FilesDataIterator(File[] files) {
		this.files = files;
	}

	private void initialFileIterator() {
		List list = new ArrayList();
		if (this.files != null) {
			for (File f : this.files) {
				if ((this.filter == null) || (this.filter.isValidFile(f))) {
					list.add(f);
				}
			}
		}

		this.fileIt = list.iterator();
	}

	public IFileFilter getFilter() {
		return this.filter;
	}

	public void setFilter(IFileFilter filter) {
		this.filter = filter;
	}

	public IFileDataParser<T, File> getParser() {
		return this.parser;
	}

	public void setParser(IFileDataParser<T, File> parser) {
		this.parser = parser;
	}

	public void close() {
		if (this.currentIt != null)
			this.currentIt.close();
	}

	public boolean hasNext() {
		if ((this.currentIt == null) || (!(this.currentIt.hasNext()))) {
			boolean success = false;
			while ((!(success)) && (getFileIterator().hasNext())) {
				if (this.currentIt != null) {
					this.currentIt.close();
				}
				File file = (File) getFileIterator().next();
				try {
					this.currentIt = this.parser.parse(file);
					if (this.handler != null) {
						this.handler.successFile(file);
					}
					success = true;
				} catch (DataParserException e) {
					this.handler.failFile(file, e);
				}
			}
		}

		if (this.currentIt != null) {
			if (this.currentIt.hasNext()) {
				return true;
			}
			this.currentIt.close();
			return false;
		}

		return false;
	}

	private Iterator<File> getFileIterator() {
		if (this.fileIt == null) {
			initialFileIterator();
		}

		return this.fileIt;
	}

	public T next() {
		return this.currentIt.next();
	}

	public void reset() {
		initialFileIterator();
		if (this.currentIt != null) {
			this.currentIt.close();
		}
		this.currentIt = null;
	}

	public IFileHandler getHandler() {
		return this.handler;
	}

	public void setHandler(IFileHandler handler) {
		this.handler = handler;
	}
}
