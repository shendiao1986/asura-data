package org.asura.data.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.asura.core.data.DataIterator;

public class FileDataLineParser<T> implements IFileDataParser<T, File> {
	private ILineParser<T> lineParser;

	public FileDataLineParser() {
	}

	public FileDataLineParser(ILineParser<T> lineParser) {
		this.lineParser = lineParser;
	}

	public ILineParser<T> getLineParser() {
		return this.lineParser;
	}

	public void setLineParser(ILineParser<T> parser) {
		this.lineParser = parser;
	}

	public DataIterator<T> parse(final File file) {
		return new DataIterator<T>() {
			private BufferedReader reader;
			private FileInputStream stream;
			
			{
				initial();
			}

			private void initial() {
				try {
					this.stream = new FileInputStream(file);
					this.reader = new BufferedReader(new InputStreamReader(this.stream, "UTF8"));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			public void close() {
				try {
					if (this.reader != null) {
						this.reader.close();
					}
					if (this.stream != null)
						this.stream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			public boolean hasNext() {
				try {
					return this.reader.ready();
				} catch (IOException e) {
					e.printStackTrace();
				}

				return false;
			}

			public T next() {
				try {
					return FileDataLineParser.this.lineParser.parse(this.reader.readLine());
				} catch (IOException e) {
					e.printStackTrace();
				}

				return null;
			}

			public void reset() {
				close();
				initial();
			}
		};
	}
}
