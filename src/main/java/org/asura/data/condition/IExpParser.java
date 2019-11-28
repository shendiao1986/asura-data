package org.asura.data.condition;

public interface IExpParser<T extends IClausable> {
	public T parse(String paramString);
}
