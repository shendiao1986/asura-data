package org.asura.data.condition;

public interface ICondition<T> extends IExpParser<ICondition<T>>, IClausable {
	public boolean meet(T paramT);

	public ICondition<T> parse(String paramString);
}
