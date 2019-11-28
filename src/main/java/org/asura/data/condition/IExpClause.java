package org.asura.data.condition;

public interface IExpClause extends IClausable {
	public void addAnd(IClausable paramIClausable, boolean paramBoolean);

	public void addOr(IClausable paramIClausable, boolean paramBoolean);

	public void addApend(IClausable paramIClausable, boolean paramBoolean);

	public IExpClause clone();
}
