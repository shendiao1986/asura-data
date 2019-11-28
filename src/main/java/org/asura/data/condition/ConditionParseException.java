package org.asura.data.condition;

public class ConditionParseException extends RuntimeException {
	private static final long serialVersionUID = -6679347737687740692L;

	public ConditionParseException() {
	}

	public ConditionParseException(String message) {
		super(message);
	}

	public ConditionParseException(Exception e) {
		super(e);
	}
}
