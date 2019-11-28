package org.asura.data.condition;

import java.util.ArrayList;
import java.util.List;

public class StartEnd {
	private int start;
	private int end;

	public StartEnd(int start, int end) {
		this.start = start;
		this.end = end;
	}

	public int getStart() {
		return this.start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return this.end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	public String[] getStrings(String[] ss, StartEnd[] subStartEnds) {
		List<String> list = new ArrayList<>();
		for (int i = this.start + 1; i < this.end; ++i) {
			StartEnd se = getStartEnd(subStartEnds, i);
			if (se == null) {
				list.add(ss[i]);
			} else if (!(list.contains(se.toKey()))) {
				list.add(se.toKey());
			}

		}

		return ((String[]) list.toArray(new String[0]));
	}

	private StartEnd getStartEnd(StartEnd[] ses, int i) {
		for (StartEnd se : ses) {
			if ((se.start <= i) && (se.end >= i)) {
				return se;
			}
		}

		return null;
	}

	public StartEnd[] getChildren(StartEnd[] startEnds) {
		List<StartEnd> list = new ArrayList<>();

		for (StartEnd se : startEnds) {
			if (contains(se)) {
				list.add(se);
			}
		}

		return ((StartEnd[]) list.toArray(new StartEnd[0]));
	}

	public String toKey() {
		return this.start + "-" + this.end;
	}

	public static StartEnd fromKey(String key) {
		String[] ss = key.split("-");

		return new StartEnd(Integer.valueOf(ss[0]).intValue(), Integer.valueOf(ss[1]).intValue());
	}

	public boolean contains(StartEnd startEnd) {
		if (equals(startEnd)) {
			return false;
		}

		return ((this.start <= startEnd.start) && (this.end >= startEnd.end));
	}

	public int hashCode() {
		int prime = 31;
		int result = 1;
		result = prime * result + this.end;
		result = prime * result + this.start;
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (super.getClass() != obj.getClass())
			return false;
		StartEnd other = (StartEnd) obj;
		if (this.end != other.end) {
			return false;
		}
		return (this.start == other.start);
	}
}
