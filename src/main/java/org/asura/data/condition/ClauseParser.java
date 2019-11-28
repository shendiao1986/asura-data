package org.asura.data.condition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.asura.core.util.StringUtil;

public class ClauseParser<T extends IClausable> implements IClausable {
	public static final String And = "&&";
	public static final String Or = "||";
	public static final String Append = "**";
	public static final String Not = "!";
	public static final String Left = "(";
	public static final String Right = ")";
	private static Set<String> spliter = new HashSet<>();

	static {
		spliter.add("&&");
		spliter.add("!");
		spliter.add("||");
		spliter.add("**");

		spliter.add("(");
		spliter.add(")");
	}

	public IClausable parse(String exp, IExpParser<T> subParser, IExpClause clause) {
		String[] ss = StringUtil.splitRemainRex(exp, (String[]) spliter.toArray(new String[0]));

		Set<Integer> used = new HashSet<>();
		List<StartEnd> startEnd = new ArrayList<>();

		HashMap<StartEnd, IClausable> map = new HashMap<>();
		boolean success;
		do {
			success = handleParenthese(ss, used, startEnd, map, subParser, clause);
		} while (success);

		String[] total = new StartEnd(-1, ss.length).getStrings(ss, (StartEnd[]) map.keySet().toArray(new StartEnd[0]));

		return parseNoParenthese(total, map, subParser, clause);
	}

	private boolean handleParenthese(String[] ss, Set<Integer> used, List<StartEnd> startEnd,
			HashMap<StartEnd, IClausable> map, IExpParser<T> parser, IExpClause clause) {
		for (int i = 0; i < ss.length; ++i) {
			if ((")".equals(ss[i])) && (!(used.contains(Integer.valueOf(i))))) {
				for (int j = i - 1; j >= 0; --j) {
					if ((")".equals(ss[j])) && (!(used.contains(Integer.valueOf(j))))) {
						break;
					}
					if (("(".equals(ss[j])) && (!(used.contains(Integer.valueOf(j))))) {
						used.add(Integer.valueOf(i));
						used.add(Integer.valueOf(j));

						StartEnd se = new StartEnd(j, i);

						IClausable con = parseNoParenthese(
								se.getStrings(ss, (StartEnd[]) startEnd.toArray(new StartEnd[0])), map, parser, clause);
						map.put(se, con);

						startEnd.add(se);

						for (StartEnd sub : se.getChildren((StartEnd[]) startEnd.toArray(new StartEnd[0]))) {
							startEnd.remove(sub);
							map.remove(sub);
						}

						return true;
					}
				}
			}
		}

		return false;
	}

	public IClausable parseNoParenthese(String[] ss, HashMap<StartEnd, IClausable> map, IExpParser<T> parser,
			IExpClause clause) {
		clause = clause.clone();
		List<Integer> ids = new ArrayList<>();
		for (int i = 0; i < ss.length; ++i) {
			if (isLogic(ss[i])) {
				ids.add(Integer.valueOf(i));
			}
		}

		check(ss, ids);

		HashSet<Integer> used = new HashSet<>();
		HashSet<Integer> nots = new HashSet<>();
		used.addAll(ids);

		if (ss.length == 1)
			clause.addAnd(getCondition(ss[0], map, parser), false);
		else if (ss.length == 2) {
			clause.addAnd(getCondition(ss[1], map, parser), true);
		}

		for (int i = 0; i < ss.length - 1; ++i) {
			if ("!".equals(ss[i])) {
				nots.add(Integer.valueOf(i + 1));
			}

		}

		for (int i = 1; i < ss.length - 1; ++i) {
			if ("&&".equals(ss[i])) {
				if (!(used.contains(Integer.valueOf(i - 1)))) {
					clause.addAnd(getCondition(ss[(i - 1)], map, parser), nots.contains(Integer.valueOf(i - 1)));

					used.add(Integer.valueOf(i - 1));
				}
				int back = i + 1;
				if ("!".equals(ss[back])) {
					back = i + 2;
				}
				if (!(used.contains(Integer.valueOf(back)))) {
					clause.addAnd(getCondition(ss[back], map, parser), nots.contains(Integer.valueOf(back)));

					used.add(Integer.valueOf(back));
				}
			}

		}

		for (int i = 1; i < ss.length - 1; ++i) {
			if ("||".equals(ss[i])) {
				if (!(used.contains(Integer.valueOf(i - 1)))) {
					clause.addOr(getCondition(ss[(i - 1)], map, parser), nots.contains(Integer.valueOf(i - 1)));

					used.add(Integer.valueOf(i - 1));
				}
				int back = i + 1;
				if ("!".equals(ss[back])) {
					back = i + 2;
				}
				if (!(used.contains(Integer.valueOf(back)))) {
					clause.addOr(getCondition(ss[back], map, parser), nots.contains(Integer.valueOf(back)));

					used.add(Integer.valueOf(back));
				}
			}

		}

		for (int i = 1; i < ss.length - 1; ++i) {
			if ("**".equals(ss[i])) {
				if (!(used.contains(Integer.valueOf(i - 1)))) {
					clause.addApend(getCondition(ss[(i - 1)], map, parser), nots.contains(Integer.valueOf(i - 1)));

					used.add(Integer.valueOf(i - 1));
				}
				int back = i + 1;
				if ("!".equals(ss[back])) {
					back = i + 2;
				}
				if (!(used.contains(Integer.valueOf(back)))) {
					clause.addApend(getCondition(ss[back], map, parser), nots.contains(Integer.valueOf(back)));

					used.add(Integer.valueOf(back));
				}
			}
		}

		return clause;
	}

	private IClausable getCondition(String exp, HashMap<StartEnd, IClausable> map, IExpParser<T> subParser) {
		for (StartEnd se : map.keySet()) {
			if (se.toKey().equals(exp)) {
				return ((IClausable) map.get(se));
			}
		}

		return subParser.parse(exp);
	}

	private void check(String[] ss, List<Integer> ids) {
		for (int i = 0; i < ids.size() - 1; ++i) {
			if (((Integer) ids.get(i)).intValue() + 1 == ((Integer) ids.get(i + 1)).intValue()) {
				if (!("!".equals(ss[((Integer) ids.get(i + 1)).intValue()])))
					throw new ConditionParseException(ss[((Integer) ids.get(i)).intValue()] + "和"
							+ ss[((Integer) ids.get(i + 1)).intValue()] + "不能连续使用");
				if ("!".equals(ss[((Integer) ids.get(i)).intValue()])) {
					throw new ConditionParseException("不建议使用!!，等同于没有用");
				}
			}
		}
		if (isLogic(ss[(ss.length - 1)])) {
			throw new ConditionParseException("最后的一个不能为逻辑符号'" + ss[(ss.length - 1)] + "'");
		}
		if (("&&".equals(ss[0])) || ("||".equals(ss[0])) || ("**".equals(ss[0])))
			throw new ConditionParseException("第一个不能为逻辑符号'" + ss[0] + "'");
	}

	private boolean isLogic(String s) {
		return (("!".equals(s)) || ("&&".equals(s)) || ("||".equals(s)) || ("**".equals(s)));
	}
}
