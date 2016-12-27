package com.jenkin.solr;

import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.log4j.Logger;

import com.jenkin.exceptions.SolrSqlException;
import com.jenkin.filters.AndSolrFilter;
import com.jenkin.filters.EqualsSolrFilter;
import com.jenkin.filters.GeSolrFilter;
import com.jenkin.filters.GtSolrFilter;
import com.jenkin.filters.IsNullSolrFilter;
import com.jenkin.filters.LeSolrFilter;
import com.jenkin.filters.LikeSolrFilter;
import com.jenkin.filters.LtSolrFilter;
import com.jenkin.filters.NotEqualsSolrFilter;
import com.jenkin.filters.NotNullSolrFilter;
import com.jenkin.filters.NotSolrFilter;
import com.jenkin.filters.OrSolrFilter;
import com.jenkin.filters.UnrecognizedSolrFilter;

public class SolrTranslator {
	private Logger logger = Logger.getLogger(SolrTranslator.class);
	private List<String> solrFieldNames;

	public SolrTranslator(List<String> solrFieldNames) {
		this.solrFieldNames = solrFieldNames;
	}

	/**
	 * @ClassName: SolrFilter
	 * @Description: TODO
	 * @author lucene 语法字符拼接 接口
	 * @date 2016年12月26日
	 *
	 */
	public interface SolrFilter {
		String toSolrQueryString() throws SolrSqlException;
	}

	public SolrFilter translate(RexNode node) {
		// TODO Auto-generated method stub
		return processUnrecognied(processNOT(translateSqlFilter2SolrFilter(node)));
	}

	private RexNode trimColumnCast(RexNode node) {
		if (node instanceof RexCall) {
			RexCall call = (RexCall) node;
			RexNode rexNode = call.operands.get(0);
			if (call.op instanceof SqlCastFunction && rexNode instanceof RexInputRef) {
				return (RexInputRef) rexNode;
			} else {
				return call;
			}
		}
		return node;
	}

	private String translateColumn(RexInputRef ref) {
		return solrFieldNames.get(ref.getIndex());
	}

	private SolrFilter processUnrecognied(SolrFilter filter) {
		if (filter instanceof AndSolrFilter) {
			AndSolrFilter _f = (AndSolrFilter) filter;
			// 只有一个构成函数
			Constructor<?> constructor = _f.getClass().getDeclaredConstructors()[0];
			Class<?>[] parameterTypes = constructor.getParameterTypes();
			if (parameterTypes[0].getSimpleName().equals(UnrecognizedSolrFilter.class.getSimpleName())
					&& parameterTypes[1].getSimpleName().equals(UnrecognizedSolrFilter.class.getSimpleName())) {
				return new UnrecognizedSolrFilter();
			} else if (parameterTypes[0] != null
					&& parameterTypes[1].getSimpleName().equals(UnrecognizedSolrFilter.class.getSimpleName())) {
				try {
					return (SolrFilter) parameterTypes[0].newInstance();
				} catch (Exception e) {
					logger.error(e);
				}
			} else if (parameterTypes[1] != null
					&& parameterTypes[0].getSimpleName().equals(UnrecognizedSolrFilter.class.getSimpleName())) {
				try {
					return (SolrFilter) parameterTypes[1].newInstance();
				} catch (Exception e) {
					logger.error(e);
				}
			}
		} else if (filter instanceof OrSolrFilter) {
			{
				OrSolrFilter _f = (OrSolrFilter) filter;
				Constructor<?> constructor = _f.getClass().getDeclaredConstructors()[0];
				Class<?>[] parameterTypes = constructor.getParameterTypes();

				if (parameterTypes[0].getSimpleName().equals(UnrecognizedSolrFilter.class.getSimpleName())) {
					return new UnrecognizedSolrFilter();
				} else if (parameterTypes[1].getSimpleName().equals(UnrecognizedSolrFilter.class.getSimpleName())) {
					return new UnrecognizedSolrFilter();
				}
			}
		}

		return filter;
	}

	private SolrFilter processNOT(SolrFilter filter) {
		if (filter instanceof AndSolrFilter) {
			AndSolrFilter asf = (AndSolrFilter) filter;
			return new AndSolrFilter(processNOT(asf.getLeft()), processNOT(asf.getRight()));
		} else if (filter instanceof OrSolrFilter) {
			OrSolrFilter osf = (OrSolrFilter) filter;
			return new OrSolrFilter(processNOT(osf.getLeft()), processNOT(osf.getRight()));
		} else if (filter instanceof NotSolrFilter) {
			NotSolrFilter nsf = (NotSolrFilter) filter;
			SolrFilter left = nsf.getLeft();
			if (left instanceof AndSolrFilter) {
				AndSolrFilter _asf = (AndSolrFilter) left;
				return new OrSolrFilter(processNOT(new NotSolrFilter(_asf.getLeft())),
						processNOT(new NotSolrFilter(_asf.getRight())));
			} else if (left instanceof OrSolrFilter) {
				OrSolrFilter _osf = (OrSolrFilter) left;
				return new AndSolrFilter(processNOT(new NotSolrFilter(_osf.getLeft())),
						processNOT(new NotSolrFilter(_osf.getRight())));
			} else if (left instanceof NotSolrFilter) {
				NotSolrFilter _nsf = (NotSolrFilter) left;
				return processNOT(_nsf.getLeft());
			} else if (left instanceof GtSolrFilter) {
				GtSolrFilter _gsf = (GtSolrFilter) left;
				return new LeSolrFilter(_gsf.getAttributeName(), _gsf.getValue());
			} else if (left instanceof GeSolrFilter) {
				GeSolrFilter _gesf = (GeSolrFilter) left;
				return new LtSolrFilter(_gesf.getAttributeName(), _gesf.getValue());
			} else if (left instanceof LtSolrFilter) {
				LtSolrFilter _ltsf = (LtSolrFilter) left;
				return new GeSolrFilter(_ltsf.getAttributeName(), _ltsf.getValue());
			} else if (left instanceof LeSolrFilter) {
				LeSolrFilter _lesf = (LeSolrFilter) left;
				return new GtSolrFilter(_lesf.getAttributeName(), _lesf.getValue());
			} else if (left instanceof EqualsSolrFilter) {
				EqualsSolrFilter _elsf = (EqualsSolrFilter) left;
				return new NotEqualsSolrFilter(_elsf.getAttributeName(), _elsf.getValue());
			} else if (left instanceof NotEqualsSolrFilter) {
				NotEqualsSolrFilter _nesf = (NotEqualsSolrFilter) left;
				return new EqualsSolrFilter(_nesf.getAttributeName(), _nesf.getValue());
			} else if (left instanceof NotNullSolrFilter) {
				NotNullSolrFilter _nnsf = (NotNullSolrFilter) left;
				return new IsNullSolrFilter(_nnsf.getAttributeName());
			} else if (left instanceof IsNullSolrFilter) {
				IsNullSolrFilter _lssf = (IsNullSolrFilter) left;
				return new NotNullSolrFilter(_lssf.getAttributeName());
			}
		}
		return filter;
	}

	private SolrFilter translateSqlFilter2SolrFilter(RexNode node) {
		if (node instanceof RexCall) {
			RexCall _node = (RexCall) node;
			RexNode left = trimColumnCast(_node.operands.get(0));
			RexNode right = (_node.operands.size() > 1) ? trimColumnCast(_node.operands.get(1)) : null;
			switch (node.getKind()) {
			case AND:
				return new AndSolrFilter(translate(left), translate(right));
			case OR:
				return new OrSolrFilter(translate(left), translate(right));
			case IS_NULL:
				if (left instanceof RexInputRef && right == null) {
					RexInputRef ref = (RexInputRef) left;
					return new IsNullSolrFilter(translateColumn(ref));
				}
				break;
			case IS_NOT_NULL:
				if (left instanceof RexInputRef && right == null) {
					RexInputRef ref = (RexInputRef) left;
					return new NotNullSolrFilter(translateColumn(ref));
				}
				break;
			case GREATER_THAN:
				if (left instanceof RexInputRef && right instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) left;
					RexLiteral lit = (RexLiteral) right;
					return new GtSolrFilter(translateColumn(ref), lit.getValue2());
				} else if (right instanceof RexInputRef && left instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) right;
					RexLiteral lit = (RexLiteral) left;
					return new LeSolrFilter(translateColumn(ref), lit.getValue2());
				}
				break;
			case LESS_THAN:
				if (left instanceof RexInputRef && right instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) left;
					RexLiteral lit = (RexLiteral) right;
					return new LtSolrFilter(translateColumn(ref), lit.getValue2());
				} else if (right instanceof RexInputRef && left instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) right;
					RexLiteral lit = (RexLiteral) left;
					return new GeSolrFilter(translateColumn(ref), lit.getValue2());
				}
				break;
			case GREATER_THAN_OR_EQUAL:
				if (left instanceof RexInputRef && right instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) left;
					RexLiteral lit = (RexLiteral) right;
					return new GeSolrFilter(translateColumn(ref), lit.getValue2());
				} else if (right instanceof RexInputRef && left instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) right;
					RexLiteral lit = (RexLiteral) left;
					return new LtSolrFilter(translateColumn(ref), lit.getValue2());
				}
				break;
			case LESS_THAN_OR_EQUAL:
				if (left instanceof RexInputRef && right instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) left;
					RexLiteral lit = (RexLiteral) right;
					return new LeSolrFilter(translateColumn(ref), lit.getValue2());
				} else if (right instanceof RexInputRef && left instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) right;
					RexLiteral lit = (RexLiteral) left;
					return new GtSolrFilter(translateColumn(ref), lit.getValue2());
				}
				break;
			case EQUALS:
				if (left instanceof RexInputRef && right instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) left;
					RexLiteral lit = (RexLiteral) right;
					return new EqualsSolrFilter(translateColumn(ref), lit.getValue2());
				} else if (right instanceof RexInputRef && left instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) right;
					RexLiteral lit = (RexLiteral) left;
					return new EqualsSolrFilter(translateColumn(ref), lit.getValue2());
				}
				break;
			case LIKE:
				if (left instanceof RexInputRef && right instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) left;
					RexLiteral lit = (RexLiteral) right;
					return new LikeSolrFilter(translateColumn(ref), lit.getValue2());
				} else if (right instanceof RexInputRef && left instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) right;
					RexLiteral lit = (RexLiteral) left;
					return new LikeSolrFilter(translateColumn(ref), lit.getValue2());
				}
				break;
			case NOT_EQUALS:
				if (left instanceof RexInputRef && right instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) left;
					RexLiteral lit = (RexLiteral) right;
					return new NotEqualsSolrFilter(translateColumn(ref), lit.getValue2());
				} else if (right instanceof RexInputRef && left instanceof RexLiteral) {
					RexInputRef ref = (RexInputRef) right;
					RexLiteral lit = (RexLiteral) left;
					return new NotEqualsSolrFilter(translateColumn(ref), lit.getValue2());
				}
				break;
			case NOT:
				if (right == null) {
					return new NotSolrFilter(translate(left));
				}
				break;
			default:
				return new UnrecognizedSolrFilter();
			}
		}
		return null;
	}
}
