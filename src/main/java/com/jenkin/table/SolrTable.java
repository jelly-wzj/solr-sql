package com.jenkin.table;

import com.jenkin.exceptions.SolrSqlException;
import com.jenkin.solr.SolrClientFactory;
import com.jenkin.solr.SolrTableFactory.ArgsParser;
import com.jenkin.solr.SolrTranslator;
import com.jenkin.utils.Constants;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class SolrTable extends AbstractTable implements ScannableTable, FilterableTable {
	private final Logger logger = Logger.getLogger(SolrTable.class);
	private int pageSize;
	private SolrClientFactory solrClientFactory;
	private Map<String, SqlTypeName> columns;
	private Map<String, String> columnMapping;

	public SolrTable(SolrClientFactory solrClientFactory, Map<String, SqlTypeName> columns,
			Map<String, String> columnMapping, Map<String, Object> options) {
		this.solrClientFactory = solrClientFactory;
		this.columns = columns;
		this.columnMapping = columnMapping;
		try {
			pageSize = ArgsParser.parseInt(options, Constants.PAGE_SIZE, "50");
		} catch (SolrSqlException e) {
			logger.error(e);
		}
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		Collection<SqlTypeName> values = columns.values();
		List<RelDataType> relDateTypeList = new ArrayList<>();
		for (SqlTypeName sqlTypeName : values) {
			relDateTypeList.add(typeFactory.createSqlType(sqlTypeName));
		}
		return typeFactory.createStructType(relDateTypeList, new ArrayList<>(columns.keySet()));
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root) {
		SolrQuery solrQuery = new SolrQuery();
		solrQuery.setQuery("*:*");
		return Linq4j.asEnumerable(new SolrQueryResults(solrClientFactory, solrQuery, pageSize));
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
		SolrQuery solrQuery = null;
		try {
			solrQuery = buildSolrQuery(filters);
		} catch (SolrSqlException e) {
			logger.error(e);
		}
		return Linq4j.asEnumerable(new SolrQueryResults(solrClientFactory, solrQuery, pageSize));
	}

	public SolrQuery buildEmptySolrQuery() {
		return new SolrQuery();
	}

	private SolrQuery buildSolrQuery(List<RexNode> filters) throws SolrSqlException {
		final SolrQuery solrQuery = new SolrQuery();
		if (filters.isEmpty())
			solrQuery.setQuery("*:*");
		else
			solrQuery.setQuery(new SolrTranslator(new ArrayList<String>(columnMapping.values())).translate(filters.get(0))
					.toSolrQueryString());
		return solrQuery;
	}

	class SolrQueryResults implements Iterable<Object[]> {
		private SolrClientFactory solrClientFactory;
		private SolrQuery solrQuery;
		private int pageSize;

		public SolrQueryResults(SolrClientFactory solrClientFactory, SolrQuery solrQuery, int pageSize) {
			this.solrClientFactory = solrClientFactory;
			this.solrQuery = solrQuery;
			this.pageSize = pageSize;
		}

		@Override
		public Iterator<Object[]> iterator() {

			return new SolrQueryResultsIterator(solrClientFactory, solrQuery, pageSize);
		}
	}

	class SolrQueryResultsIterator implements Iterator<Object[]> {
		private SolrClientFactory solrClientFactory;
		private SolrQuery mySolrQuery;
		private int pageSize;
		private int startOfCurrentPage = 0;
		private long totalCountOfRows = -1L;
		private Iterator<Object[]> rowIteratorWithinCurrentPage;
		

		public SolrQueryResultsIterator(SolrClientFactory solrClientFactory, SolrQuery solrQuery, int pageSize) {
			this.solrClientFactory = solrClientFactory;
			this.mySolrQuery = solrQuery;
			this.pageSize = pageSize;
			readNextPage();
		}

		private Object fieldValue2ColumnValue(SolrDocument doc, String fieldName, SqlTypeName targetType)
				throws SolrSqlException {
			Object value = doc.getFieldValue(fieldName);
			if (value == null) {
				return null;
			}
			switch (targetType) {
			case CHAR:
			case VARCHAR:
				return value instanceof String ? value : value.toString();
			case INTEGER:
				return value instanceof Integer ? value : Integer.valueOf(value.toString());
			case BIGINT:
				return value instanceof Long ? value : Long.valueOf(value.toString());
			case FLOAT:
				return value instanceof Float ? value : Float.valueOf(value.toString());
			case DOUBLE:
				return value instanceof Double ? value : Double.valueOf(value.toString());
			case DATE:
				try {
					return value instanceof Date ? value : new SimpleDateFormat("yyyy-mm-dd").parse(value.toString());
				} catch (ParseException e) {
					logger.error(e);
				}
			default:
				throw new SolrSqlException(String.format("unexpected value: %s, type %s required",value,targetType));
			}
		}

		private boolean readNextPage() {
			if (totalCountOfRows < 0 || startOfCurrentPage < totalCountOfRows) {
				mySolrQuery.set("start", startOfCurrentPage);
				mySolrQuery.set("rows", pageSize);
				startOfCurrentPage += pageSize;
				QueryResponse rsp = null;
				List<Object[]> rows = new ArrayList<>();
				try {
					rsp = solrClientFactory.getClient().query(mySolrQuery);
					SolrDocumentList docs = rsp.getResults();
					totalCountOfRows = docs.getNumFound();
					for (SolrDocument doc : docs) {
						rows.add(doc2Row(doc));
					}
				} catch (Exception e) {
					logger.error(e);
				}
				rowIteratorWithinCurrentPage = rows.iterator();
				return true;
			}
			return false;
		}

		private Object[] doc2Row(SolrDocument doc) throws SolrSqlException {
			final List<Object> _l = new ArrayList<>();
			for (Entry<String, SqlTypeName> _cMap : columns.entrySet()) {
				_l.add(fieldValue2ColumnValue(doc, columnMapping.get(_cMap.getKey()), _cMap.getValue()));
			}
			return _l.toArray();
		}

		@Override
		public boolean hasNext() {
			return rowIteratorWithinCurrentPage.hasNext() || startOfCurrentPage < totalCountOfRows;
		}

		@Override
		public Object[] next() {
			if (!rowIteratorWithinCurrentPage.hasNext()) {
				if (!readNextPage())
					throw new NoSuchElementException();
			}
			return rowIteratorWithinCurrentPage.next();
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
		}

	}
}
