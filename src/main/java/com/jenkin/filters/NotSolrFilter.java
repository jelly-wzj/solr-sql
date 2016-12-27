package com.jenkin.filters;

import com.jenkin.exceptions.SolrSqlException;
import com.jenkin.solr.SolrTranslator.SolrFilter;

public class NotSolrFilter implements SolrFilter {
	private SolrFilter left;

	public SolrFilter getLeft() {
		return left;
	}

	public void setLeft(SolrFilter left) {
		this.left = left;
	}

	@Override
	public String toSolrQueryString() throws SolrSqlException {
		throw new SolrSqlException(String.format("should never be called: %s*", left));
	}

	public NotSolrFilter(SolrFilter left) {
		this.left = left;
	}
}