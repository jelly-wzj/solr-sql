package com.jenkin.filters;

import com.jenkin.exceptions.SolrSqlException;
import com.jenkin.solr.SolrTranslator.SolrFilter;

public class OrSolrFilter implements SolrFilter {
	private SolrFilter left;
	private SolrFilter right;

	public OrSolrFilter(SolrFilter left, SolrFilter right) {
		this.left = left;
		this.right = right;
	}

	public SolrFilter getLeft() {
		return left;
	}

	public void setLeft(SolrFilter left) {
		this.left = left;
	}

	public SolrFilter getRight() {
		return right;
	}

	public void setRight(SolrFilter right) {
		this.right = right;
	}

	@Override
	public String toSolrQueryString() throws SolrSqlException {
		// TODO Auto-generated method stub
		return left.toSolrQueryString() + " OR " + right.toSolrQueryString();
	}
}