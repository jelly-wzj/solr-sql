package com.jenkin.filters;

import com.jenkin.solr.SolrTranslator.SolrFilter;

public class UnrecognizedSolrFilter implements SolrFilter {
	private String attributeName;
	private Object value;

	public String getAttributeName() {
		return attributeName;
	}

	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public UnrecognizedSolrFilter() {
	}

	public UnrecognizedSolrFilter(String attributeName) {
		this.attributeName = attributeName;
	}

	public UnrecognizedSolrFilter(String attributeName, Object value) {
		this.attributeName = attributeName;
		this.value = value;
	}

	public String toSolrQueryString(){
		return "*:*";
	}
}