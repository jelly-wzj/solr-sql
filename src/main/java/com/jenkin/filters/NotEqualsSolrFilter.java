package com.jenkin.filters;

public class NotEqualsSolrFilter extends UnrecognizedSolrFilter {

	public NotEqualsSolrFilter(String attributeName, Object value) {
		super(attributeName, value);
	}

	@Override
	public String toSolrQueryString() {
		// TODO Auto-generated method stub
		return String.format("NOT %s:%s", getAttributeName(), getValue());
	}

}