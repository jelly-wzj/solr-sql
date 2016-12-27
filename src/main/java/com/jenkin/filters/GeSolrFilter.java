package com.jenkin.filters;

public class GeSolrFilter extends UnrecognizedSolrFilter {

	public GeSolrFilter(String attributeName, Object value) {
		super(attributeName, value);
	}

	@Override
	public String toSolrQueryString() {
		// TODO Auto-generated method stub
		return String.format("%s:[%s TO *]", getAttributeName(), getValue());
	}

}