package com.jenkin.filters;

public class LeSolrFilter extends UnrecognizedSolrFilter {

	public LeSolrFilter(String attributeName, Object value) {
		super(attributeName, value);
	}

	@Override
	public String toSolrQueryString() {
		// TODO Auto-generated method stub
		return String.format("%s:[* TO %s]", getAttributeName(), getValue());
	}

}