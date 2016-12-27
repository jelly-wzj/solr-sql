package com.jenkin.filters;

public class EqualsSolrFilter extends UnrecognizedSolrFilter {

	public EqualsSolrFilter(String attributeName, Object value) {
		super(attributeName, value);
	}

	@Override
	public String toSolrQueryString() {
		// TODO Auto-generated method stub
		return String.format("%s:%s", getAttributeName(), getValue());
	}

}