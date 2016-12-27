package com.jenkin.filters;

public class LikeSolrFilter extends UnrecognizedSolrFilter {

	public LikeSolrFilter(String attributeName, Object value) {
		super(attributeName, value);
	}

	@Override
	public String toSolrQueryString() {
		// TODO Auto-generated method stub
		String value2 = getValue().toString().replaceAll("%", "*").replaceAll("_", "?");
		return String.format("%s:%s", getAttributeName(), value2);
	}

}