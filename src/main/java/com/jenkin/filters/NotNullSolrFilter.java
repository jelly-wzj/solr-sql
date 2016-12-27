package com.jenkin.filters;

public class NotNullSolrFilter extends UnrecognizedSolrFilter {

	public NotNullSolrFilter(String attributeName) {
		super(attributeName);
	}

	@Override
	public String toSolrQueryString() {
		// TODO Auto-generated method stub
		return String.format("%s:*", getAttributeName());
	}

}