package com.jenkin.filters;

public class IsNullSolrFilter extends UnrecognizedSolrFilter {

	public IsNullSolrFilter(String attributeName) {
		super(attributeName);
	}

	@Override
	public String toSolrQueryString() {
		// TODO Auto-generated method stub
		return String.format("NOT %s:*", getAttributeName());
	}

}