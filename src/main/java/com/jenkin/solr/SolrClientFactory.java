package com.jenkin.solr;

import org.apache.solr.client.solrj.SolrClient;

public interface SolrClientFactory {
	SolrClient getClient();
}
