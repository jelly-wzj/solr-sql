package com.jenkin.test;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class SolrSqlTest {

	@BeforeClass
	public static void setup() throws Exception {
		// setupSolrDocuments();
	}

	@Test
	public void test() {
		try {
			Assert.assertEquals(1, query("select * from docs where age>35 and name='bluejoe'").size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test2() throws Exception {
		Assert.assertEquals(3, query("select * from docs limit 10").size());
	}

	@Test
	public void test3() throws Exception {
		Assert.assertEquals(1, query("select * from docs where age<35").size());
	}

	@Test
	public void test4() throws Exception {
		Assert.assertEquals(1, query("select * from docs where age>35").size());
	}

	@Test
	public void test5() throws Exception {
		Assert.assertEquals(2, query("select * from docs where age>=35").size());
	}

	@Test
	public void test6() throws Exception {
		Assert.assertEquals(2, query("select * from docs where age<=35").size());
	}

	@Test
	public void test7() throws Exception {
		Assert.assertEquals(2, query("select * from docs where not (age>35)").size());
	}

	@Test
	public void test8() throws Exception {
		Assert.assertEquals(2, query("select * from docs where not (age>35 and name='bluejoe')").size());
	}

	@Test
	public void test9() throws Exception {
		Assert.assertEquals(2, query("select * from docs where age>35 or name='even'").size());
	}

	private static void setupSolrDocuments() throws SolrServerException, IOException {
		Builder builder = new CloudSolrClient.Builder();
		builder.withZkHost(
				Arrays.asList(new String[] { "192.168.204.181:2181,192.168.204.182:2181,192.168.204.183:2181" }));
		CloudSolrClient client = builder.build();
		client.setDefaultCollection("sql");
		client.deleteByQuery("*:*");
		insertDocument(client, 1, "bluejoe", 38);
		insertDocument(client, 2, "even", 35);
		insertDocument(client, 3, "alex", 8);
		client.commit();
		client.close();
	}

	private static void insertDocument(CloudSolrClient client, int id, String name, int age)
			throws SolrServerException, IOException {
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", id);
		doc.addField("name_s", name);
		doc.addField("age_i", age);
		client.add(doc);
	}

	public static List<Map<String, Object>> query(String sql) throws Exception {
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		String jsonFile = SolrSqlTest.class.getClassLoader().getResource("solr.json").toString().replaceAll("file:/",
				"");
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + jsonFile, info);
		Statement statement = connection.createStatement();
		System.out.println("executing sql: " + sql);
		ResultSet resultSet = statement.executeQuery(sql);
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		while (resultSet.next()) {
			Map<String, Object> row = new HashMap<String, Object>();
			rows.add(row);
			for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
				row.put(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
			}
		}
		resultSet.close();
		statement.close();
		connection.close();
		System.out.println(rows);
		return rows;
	}

}
