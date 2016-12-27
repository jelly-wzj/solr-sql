package com.jenkin.test;

import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Frameworks.ConfigBuilder;
import org.apache.calcite.tools.Planner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jenkin.solr.SolrTranslator;

public class SqlTranslatorTest {
	static SolrTranslator _trans;

	@BeforeClass
	public static void setup() {
		_trans = new SolrTranslator(Arrays.asList(new String[] { "id", "name_s", "age_i" }));
	}

	@Test
	public void test1() throws Exception {
		Assert.assertEquals("age_i:{20 TO *}", trans("select * from docs where age>20"));
	}

	@Test
	public void test2() throws Exception {
		Assert.assertEquals("name_s:bluejoe", trans("select * from docs where name='bluejoe'"));
	}

	@Test
	public void test3() throws Exception {
		Assert.assertEquals("age_i:{20 TO *} AND name_s:bluejoe",
				trans("select * from docs where age>20 and name='bluejoe'"));
	}

	@Test
	public void test4() throws Exception {
		Assert.assertEquals("age_i:[* TO 20]", trans("select * from docs where NOT (age>20)"));
	}

	@Test
	public void test5() throws Exception {
		Assert.assertEquals("age_i:[* TO 20] OR NOT name_s:bluejoe",
				trans("select * from docs where NOT (age>20 and name='bluejoe')"));
	}

	@Test
	public void test6() throws Exception {
		Assert.assertEquals("age_i:[* TO 20] OR name_s:bluejoe",
				trans("select * from docs where NOT (age>20 and name<>'bluejoe')"));
	}

	@Test
	public void test7() throws Exception {
		Assert.assertEquals("name_s:bluejoe*", trans("select * from docs where name like 'bluejoe%'"));
	}

	private String trans(String sql) throws Exception {
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		String jsonFile = SolrSqlTest.class.getClassLoader().getResource("solr.json").toString().replaceAll("file:/", "");
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		
		CalciteConnection connection = (CalciteConnection) DriverManager.getConnection("jdbc:calcite:model="+jsonFile, info);
		final SchemaPlus schema = connection.getRootSchema().getSubSchema("solr");
		connection.close();

		ConfigBuilder builder = Frameworks.newConfigBuilder().defaultSchema(schema).parserConfig(SqlParser.configBuilder().setCaseSensitive(false).build());
		FrameworkConfig config = builder.build();
		Planner planner = Frameworks.getPlanner(config);
		SqlNode sqlNode = planner.parse(sql);
		SqlNode node = planner.validate(sqlNode);
		RelRoot relRoot = planner.rel(node);
		RelNode project = relRoot.project();
		RexNode condition = ((Filter) ((Project) project).getInput()).getCondition();

		return _trans.translate(condition).toSolrQueryString();
	}
}
