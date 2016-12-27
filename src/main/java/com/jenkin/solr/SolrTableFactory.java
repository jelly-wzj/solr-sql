package com.jenkin.solr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.jenkin.exceptions.SolrSqlException;
import com.jenkin.table.SolrTable;
import com.jenkin.utils.Constants;
import com.jenkin.utils.MapUtils;

public class SolrTableFactory implements TableFactory<SolrTable> {

	private final static Logger logger = Logger.getLogger(SolrTableFactory.class);

	public SolrTable create(SchemaPlus parentSchema, String name, final Map<String, Object> operand,
			RelDataType rowTypw) {
		Map<String, SqlTypeName> columns = null;
		SolrClientFactory solrClientFactory = null;
		Map<String, String> filledColumnMapping = null;
		try {
			ArgsParser.argumentsRequired(operand, Constants.COULMNS);
			columns = ArgsParser.parseColumns(operand, Constants.COULMNS);
			filledColumnMapping = MapUtils.mergeMap(columns, ArgsParser.parseMap(operand, Constants.COLUMN_MAPPING));
			solrClientFactory = new SolrClientFactory() {
				List<SolrClient> clients = new ArrayList<>();

				@Override
				public SolrClient getClient() {
					if (operand.containsKey(Constants.SOLR_ZK_HOSTS)) {
						String[] zkHosts = operand.get(Constants.SOLR_ZK_HOSTS).toString().split(",");
						Builder builder = new CloudSolrClient.Builder();
						builder.withZkHost(Arrays.asList(zkHosts));
						CloudSolrClient csc = builder.build();
						csc.setDefaultCollection(operand.get(Constants.DEFAULT_COLLECTIOLN).toString());
						clients.add(csc);
					} else {
						try {
							ArgsParser.argumentsRequired(operand, Constants.SOLR_ZK_HOSTS, Constants.SOLR_SERVER_URL);
							String[] urls = operand.get(Constants.SOLR_SERVER_URL).toString().split(",");
							for (String url : urls) {
								clients.add(new HttpSolrClient.Builder(url).build());
							}
						} catch (SolrSqlException e) {
							logger.error(e);
							return null;
						}
					}
					Collections.shuffle(clients);
					return clients.get(0);
				}
			};

		} catch (SolrSqlException e) {
			logger.error(e);
		}
		return new SolrTable(solrClientFactory, columns, filledColumnMapping, operand);
	}

	public static class ArgsParser {
		private static void argumentsRequired(Map<String, Object> args, String... names) throws SolrSqlException {
			for (String name : names) {
				if (args.containsKey(name)) {
					return;
				}
			}
			throw new SolrSqlException("operand is not empty");
		}

		private static SqlTypeName toSqlTypeName(String typeName) throws SolrSqlException {
			SqlTypeName sqlType = SqlTypeName.get(typeName.toUpperCase());
			if (sqlType == null)
				throw new SolrSqlException(String.format("unknown column type:%s",typeName));
			return sqlType;
		}

		private static Map<String, SqlTypeName> parseColumns(final Map<String, Object> args, final String columnName)
				throws SolrSqlException {
			return parseSafeArgument(args, columnName,
					"comma seperated column definitions, each column is describled in format 'column_name column_type_name', e.g. 'id integer, name char, age integer'",
					"", new Parser() {
						final Map<String, SqlTypeName> _tMap = new LinkedHashMap<>();

						@SuppressWarnings("unchecked")
						public Map<String, SqlTypeName> parse(String value) throws SolrSqlException {
							String column = (String) args.get(columnName);
							String[] fileds = column.split("\\s*,\\s*");
							for (String f : fileds) {
								String[] filedAndValue = f.split("\\s");
								_tMap.put(filedAndValue[0], toSqlTypeName(filedAndValue[1]));
							}
							return _tMap;
						}
					});
		}

		private static Map<String, String> parseMap(final Map<String, Object> args, final String columnName)
				throws SolrSqlException {
			return parseSafeArgument(args, columnName,
					"comma seperated column mappings, each column mapping is describled in format 'columnName->field_name_in_solr_document', e.g. 'name->name_s, age->age_i'",
					"", new Parser() {
						final Map<String, String> _tMap = new HashMap<>();

						@SuppressWarnings("unchecked")
						public Map<String, String> parse(String value) throws SolrSqlException {
							String column = (String) args.get(columnName);
							String[] fileds = column.split("\\s*,\\s*");
							for (String f : fileds) {
								String[] filedAndValue = f.split("\\s*->\\s*");
								_tMap.put(filedAndValue[0], filedAndValue[1]);
							}
							return _tMap;
						}
					});
		}

		public static Integer parseInt(final Map<String, Object> args, final String columnName,
				final String defaultValue) throws SolrSqlException {
			return parseSafeArgument(args, columnName, "a integer number, e.g. 100", "0", new Parser() {
				@SuppressWarnings("unchecked")
				@Override
				public Integer parse(String value) throws SolrSqlException {
					Object v = args.get(columnName) == null ? defaultValue : args.get(columnName);
					return Integer.valueOf(v == null ? "0" : v.toString());
				}
			});
		}

		private static <T> T parseSafeArgument(Map<String, Object> args, String columnName, String expected,
				String defaultValue, Parser p) throws SolrSqlException {
			String value = columnName.isEmpty() ? defaultValue : columnName;
			try {
				return p.parse(value);
			} catch (SolrSqlException e) {
				throw new SolrSqlException(
						String.format("wrong format of column '%s':  %s, expected: %s",columnName,value,expected, e));
			}
		}
	}

	private interface Parser {
		<T> T parse(String value) throws SolrSqlException;
	}

}
