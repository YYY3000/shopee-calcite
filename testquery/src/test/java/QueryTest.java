import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcUtils;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.BuiltInMethod;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

public class QueryTest {

  private static final String modelFile = "/Users/yiyunyin/java/shopee-calcite/testquery/src/test" +
      "/resources/druid-campaign-model.json";

  @Test
  void testDruidQuery() {
    String query = "SELECT shopid, floor(__time to DAY) as dt, sum(view) as pv\n" +
        "FROM druid_campaign_station.shopee_mkpldp_campaign__mp_id_shop_item_traffic_view\n" +
        "WHERE __time>='2022-08-16T00:00:00.000Z'\n" +
        "and __time<'2022-08-18T06:00:00.000Z'\n" +
        "group by shopid, floor(__time to DAY) \n" +
        "order by sum(view) desc \n" +
        "limit 10";

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    info.setProperty("model", modelFile);
    try (CalciteConnection calciteConnection =
             DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
         Statement statement = calciteConnection.createStatement()) {

      // execute query
      try (ResultSet resultSet = statement.executeQuery(query)) {
        while (resultSet.next()) {
          ResultSetMetaData metaData = resultSet.getMetaData();
          for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnIndex = i + 1;
            String columnName = metaData.getColumnName(columnIndex);
            String columnValue = resultSet.getString(columnIndex);
            System.out.println(columnName + ":" + columnValue);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void testDruidMultipleQuery() {
    String query = "SELECT a.__time,a.shopid,a.view,b.region\n" +
        "FROM druid_campaign_station.shopee_mkpldp_campaign__mp_id_shop_item_traffic_view a\n" +
        "LEFT JOIN druid_campaign_station.shopee_mkpldp_campaign__brand_ranking_semart_setting b\n" +
        "ON a.shopid=b.os_shop_id and a.shopid=b.se_shop_id\n" +
        "WHERE a.__time>='2022-08-16T17:00:00.000Z'\n" +
        "and a.__time<'2022-08-18T17:00:00.000Z'\n" +
        "and b.region='ID'\n" +
        "and b.__time='2022-08-12T00:00:00.000Z'";

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    info.setProperty("model", modelFile);
    try (CalciteConnection calciteConnection =
             DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
         Statement statement = calciteConnection.createStatement()) {

      // execute query
      try (ResultSet resultSet = statement.executeQuery(query)) {
        while (resultSet.next()) {
          ResultSetMetaData metaData = resultSet.getMetaData();
          for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnIndex = i + 1;
            String columnName = metaData.getColumnName(columnIndex);
            String columnValue = resultSet.getString(columnIndex);
            System.out.println(columnName + ":" + columnValue);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void testDruidJoinQuery() {
    String query = "SELECT a.__time,a.shopid,a.view,b.region\n" +
        "FROM druid_campaign_station.shopee_mkpldp_campaign__mp_sg_shop_item_traffic_view_campaign_history a\n" +
        "LEFT JOIN druid_campaign_station.shopee_mkpldp_campaign__brand_ranking_semart_setting b\n" +
        "ON a.shopid=b.os_shop_id\n" +
        "WHERE a.__time>='2021-12-30T17:00:00.000Z'\n" +
        "and a.__time<'2021-12-31T17:00:00.000Z'\n" +
        "and b.os_shop_id>0\n";

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    info.setProperty("model", modelFile);
    try (CalciteConnection calciteConnection =
             DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
         Statement statement = calciteConnection.createStatement()) {

      // execute query
      try (ResultSet resultSet = statement.executeQuery(query)) {
        while (resultSet.next()) {
          ResultSetMetaData metaData = resultSet.getMetaData();
          for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnIndex = i + 1;
            String columnName = metaData.getColumnName(columnIndex);
            String columnValue = resultSet.getString(columnIndex);
            System.out.println(columnName + ":" + columnValue);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void testQueryWithManuallySchema() {
    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");

    String sql = "SELECT a.shopid,a.dt,b.shopname FROM \n" +
        "(SELECT shopid, floor(__time to MINUTE ) as dt\n" +
        "FROM druid_campaign_station.shopee_mkpldp_campaign__mp_sg_shop_item_traffic_view\n" +
        "WHERE __time>='2022-04-20T06:00:00.000Z'\n" +
        "and __time<'2022-04-28T06:00:00.000Z'\n" +
        "limit 10) as a\n" +
        "left join\n" +
        "(SELECT * FROM mysql_test_local.shop_tab) as b\n" +
        "ON a.shopid=b.shopid";

    DruidSchema druidSchema =
        new DruidSchema(
            "http://10.128.144.98:8888",
            "http://10.128.144.98:8888",
            "admin",
            "TODEdXYSQ3r2W6RIOgTk92nioKqmENl4Hcy5MXcT",
            true);

    DataSource dataSource =
        JdbcSchema.dataSource("jdbc:mysql://localhost:3306/test",
            "com.mysql.cj.jdbc.Driver",
            "root",
            "3000");
    Expression rootExpression = Expressions.call(
        DataContext.ROOT,
        BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    Expression expression =
        Expressions.call(
            rootExpression,
            BuiltInMethod.SCHEMA_GET_SUB_SCHEMA.method,
            Expressions.constant("mysql_test_local"));

    SqlDialect dialect = JdbcUtils.DialectPool.INSTANCE.get(SqlDialectFactoryImpl.INSTANCE,
        dataSource);
    JdbcConvention convention =
        JdbcConvention.of(dialect, expression, "mysql_test_local");
    JdbcSchema mysqlSchema = new JdbcSchema(dataSource, dialect, convention, null, null);

    try (CalciteConnection calciteConnection =
             DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
         Statement statement = calciteConnection.createStatement()) {
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("druid_campaign_station", druidSchema);
      rootSchema.add("mysql_test_local", mysqlSchema);

      // execute query sql
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          ResultSetMetaData metaData = resultSet.getMetaData();
          for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnIndex = i + 1;
            String columnName = metaData.getColumnName(columnIndex);
            String columnValue = resultSet.getString(columnIndex);
            System.out.println(columnName + ":" + columnValue);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void testSqlExecute() {
    String sql = "SELECT a.shopid,a.dt,b.shopname FROM \n" +
        "(SELECT shopid, floor(__time to MINUTE ) as dt\n" +
        "FROM druid_campaign_station.shopee_mkpldp_campaign__mp_sg_shop_item_traffic_view\n" +
        "WHERE __time>='2022-04-20T06:00:00.000Z'\n" +
        "and __time<'2022-04-26T06:00:00.000Z'\n" +
        "limit 10) as a\n" +
        "left join\n" +
        "(SELECT * FROM mysql_test_local.shop_tab) as b\n" +
        "ON a.shopid=b.shopid";

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    info.setProperty("model", modelFile);
    try (CalciteConnection calciteConnection =
             DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
         Statement statement = calciteConnection.createStatement()) {

      // Parser (sql -> SqlNode)
      SqlParser parser = SqlParser.create(sql);
      SqlNode sqlNode = parser.parseQuery();
      System.out.println(sqlNode);

      CalciteServerStatement calciteServerStatement =
          statement.unwrap(CalciteServerStatement.class);
      CalcitePrepare.Context prepareContext = calciteServerStatement.createPrepareContext();

      SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      CalciteCatalogReader calciteCatalogReader =
          new CalciteCatalogReader(prepareContext.getRootSchema(),
              prepareContext.getDefaultSchemaPath(), typeFactory,
              new CalciteConnectionConfigImpl(info));

      // Validator (SqlNode -> SqlNode)
      SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
          calciteCatalogReader, typeFactory,
          SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
      SqlNode validatedSqlNode = validator.validate(sqlNode);
      System.out.println(validatedSqlNode);

      // To RelNode (SqlNode -> RelNode)
      VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(info));
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
      SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
          .withTrimUnusedFields(true)
          .withExpand(false);
      SqlToRelConverter converter = new SqlToRelConverter(
          null,
          validator,
          calciteCatalogReader,
          cluster,
          StandardConvertletTable.INSTANCE,
          converterConfig);
      RelRoot relRoot = converter.convertQuery(validatedSqlNode, false, true);
      System.out.println(relRoot);

      // Optimizer (RelNode -> RelNode)
      RuleSet rules = RuleSets.ofList(
          CoreRules.FILTER_TO_CALC,
          CoreRules.PROJECT_TO_CALC,
          CoreRules.FILTER_CALC_MERGE,
          CoreRules.PROJECT_CALC_MERGE,
          CoreRules.FILTER_INTO_JOIN,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
          EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_CALC_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
      Program program = Programs.of(RuleSets.ofList(rules));
      RelNode optimizerRelTree = program.run(
          planner,
          relRoot.rel,
          relRoot.rel.getTraitSet().plus(EnumerableConvention.INSTANCE),
          Collections.emptyList(),
          Collections.emptyList());
      System.out.println(optimizerRelTree);

      // Execute

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void testSqlParser() {
    String sql = "SELECT shopid,time_floor(__time to day, 'UTC') \n" +
        "FROM druid_campaign_station.shopee_mkpldp_campaign__mp_sg_shop_item_traffic_view\n" +
        "WHERE __time>='2022-04-20T06:00:00.000Z'\n" +
        "AND __time<'2022-04-28T06:00:00.000Z'\n" +
        "LIMIT 10";

    // Parser (sql -> SqlNode)
    try {
      SqlParser parser = SqlParser.create(sql);
      SqlNode sqlNode = parser.parseQuery();
      System.out.println(sqlNode);
    } catch (SqlParseException e) {
      e.printStackTrace();
    }
  }

}
