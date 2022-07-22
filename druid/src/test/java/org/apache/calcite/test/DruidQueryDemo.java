package org.apache.calcite.test;

import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.Properties;
import java.util.Set;

public class DruidQueryDemo {

  public static void main(String[] args) {
//    DruidSchema druidSchema =
//          new DruidSchema(
//              "http://10.128.144.98:8888/",
//              "http://10.128.144.98:8888/",
//              "admin",
//              "TODEdXYSQ3r2W6RIOgTk92nioKqmENl4Hcy5MXcT",
//              true);
//
//      Set<String> tableNames = druidSchema.getTableNames();
//      System.out.println(tableNames);

    Properties info = new Properties();
    info.setProperty("caseSensitive", "false");
    info.setProperty("model", "/Users/yiyunyin/java/shopee-calcite/druid/src/test/resources" +
        "/druid-campaign-model.json");
    try(Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

      SchemaPlus schema = calciteConnection.getRootSchema().getSubSchema("mysql_test");
      System.out.println(schema.getName());

      String sql1 = "SELECT a.* FROM \n" +
          "(SELECT \n" +
          "shopid, floor(__time to MINUTE ) as dt,\n" +
          "sum(view) as pv,APPROX_COUNT_DISTINCT(uniq_deviceid) as uv\n" +
          "FROM druid_campaign_station.shopee_mkpldp_campaign__mp_sg_shop_item_traffic_view\n" +
          "WHERE __time>='2022-04-20T06:00:00.000Z'\n" +
          "and __time<'2022-04-26T06:00:00.000Z'\n" +
          "GROUP by shopid, floor(__time to MINUTE )\n" +
          "limit 10) as a";
      String sql2 = "select * from mysql_test.shop_tab limit 10";
      Statement statement = calciteConnection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql1);

      while (resultSet.next()) {
        int columnIndex1 = resultSet.findColumn("shopid");
        int columnIndex2 = resultSet.findColumn("dt");
        System.out.println(resultSet.getString(columnIndex1));
        System.out.println(resultSet.getString(columnIndex2));
      }

      resultSet.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
