package org.apache.calcite.adapter.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.calcite.adapter.druid.DruidJsonFilter;
import org.apache.calcite.adapter.druid.VirtualColumn;
import org.apache.calcite.adapter.druid.datasource.DataSource;
import org.apache.calcite.adapter.druid.DruidQuery;

import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

public class ScanQuery extends BaseDruidQuery {

  private DataSource dataSource;

  private List<Interval> intervals;

  private DruidJsonFilter jsonFilter;

  private List<VirtualColumn> virtualColumnList;

  private List<String> columns;

  private Integer fetchLimit;

  private String direction;

  protected ScanQuery(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<VirtualColumn> virtualColumnList,
      List<String> columns,
      Integer fetchLimit,
      String direction) {
    this.dataSource = dataSource;
    this.intervals = intervals;
    this.jsonFilter = jsonFilter;
    this.virtualColumnList = virtualColumnList;
    this.columns = columns;
    this.fetchLimit = fetchLimit;
    this.direction = direction;
  }

  public static ScanQuery create(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<VirtualColumn> virtualColumnList,
      List<String> columns,
      Integer fetchLimit,
      String direction) {
    return new ScanQuery(dataSource, intervals, jsonFilter, virtualColumnList, columns,
        fetchLimit, direction);
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("queryType", "scan");
    DruidQuery.writeField(generator, "dataSource", dataSource);
    DruidQuery.writeField(generator, "intervals", intervals);
    DruidQuery.writeFieldIf(generator, "filter", jsonFilter);
    DruidQuery.writeFieldIf(generator, "virtualColumns",
        virtualColumnList.size() > 0 ? virtualColumnList : null);
    DruidQuery.writeField(generator, "columns", columns);
    generator.writeStringField("resultFormat", "compactedList");
    if (fetchLimit != null) {
      generator.writeNumberField("limit", fetchLimit);
    }
    if (direction != null) {
      generator.writeStringField("order", direction);
    }
    generator.writeEndObject();
  }
}
