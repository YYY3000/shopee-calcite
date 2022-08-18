package org.apache.calcite.adapter.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.calcite.adapter.druid.*;
import org.apache.calcite.adapter.druid.datasource.DataSource;

import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

public class TimeSeriesQuery extends BaseDruidQuery {

  private DataSource dataSource;

  private List<Interval> intervals;

  private DruidJsonFilter jsonFilter;

  private List<VirtualColumn> virtualColumnList;

  private List<DruidQuery.JsonAggregation> aggregations;

  private List<DruidQuery.JsonExpressionPostAgg> postAggregations;

  private String sortDirection;

  private Granularity timeSeriesGranularity;

  private boolean skipEmptyBuckets;

  protected TimeSeriesQuery(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<VirtualColumn> virtualColumnList,
      List<DruidQuery.JsonAggregation> aggregations,
      List<DruidQuery.JsonExpressionPostAgg> postAggregations,
      String sortDirection,
      Granularity timeSeriesGranularity,
      boolean skipEmptyBuckets) {
    this.dataSource = dataSource;
    this.intervals = intervals;
    this.jsonFilter = jsonFilter;
    this.virtualColumnList = virtualColumnList;
    this.aggregations = aggregations;
    this.postAggregations = postAggregations;
    this.sortDirection = sortDirection;
    this.timeSeriesGranularity = timeSeriesGranularity;
    this.skipEmptyBuckets = skipEmptyBuckets;
  }

  public static TimeSeriesQuery create(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<VirtualColumn> virtualColumnList,
      List<DruidQuery.JsonAggregation> aggregations,
      List<DruidQuery.JsonExpressionPostAgg> postAggregations,
      String sortDirection,
      Granularity timeSeriesGranularity,
      boolean skipEmptyBuckets) {
    return new TimeSeriesQuery(dataSource, intervals, jsonFilter, virtualColumnList, aggregations,
        postAggregations, sortDirection, timeSeriesGranularity, skipEmptyBuckets);
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("queryType", "timeseries");
    DruidQuery.writeField(generator, "dataSource", dataSource);
    generator.writeBooleanField("descending", sortDirection != null
        && sortDirection.equals("descending"));
    DruidQuery.writeField(generator, "granularity", timeSeriesGranularity);
    DruidQuery.writeFieldIf(generator, "filter", jsonFilter);
    DruidQuery.writeField(generator, "aggregations", aggregations);
    DruidQuery.writeFieldIf(generator, "virtualColumns",
        virtualColumnList.size() > 0 ? virtualColumnList : null);
    DruidQuery.writeFieldIf(generator, "postAggregations",
        postAggregations.size() > 0 ? postAggregations : null);
    DruidQuery.writeField(generator, "intervals", intervals);
    generator.writeFieldName("context");
    // The following field is necessary to conform with SQL semantics (CALCITE-1589)
    generator.writeStartObject();
    // Count(*) returns 0 if result set is empty thus need to set skipEmptyBuckets to false
    generator.writeBooleanField("skipEmptyBuckets", skipEmptyBuckets);
    generator.writeEndObject();
  }


}
