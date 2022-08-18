package org.apache.calcite.adapter.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.calcite.adapter.druid.*;
import org.apache.calcite.adapter.druid.datasource.DataSource;

import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

public class TopNQuery extends BaseDruidQuery {

  private DataSource dataSource;

  private List<Interval> intervals;

  private DruidJsonFilter jsonFilter;

  private List<DimensionSpec> groupByKeyDims;

  private List<VirtualColumn> virtualColumnList;

  private DruidJsonTopNMetric topNMetric;

  private List<DruidQuery.JsonAggregation> aggregations;

  private List<DruidQuery.JsonExpressionPostAgg> postAggregations;

  private DruidQuery.JsonLimit limit;

  protected TopNQuery(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<DimensionSpec> groupByKeyDims,
      List<VirtualColumn> virtualColumnList,
      DruidJsonTopNMetric topNMetric,
      List<DruidQuery.JsonAggregation> aggregations,
      List<DruidQuery.JsonExpressionPostAgg> postAggregations,
      DruidQuery.JsonLimit limit) {
    this.dataSource = dataSource;
    this.intervals = intervals;
    this.jsonFilter = jsonFilter;
    this.groupByKeyDims = groupByKeyDims;
    this.virtualColumnList = virtualColumnList;
    this.topNMetric = topNMetric;
    this.aggregations = aggregations;
    this.postAggregations = postAggregations;
    this.limit = limit;
  }

  public static TopNQuery create(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<DimensionSpec> groupByKeyDims,
      List<VirtualColumn> virtualColumnList,
      DruidJsonTopNMetric topNMetric,
      List<DruidQuery.JsonAggregation> aggregations,
      List<DruidQuery.JsonExpressionPostAgg> postAggregations,
      DruidQuery.JsonLimit limit) {
    return new TopNQuery(dataSource, intervals, jsonFilter, groupByKeyDims, virtualColumnList,
        topNMetric, aggregations, postAggregations, limit);
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("queryType", "topN");
    DruidQuery.writeField(generator, "dataSource", dataSource);
    DruidQuery.writeField(generator, "granularity", Granularities.all());
    DruidQuery.writeField(generator, "dimension", groupByKeyDims.get(0));
    DruidQuery.writeFieldIf(generator, "virtualColumns",
        virtualColumnList.size() > 0 ? virtualColumnList : null);
    DruidQuery.writeField(generator, "metric", topNMetric);
    DruidQuery.writeFieldIf(generator, "filter", jsonFilter);
    DruidQuery.writeField(generator, "aggregations", aggregations);
    DruidQuery.writeFieldIf(generator, "postAggregations",
        postAggregations.size() > 0 ? postAggregations : null);
    DruidQuery.writeField(generator, "intervals", intervals);
    generator.writeNumberField("threshold", limit.limit);
    generator.writeEndObject();
  }


}
