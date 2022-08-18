package org.apache.calcite.adapter.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.calcite.adapter.druid.*;
import org.apache.calcite.adapter.druid.datasource.DataSource;

import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

public class GroupByQuery extends BaseDruidQuery {

  private DataSource dataSource;

  private List<Interval> intervals;

  private DruidJsonFilter jsonFilter;

  private List<DimensionSpec> groupByKeyDims;

  private List<VirtualColumn> virtualColumnList;

  private List<DruidQuery.JsonAggregation> aggregations;

  private List<DruidQuery.JsonExpressionPostAgg> postAggregations;

  private DruidQuery.JsonLimit limit;

  private DruidJsonFilter havingFilter;

  protected GroupByQuery(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<DimensionSpec> groupByKeyDims,
      List<VirtualColumn> virtualColumnList,
      List<DruidQuery.JsonAggregation> aggregations,
      List<DruidQuery.JsonExpressionPostAgg> postAggregations,
      DruidQuery.JsonLimit limit,
      DruidJsonFilter havingFilter) {
    this.dataSource = dataSource;
    this.intervals = intervals;
    this.jsonFilter = jsonFilter;
    this.groupByKeyDims = groupByKeyDims;
    this.virtualColumnList = virtualColumnList;
    this.aggregations = aggregations;
    this.postAggregations = postAggregations;
    this.limit = limit;
    this.havingFilter = havingFilter;
  }

  public static GroupByQuery create(DataSource dataSource,
      List<Interval> intervals,
      DruidJsonFilter jsonFilter,
      List<DimensionSpec> groupByKeyDims,
      List<VirtualColumn> virtualColumnList,
      List<DruidQuery.JsonAggregation> aggregations,
      List<DruidQuery.JsonExpressionPostAgg> postAggregations,
      DruidQuery.JsonLimit limit,
      DruidJsonFilter havingFilter) {
    return new GroupByQuery(dataSource, intervals, jsonFilter, groupByKeyDims, virtualColumnList,
        aggregations, postAggregations, limit, havingFilter);
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("queryType", "groupBy");
    DruidQuery.writeField(generator, "dataSource", dataSource);
    DruidQuery.writeField(generator, "granularity", Granularities.all());
    DruidQuery.writeField(generator, "dimensions", groupByKeyDims);
    DruidQuery.writeFieldIf(generator, "virtualColumns",
        virtualColumnList.size() > 0 ? virtualColumnList : null);
    DruidQuery.writeFieldIf(generator, "limitSpec", limit);
    DruidQuery.writeFieldIf(generator, "filter", jsonFilter);
    DruidQuery.writeField(generator, "aggregations", aggregations);
    DruidQuery.writeFieldIf(generator, "postAggregations",
        postAggregations.size() > 0 ? postAggregations : null);
    DruidQuery.writeField(generator, "intervals", intervals);
    DruidQuery.writeFieldIf(generator, "having",
        havingFilter == null ? null : new DruidJsonFilter.JsonDimHavingFilter(havingFilter));
    generator.writeEndObject();
  }

}
