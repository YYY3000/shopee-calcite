package org.apache.calcite.adapter.druid.query;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.druid.QueryType;

import java.util.List;
import java.util.Objects;

public class QuerySpec {

  final QueryType queryType;
  final BaseDruidQuery query;
  final String queryString;
  final List<String> fieldNames;

  public QuerySpec(QueryType queryType, BaseDruidQuery query,
      List<String> fieldNames) {
    this.queryType = Objects.requireNonNull(queryType, "queryType");
    this.query = Objects.requireNonNull(query, "query");
    this.queryString = query.toQuery();
    this.fieldNames = ImmutableList.copyOf(fieldNames);
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public BaseDruidQuery getQuery() {
    return query;
  }

  public String getQueryString() {
    return queryString;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryType, queryString, fieldNames);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this
        || obj instanceof QuerySpec
        && queryType == ((QuerySpec) obj).queryType
        && queryString.equals(((QuerySpec) obj).queryString)
        && fieldNames.equals(((QuerySpec) obj).fieldNames);
  }

  @Override
  public String toString() {
    return "{queryType: " + queryType
        + ", queryString: " + queryString
        + ", fieldNames: " + fieldNames + "}";
  }

  public String getQueryStringWithPage(String pagingIdentifier, int offset) {
    if (pagingIdentifier == null) {
      return queryString;
    }
    return queryString.replace("\"threshold\":",
        "\"pagingIdentifiers\":{\"" + pagingIdentifier + "\":" + offset
            + "},\"threshold\":");
  }

}
