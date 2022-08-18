package org.apache.calcite.adapter.druid.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@JsonTypeName("query")
public class QueryDataSource implements DataSource {

  @JsonProperty
  private final DruidQuery query;

  @JsonCreator
  protected QueryDataSource(@JsonProperty("query") DruidQuery query) {
    this.query = Preconditions.checkNotNull(query, "'query' must be nonnull");
  }

  public static QueryDataSource create(DruidQuery query) {
    return new QueryDataSource(query);
  }

  @Override
  public List<DruidTable> getDruidTables() {
    return query.getDataSource().getDruidTables();
  }

  @Override
  public DruidSchema getDruidSchema() {
    return query.getDataSource().getDruidSchema();
  }

  @Override
  public List<String> getTableNames() {
    return query.getDataSource().getTableNames();
  }

  @JsonProperty
  public DruidQuery getQuery() {
    return query;
  }

  @Override
  public List<DataSource> getChildren() {
    return Collections.singletonList(query.getDataSource());
  }

  @Override
  public boolean isCacheable(boolean isBroker) {
    return false;
  }

  @Override
  public boolean isGlobal() {
    return query.getDataSource().isGlobal();
  }

  @Override
  public boolean isConcrete() {
    return false;
  }

  @Override
  public String toString() {
    return query.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryDataSource that = (QueryDataSource) o;

    if (!query.equals(that.query)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return query.hashCode();
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    DruidQuery.writeField(generator, "type", "query");
    DruidQuery.writeField(generator, "query", query.getQuerySpec().getQuery());
    generator.writeEndObject();
  }
}
