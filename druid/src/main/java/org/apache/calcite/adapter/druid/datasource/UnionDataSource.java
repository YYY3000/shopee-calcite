package org.apache.calcite.adapter.druid.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UnionDataSource implements DataSource {

  @JsonProperty
  private final List<TableDataSource> dataSources;

  @JsonCreator
  public UnionDataSource(@JsonProperty("dataSources") List<TableDataSource> dataSources) {
    if (dataSources.isEmpty()) {
      throw new AssertionError("'dataSources' must be non-null and non-empty for 'union'");
    }

    this.dataSources = dataSources;
  }

  @Override
  public List<DruidTable> getDruidTables() {
    return dataSources.stream()
        .map(input -> Iterables.getOnlyElement(input.getDruidTables()))
        .collect(Collectors.toList());
  }

  @Override
  public DruidSchema getDruidSchema() {
    for (TableDataSource dataSource : dataSources) {
      return dataSource.getDruidSchema();
    }
    return null;
  }

  @Override
  public List<String> getTableNames() {
    return dataSources.stream()
        .map(input -> Iterables.getOnlyElement(input.getTableNames()))
        .collect(Collectors.toList());
  }

  @JsonProperty
  public List<TableDataSource> getDataSources() {
    return dataSources;
  }

  @Override
  public List<DataSource> getChildren() {
    return ImmutableList.copyOf(dataSources);
  }

  @Override
  public boolean isCacheable(boolean isBroker) {
    // Disables result-level caching for 'union' datasources, which doesn't work currently.
    // See https://github.com/apache/druid/issues/8713 for reference.
    //
    // Note that per-segment caching is still effective, since at the time the per-segment cache
    // evaluates a query
    // for cacheability, it would have already been rewritten to a query on a single table.
    return false;
  }

  @Override
  public boolean isGlobal() {
    return dataSources.stream().allMatch(DataSource::isGlobal);
  }

  @Override
  public boolean isConcrete() {
    return dataSources.stream().allMatch(DataSource::isConcrete);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UnionDataSource that = (UnionDataSource) o;

    if (!dataSources.equals(that.dataSources)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return dataSources.hashCode();
  }

  @Override
  public String toString() {
    return "UnionDataSource{" +
        "dataSources=" + dataSources +
        '}';
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    DruidQuery.writeField(generator, "type", "union");
    DruidQuery.writeField(generator, "dataSources", new ArrayList<>(getTableNames()));
    generator.writeEndObject();
  }
}
