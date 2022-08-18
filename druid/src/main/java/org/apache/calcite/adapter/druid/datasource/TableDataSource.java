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
import java.util.Objects;

@JsonTypeName("table")
public class TableDataSource implements DataSource {

  private DruidTable druidTable;

  @JsonCreator
  protected TableDataSource(DruidTable druidTable) {
    this.druidTable = Preconditions.checkNotNull(druidTable, "'druid table' must be nonnull");
  }

  @JsonCreator
  public static TableDataSource create(final DruidTable druidTable) {
    return new TableDataSource(druidTable);
  }

  public DruidTable getDruidTable() {
    return druidTable;
  }

  @Override
  public List<DruidTable> getDruidTables() {
    return Collections.singletonList(druidTable);
  }

  @Override
  public DruidSchema getDruidSchema() {
    return druidTable.getSchema();
  }

  @JsonProperty
  public String getName() {
    return druidTable.getDataSource();
  }

  @Override
  public List<String> getTableNames() {
    return Collections.singletonList(druidTable.getDataSource());
  }

  @Override
  public List<DataSource> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean isCacheable(boolean isBroker) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean isConcrete() {
    return true;
  }

  @Override
  public String toString() {
    return druidTable.getDataSource();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableDataSource that = (TableDataSource) o;
    return this.getName().equals(that.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(druidTable.getDataSource());
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    DruidQuery.writeField(generator, "type", "table");
    DruidQuery.writeField(generator, "name", druidTable.getDataSource());
    generator.writeEndObject();
  }
}
