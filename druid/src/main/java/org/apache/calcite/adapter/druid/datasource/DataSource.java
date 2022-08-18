package org.apache.calcite.adapter.druid.datasource;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.calcite.adapter.druid.DruidJson;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;

import java.util.List;
import java.util.Set;

/**
 * Represents a source... of data... for a query. Analogous to the "FROM" clause in SQL.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TableDataSource.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TableDataSource.class, name = "table"),
    @JsonSubTypes.Type(value = QueryDataSource.class, name = "query"),
    @JsonSubTypes.Type(value = UnionDataSource.class, name = "union"),
    @JsonSubTypes.Type(value = JoinDataSource.class, name = "join"),
})
public interface DataSource extends DruidJson {

  List<DruidTable> getDruidTables();

  DruidSchema getDruidSchema();

  /**
   * Returns the names of all table datasources involved in this query. Does not include names
   * for non-tables, like
   * lookups or inline datasources.
   */
  List<String> getTableNames();

  /**
   * Returns datasources that this datasource depends on. Will be empty for leaf datasources like
   * 'table'.
   */
  List<DataSource> getChildren();

  /**
   * Returns true if queries on this dataSource are cacheable at both the result level and
   * per-segment level.
   * Currently, dataSources that do not actually reference segments (like 'inline'), are not
   * cacheable since cache keys
   * are always based on segment identifiers.
   */
  boolean isCacheable(boolean isBroker);

  /**
   * Returns true if all servers have a full copy of this datasource. True for things like
   * inline, lookup, etc, or
   * for queries of those.
   */
  boolean isGlobal();

  /**
   * Returns true if this datasource represents concrete data that can be scanned via a
   * segment adapter of some kind. True for e.g. 'table' but not
   * for 'query' or 'join'.
   */
  boolean isConcrete();
}
