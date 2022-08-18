package org.apache.calcite.adapter.druid.datasource;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.calcite.adapter.druid.util.JoinPrefixUtils;
import org.apache.calcite.rel.core.JoinRelType;

import java.io.IOException;
import java.util.*;

/**
 * Represents a join of two datasources.
 * <p>
 * Logically, this datasource contains the result of:
 * <p>
 * (1) prefixing all right-side columns with "rightPrefix"
 * (2) then, joining the left and (prefixed) right sides using the provided type and condition
 * <p>
 * Any columns from the left-hand side that start with "rightPrefix", and are at least one
 * character longer than
 * the prefix, will be shadowed. It is up to the caller to ensure that no important columns are
 * shadowed by the
 * chosen prefix.
 */
public class JoinDataSource implements DataSource {

  private final DruidQuery left;
  private final DruidQuery right;
  private final String rightPrefix;
  private final String condition;
  private final JoinRelType joinType;

  private JoinDataSource(
      DruidQuery left,
      DruidQuery right,
      String rightPrefix,
      String condition,
      JoinRelType joinType
  ) {
    this.left = Preconditions.checkNotNull(left, "left");
    this.right = Preconditions.checkNotNull(right, "right");
    this.rightPrefix = JoinPrefixUtils.validatePrefix(rightPrefix);
    this.condition = Preconditions.checkNotNull(condition, "condition");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
  }

  /**
   * Create a join dataSource from an existing condition
   */
  public static JoinDataSource create(
      final DruidQuery left,
      final DruidQuery right,
      final String rightPrefix,
      final String condition,
      final JoinRelType joinType
  ) {
    return new JoinDataSource(left, right, rightPrefix, condition, joinType);
  }

  /**
   * Create a join dataSource from an existing condition
   */
  public static JoinDataSource create(
      final DruidQuery left,
      final DruidQuery right,
      final String condition,
      final JoinRelType joinType
  ) {
    return new JoinDataSource(left, right, "r.", condition, joinType);
  }

  @Override
  public List<DruidTable> getDruidTables() {
    final List<DruidTable> druidTables = new ArrayList<>();
    druidTables.addAll(left.getDataSource().getDruidTables());
    druidTables.addAll(right.getDataSource().getDruidTables());
    return druidTables;
  }

  @Override
  public DruidSchema getDruidSchema() {
    return left.getDruidSchema();
  }

  @Override
  public List<String> getTableNames() {
    final List<String> names = new ArrayList<>();
    names.addAll(left.getDataSource().getTableNames());
    names.addAll(right.getDataSource().getTableNames());
    return names;
  }

  @JsonProperty
  public DruidQuery getLeft() {
    return left;
  }

  @JsonProperty
  public DruidQuery getRight() {
    return right;
  }

  @JsonProperty
  public String getRightPrefix() {
    return rightPrefix;
  }

  @JsonProperty
  public String getCondition() {
    return condition;
  }

  @JsonProperty
  public JoinRelType getJoinType() {
    return joinType;
  }

  @Override
  public List<DataSource> getChildren() {
    return ImmutableList.of(left.getDataSource(), right.getDataSource());
  }

  @Override
  public boolean isCacheable(boolean isBroker) {
    return left.getDataSource().isCacheable(isBroker) && right.getDataSource().isCacheable(isBroker);
  }

  @Override
  public boolean isGlobal() {
    return left.getDataSource().isGlobal() && right.getDataSource().isGlobal();
  }

  @Override
  public boolean isConcrete() {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinDataSource that = (JoinDataSource) o;
    return Objects.equals(left, that.left) &&
        Objects.equals(right, that.right) &&
        Objects.equals(rightPrefix, that.rightPrefix) &&
        Objects.equals(condition, that.condition) &&
        joinType == that.joinType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right, rightPrefix, condition, joinType);
  }

  @Override
  public String toString() {
    return "JoinDataSource{" +
        "left=" + left +
        ", right=" + right +
        ", rightPrefix='" + rightPrefix + '\'' +
        ", condition=" + condition +
        ", joinType=" + joinType +
        '}';
  }

  @Override
  public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    DruidQuery.writeField(generator, "type", "join");
    DruidQuery.writeField(generator, "left", new QueryDataSource(left));
    DruidQuery.writeField(generator, "right", new QueryDataSource(right));
    DruidQuery.writeField(generator, "rightPrefix", rightPrefix);
    DruidQuery.writeField(generator, "condition", condition);
    DruidQuery.writeField(generator, "joinType", joinType.toString());
    generator.writeEndObject();
  }

}
