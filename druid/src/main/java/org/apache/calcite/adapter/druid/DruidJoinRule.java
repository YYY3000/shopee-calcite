package org.apache.calcite.adapter.druid;

import com.google.common.base.Joiner;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.druid.datasource.JoinDataSource;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to push a {@link org.apache.calcite.rel.core.Join}
 * into a {@link DruidQuery}.
 */
public class DruidJoinRule extends RelRule<DruidJoinRule.DruidJoinRuleConfig> {

  /**
   * Creates a DruidJoinRule.
   */
  protected DruidJoinRule(DruidJoinRuleConfig config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    System.out.println("Druid Join On Match");
    final Join join = call.rel(0);
    final DruidQuery left = call.rel(1);
    final DruidQuery right = call.rel(2);

    if (!left.getDruidSchema().equals(right.getDruidSchema())) {
      return;
    }

    RexNode condition = join.getCondition();
    JoinRelType joinRelType = join.getJoinType();

    if (joinRelType != JoinRelType.LEFT && joinRelType != JoinRelType.INNER) {
      return;
    }

    JoinDataSource joinDataSource = JoinDataSource.create(left, right, getConditionExpression(condition, left, right), joinRelType);

    final TableScan scan = LogicalTableScan.create(join.getCluster(), left.getTable(), ImmutableList.of());
    DruidQuery query = DruidQuery.create(join.getCluster(), join.getTraitSet(),
        left.getTable(), joinDataSource, left.intervals,
        ImmutableList.of(scan));
    System.out.println(query.getQuerySpec().getQueryString());
//    call.transformTo(query);
  }

  private String getConditionExpression(RexNode condition, DruidQuery left, DruidQuery right) {
    List<String> expressions = new ArrayList<>();

    List<RexNode> conds = RelOptUtil.conjunctions(condition);
    for (RexNode e : conds) {
      RexCall cal = (RexCall) e;
      DruidSqlOperatorConverter conversion =
          DruidQuery.getDefaultConverterOperatorMap().get(cal.getOperator());
      if (!(conversion instanceof DruidJoinSqlOperatorConverter)) {
        continue;
      }

      String expression =
          ((DruidJoinSqlOperatorConverter) conversion).toDruidJoinConditionExpression(e,
              left.getRowType(), right.getRowType());
      expressions.add(expression);
    }

    return Joiner.on(" AND ").join(expressions);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable(singleton = false)
  public interface DruidJoinRuleConfig extends RelRule.Config {
    DruidJoinRuleConfig DEFAULT = ImmutableDruidJoinRuleConfig.builder()
        .withOperandSupplier(b0 -> b0.operand(Join.class).inputs(
            b1 -> b1.operand(DruidQuery.class).noInputs(),
            b2 -> b2.operand(DruidQuery.class).noInputs()))
        .build();

    @Override
    default DruidJoinRule toRule() {
      return new DruidJoinRule(this);
    }
  }

}
