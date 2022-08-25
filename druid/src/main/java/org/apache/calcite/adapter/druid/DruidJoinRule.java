package org.apache.calcite.adapter.druid;

import com.google.common.base.Joiner;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.druid.datasource.JoinDataSource;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
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

  private static final List<JoinRelType> allowedJoinTypes = Arrays.asList(JoinRelType.LEFT,
      JoinRelType.RIGHT, JoinRelType.FULL, JoinRelType.INNER);

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Join join = call.rel(1);
    final DruidQuery left = call.rel(2);
    final DruidQuery right = call.rel(3);

    if (!left.getDruidSchema().equals(right.getDruidSchema())) {
      return;
    }

    JoinRelType joinRelType = join.getJoinType();
    if (!allowedJoinTypes.contains(joinRelType)) {
      return;
    }

    String rightPrefix = "r.";
    RexNode condition = join.getCondition();
    String joinExpression = getConditionExpression(condition, left, right, rightPrefix);
    if ("".equals(joinExpression)) {
      return;
    }

    JoinDataSource joinDataSource = JoinDataSource.create(left, right, rightPrefix,
        joinExpression, joinRelType);

    DruidQuery query = DruidQuery.create(project.getCluster(), project.getTraitSet(),
        left.getTable(), joinDataSource, left.intervals,
        ImmutableList.of(join, project));
    call.transformTo(query);
  }

  private String getConditionExpression(RexNode condition, DruidQuery left, DruidQuery right,
      String rightPrefix) {
    List<String> expressions = new ArrayList<>();

    List<RexNode> conds = RelOptUtil.conjunctions(condition);
    for (RexNode e : conds) {
      if (!(e instanceof RexCall)) {
        continue;
      }

      RexCall cal = (RexCall) e;
      DruidSqlOperatorConverter conversion =
          DruidQuery.getDefaultConverterOperatorMap().get(cal.getOperator());
      if (!(conversion instanceof DruidJoinSqlOperatorConverter)) {
        continue;
      }

      String expression =
          ((DruidJoinSqlOperatorConverter) conversion).toDruidJoinConditionExpression(cal,
              left.getRowType(), right.getRowType(), rightPrefix);
      if (null == expression || expression.equals("")) {
        continue;
      }

      expressions.add(expression);
    }

    if (expressions.isEmpty()) {
      return "";
    }
    return Joiner.on(" AND ").join(expressions);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable(singleton = false)
  public interface DruidJoinRuleConfig extends RelRule.Config {
    DruidJoinRuleConfig DEFAULT = ImmutableDruidJoinRuleConfig.builder()
        .withOperandSupplier(b0 -> b0.operand(Project.class)
            .inputs(b1 -> b1.operand(Join.class)
                .inputs(b2 -> b2.operand(DruidQuery.class).noInputs(),
                    b3 -> b3.operand(DruidQuery.class).noInputs())))
        .build();

    @Override
    default DruidJoinRule toRule() {
      return new DruidJoinRule(this);
    }
  }

}
