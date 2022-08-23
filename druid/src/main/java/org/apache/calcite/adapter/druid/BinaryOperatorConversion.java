/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.druid;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * Binary operator conversion utility class; used to convert expressions like
 * {@code exp1 Operator exp2}.
 */
public class BinaryOperatorConversion implements DruidSqlOperatorConverter,
    DruidJoinSqlOperatorConverter {
  private final SqlOperator operator;
  private final String druidOperator;

  public BinaryOperatorConversion(final SqlOperator operator, final String druidOperator) {
    this.operator = operator;
    this.druidOperator = druidOperator;
  }

  @Override
  public SqlOperator calciteOperator() {
    return operator;
  }

  @Override
  public String toDruidExpression(RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {

    final RexCall call = (RexCall) rexNode;

    final List<String> druidExpressions = DruidExpressions.toDruidExpressions(
        druidQuery, rowType,
        call.getOperands());
    if (druidExpressions == null) {
      return null;
    }
    if (druidExpressions.size() != 2) {
      throw new IllegalStateException(
          DruidQuery.format("Got binary operator[%s] with %s args?", operator.getName(),
              druidExpressions.size()));
    }

    return DruidQuery
        .format("(%s %s %s)", druidExpressions.get(0), druidOperator, druidExpressions.get(1));
  }

  @Override
  public String toDruidJoinConditionExpression(RexCall condition, RelDataType leftRowType,
      RelDataType rightRowType, String rightPrefix) {
    // TODO: need support function
    if (condition.getKind() != SqlKind.EQUALS) {
      return "";
    }

    int leftFieldCount = leftRowType.getFieldCount();
    final List<RexNode> operands = condition.getOperands();
    if ((operands.get(0) instanceof RexInputRef) && (operands.get(1) instanceof RexInputRef)) {
      RexInputRef op0 = (RexInputRef) operands.get(0);
      RexInputRef op1 = (RexInputRef) operands.get(1);
      RexInputRef leftField;
      RexInputRef rightField;
      if (op0.getIndex() < leftFieldCount && op1.getIndex() >= leftFieldCount) {
        // Arguments were of form 'op0 = op1'
        leftField = op0;
        rightField = op1;
      } else if (op1.getIndex() < leftFieldCount && op0.getIndex() >= leftFieldCount) {
        // Arguments were of form 'op1 = op0'
        leftField = op1;
        rightField = op0;
      } else {
        return "";
      }

      String leftColumn = leftRowType.getFieldList().get(leftField.getIndex()).getName();
      String rightColumn =
          rightRowType.getFieldList().get(rightField.getIndex() - leftFieldCount).getName();
      return leftColumn + druidOperator + "\"" + rightPrefix + rightColumn + "\"";
    } else {
      return "";
    }
  }
}
