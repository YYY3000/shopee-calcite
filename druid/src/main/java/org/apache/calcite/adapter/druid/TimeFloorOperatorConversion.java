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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.TimeZone;

/**
 * DruidSqlOperatorConverter implementation that handles Floor operations
 * conversions.
 */
public class TimeFloorOperatorConversion implements DruidSqlOperatorConverter {
  @Override
  public SqlOperator calciteOperator() {
    return SqlStdOperatorTable.TIME_FLOOR;
  }

  @Override
  public @Nullable String toDruidExpression(RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {
    final RexCall call = (RexCall) rexNode;
    final RexNode arg = call.getOperands().get(0);
    final String druidExpression = DruidExpressions.toDruidExpression(
        arg,
        rowType,
        druidQuery);
    if (druidExpression == null) {
      return null;
    } else if (call.getOperands().size() == 3) {
      // TIME_FLOOR(expr TO timeUnit, timeZone)
      final TimeZone tz = TimeZone.getTimeZone(RexLiteral.stringValue(call.getOperands().get(2)));
      final Granularity granularity = DruidDateTimeUtils
          .extractGranularity(call, tz.getID());
      if (granularity == null) {
        return null;
      }
      String isoPeriodFormat = DruidDateTimeUtils.toISOPeriodFormat(granularity.getType());
      if (isoPeriodFormat == null) {
        return null;
      }
      return DruidExpressions.applyTimestampFloor(
          druidExpression,
          isoPeriodFormat,
          "",
          tz);
    } else {
      return null;
    }
  }
}
