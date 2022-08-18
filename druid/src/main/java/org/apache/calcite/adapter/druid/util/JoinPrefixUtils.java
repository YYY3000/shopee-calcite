package org.apache.calcite.adapter.druid.util;

import org.apache.calcite.adapter.druid.DruidTable;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;

/**
 * Utility class for working with prefixes in join operations
 */
public class JoinPrefixUtils {
  private static final Comparator<String> DESCENDING_LENGTH_STRING_COMPARATOR = (s1, s2) ->
      Integer.compare(s2.length(), s1.length());

  /**
   * Checks that "prefix" is a valid prefix for a join clause and, if so,
   * returns it. Otherwise, throws an exception.
   */
  public static String validatePrefix(@Nullable final String prefix) {
    if (prefix == null || prefix.isEmpty()) {
      throw new AssertionError("Join clause cannot have null or empty prefix");
    } else if (isPrefixedBy(DruidTable.DEFAULT_TIMESTAMP_COLUMN, prefix) || DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(prefix)) {
      throw new AssertionError(
          String.format("Join clause cannot have prefix[%s], since it would shadow %s",
              prefix,
              DruidTable.DEFAULT_TIMESTAMP_COLUMN)
      );
    } else {
      return prefix;
    }
  }

  public static boolean isPrefixedBy(final String columnName, final String prefix) {
    return columnName.length() > prefix.length() && columnName.startsWith(prefix);
  }

  /**
   * Check if any prefixes in the provided list duplicate or shadow each other.
   *
   * @param prefixes A mutable list containing the prefixes to check. This list will be sorted by
   *                 descending
   *                 string length.
   */
  public static void checkPrefixesForDuplicatesAndShadowing(
      final List<String> prefixes
  ) {
    // this is a naive approach that assumes we'll typically handle only a small number of prefixes
    prefixes.sort(DESCENDING_LENGTH_STRING_COMPARATOR);
    for (int i = 0; i < prefixes.size(); i++) {
      String prefix = prefixes.get(i);
      for (int k = i + 1; k < prefixes.size(); k++) {
        String otherPrefix = prefixes.get(k);
        if (prefix.equals(otherPrefix)) {
          throw new AssertionError(String.format("Detected duplicate prefix in join clauses: " +
              "[%s]", prefix));
        }
        if (isPrefixedBy(prefix, otherPrefix)) {
          throw new AssertionError(String.format("Detected conflicting prefixes in join clauses: " +
              "[%s, %s]", prefix, otherPrefix));
        }
      }
    }
  }
}
