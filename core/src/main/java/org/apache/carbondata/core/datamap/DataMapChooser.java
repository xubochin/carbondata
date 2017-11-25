/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.datamap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.core.datamap.dev.expr.AndDataMapExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapperImpl;
import org.apache.carbondata.core.datamap.dev.expr.OrDataMapExprWrapper;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.TrueConditionalResolverImpl;

public class DataMapChooser {

  public DataMapExprWrapper choose(CarbonTable carbonTable,
      FilterResolverIntf resolverIntf) {
    if (resolverIntf != null) {
      Expression expression = resolverIntf.getFilterExpression();
      // First check for FG datamaps if any exist
      List<TableDataMap> allDataMapFG =
          DataMapStoreManager.getInstance().getAllDataMap(carbonTable, DataMapType.FG);
      ExpressionTuple tuple = selectDataMap(expression, allDataMapFG);
      if (tuple.dataMapExprWrapper == null) {
        // Check for CG datamap
        List<TableDataMap> allDataMapCG =
            DataMapStoreManager.getInstance().getAllDataMap(carbonTable, DataMapType.CG);
        tuple = selectDataMap(expression, allDataMapCG);
      }
      if (tuple.dataMapExprWrapper != null) {
        return tuple.dataMapExprWrapper;
      }
    }
    // Return the default datamap if no other datamap exists.
    return new DataMapExprWrapperImpl(DataMapStoreManager.getInstance()
        .getDefaultDataMap(carbonTable.getAbsoluteTableIdentifier()), resolverIntf);
  }

  private ExpressionTuple selectDataMap(Expression expression, List<TableDataMap> allDataMap) {
    switch (expression.getFilterExpressionType()) {
      case AND:
        if (expression instanceof AndExpression) {
          AndExpression andExpression = (AndExpression) expression;
          ExpressionTuple left = selectDataMap(andExpression.getLeft(), allDataMap);
          ExpressionTuple right = selectDataMap(andExpression.getRight(), allDataMap);
          Set<ExpressionType> filterExpressionTypes = new HashSet<>();
          if (left.dataMapExprWrapper != null && right.dataMapExprWrapper != null) {
            filterExpressionTypes.add(
                left.dataMapExprWrapper.getFilterResolverIntf().getFilterExpression()
                    .getFilterExpressionType());
            filterExpressionTypes.add(
                right.dataMapExprWrapper.getFilterResolverIntf().getFilterExpression()
                    .getFilterExpressionType());
            List<ColumnExpression> columnExpressions = new ArrayList<>();
            columnExpressions.addAll(left.columnExpressions);
            columnExpressions.addAll(right.columnExpressions);
            TableDataMap dataMap =
                chooseDataMap(allDataMap, columnExpressions, filterExpressionTypes);
            if (dataMap != null) {
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.dataMapExprWrapper = new DataMapExprWrapperImpl(dataMap,
                  new TrueConditionalResolverImpl(expression, false, false, null));
              return tuple;
            } else {
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.dataMapExprWrapper =
                  new AndDataMapExprWrapper(left.dataMapExprWrapper, right.dataMapExprWrapper,
                      new TrueConditionalResolverImpl(expression, false, false, null));
              return tuple;
            }
          } else if (left.dataMapExprWrapper != null && right.dataMapExprWrapper == null) {
            return left;
          } else if (left.dataMapExprWrapper == null && right.dataMapExprWrapper != null) {
            return right;
          } else {
            return left;
          }
        }
        break;
      case OR:
        if (expression instanceof OrExpression) {
          OrExpression orExpression = (OrExpression) expression;
          ExpressionTuple left = selectDataMap(orExpression.getLeft(), allDataMap);
          ExpressionTuple right = selectDataMap(orExpression.getRight(), allDataMap);
          Set<ExpressionType> filterExpressionTypes = new HashSet<>();
          if (left.dataMapExprWrapper != null && right.dataMapExprWrapper != null) {
            filterExpressionTypes.add(
                left.dataMapExprWrapper.getFilterResolverIntf().getFilterExpression()
                    .getFilterExpressionType());
            filterExpressionTypes.add(
                right.dataMapExprWrapper.getFilterResolverIntf().getFilterExpression()
                    .getFilterExpressionType());
            List<ColumnExpression> columnExpressions = new ArrayList<>();
            columnExpressions.addAll(left.columnExpressions);
            columnExpressions.addAll(right.columnExpressions);
            TableDataMap dataMap =
                chooseDataMap(allDataMap, columnExpressions, filterExpressionTypes);
            if (dataMap != null) {
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.dataMapExprWrapper = new DataMapExprWrapperImpl(dataMap,
                  new TrueConditionalResolverImpl(expression, false, false, null));
              return tuple;
            } else {
              ExpressionTuple tuple = new ExpressionTuple();
              tuple.columnExpressions = columnExpressions;
              tuple.dataMapExprWrapper =
                  new OrDataMapExprWrapper(left.dataMapExprWrapper, right.dataMapExprWrapper,
                      new TrueConditionalResolverImpl(expression, false, false, null));
              return tuple;
            }
          } else {
            left.dataMapExprWrapper = null;
            return left;
          }
        }
        break;
      default:
        ExpressionTuple tuple = new ExpressionTuple();
        extractColumnExpression(expression, tuple.columnExpressions);
        Set<ExpressionType> filterExpressionTypes = new HashSet<>();
        filterExpressionTypes.add(expression.getFilterExpressionType());
        TableDataMap dataMap =
            chooseDataMap(allDataMap, tuple.columnExpressions, filterExpressionTypes);
        if (dataMap != null) {
          tuple.dataMapExprWrapper = new DataMapExprWrapperImpl(dataMap,
              new TrueConditionalResolverImpl(expression, false, false, null));
        }
        return tuple;
    }
    return new ExpressionTuple();
  }

  private void extractColumnExpression(Expression expression,
      List<ColumnExpression> columnExpressions) {
    if (expression instanceof ColumnExpression) {
      columnExpressions.add((ColumnExpression) expression);
    } else if (expression != null) {
      List<Expression> children = expression.getChildren();
      if (children != null && children.size() > 0) {
        for (Expression exp : children) {
          extractColumnExpression(exp, columnExpressions);
        }
      }
    }
  }

  private TableDataMap chooseDataMap(List<TableDataMap> allDataMap,
      List<ColumnExpression> columnExpressions, Set<ExpressionType> expressionTypes) {
    List<DataMapTuple> tuples = new ArrayList<>();
    for (TableDataMap dataMap : allDataMap) {
      if (contains(dataMap.getDataMapFactory().getMeta(), columnExpressions, expressionTypes)) {
        tuples.add(
            new DataMapTuple(dataMap.getDataMapFactory().getMeta().getIndexedColumns().size(),
                dataMap));
      }
    }
    if (tuples.size() > 0) {
      Collections.sort(tuples);
      return tuples.get(0).dataMap;
    }
    return null;
  }

  private boolean contains(DataMapMeta mapMeta, List<ColumnExpression> columnExpressions,
      Set<ExpressionType> expressionTypes) {
    if (mapMeta.getIndexedColumns().size() == 0 || columnExpressions.size() == 0) {
      return false;
    }
    boolean contains = true;
    for (ColumnExpression expression : columnExpressions) {
      if (!mapMeta.getIndexedColumns().contains(expression.getColumnName()) || !mapMeta
          .getOptimizedOperation().containsAll(expressionTypes)) {
        contains = false;
        break;
      }
    }
    return contains;
  }

  private static class ExpressionTuple {

    DataMapExprWrapper dataMapExprWrapper;

    List<ColumnExpression> columnExpressions = new ArrayList<>();

  }

  private static class DataMapTuple implements Comparable<DataMapTuple> {

    int order;

    TableDataMap dataMap;

    public DataMapTuple(int order, TableDataMap dataMap) {
      this.order = order;
      this.dataMap = dataMap;
    }

    @Override public int compareTo(DataMapTuple o) {
      return order - o.order;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataMapTuple that = (DataMapTuple) o;

      if (order != that.order) return false;
      return dataMap != null ? dataMap.equals(that.dataMap) : that.dataMap == null;
    }

    @Override public int hashCode() {
      int result = order;
      result = 31 * result + (dataMap != null ? dataMap.hashCode() : 0);
      return result;
    }
  }

}
