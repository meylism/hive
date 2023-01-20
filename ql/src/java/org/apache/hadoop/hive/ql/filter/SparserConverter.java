/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.filter;

import com.meylism.sparser.predicate.*;
import com.meylism.sparser.support.PredicateSupport;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;
import java.util.List;

public class SparserConverter {
  /**
   * Convert the tree of ExprNodeDesc to Sparser-specific format.
   * expr is expected to be in Disjunctive Normal Form(DNF).
   */
  public static List<ConjunctiveClause> convert(ExprNodeGenericFuncDesc expr) {
    ExprNodeDesc dnf = ExprNodeDescUtils.toDnfW(expr);
    return convert(expr, new ArrayList<>());
  }

  private static List<ConjunctiveClause> convert(ExprNodeGenericFuncDesc expr, List<ConjunctiveClause> conjunctiveClauses) {
    if (ExprNodeDescUtils.isOr(expr)) {
      for (ExprNodeDesc e : expr.getChildren()) {
        convert((ExprNodeGenericFuncDesc) e);
      }
    } else if (ExprNodeDescUtils.isAnd(expr)) {
      ConjunctiveClause clause = processConjunctiveClause(expr);
      if (clause == null) return null;
      conjunctiveClauses.add(clause);
    } else {
      ConjunctiveClause clause = new ConjunctiveClause();
      SimplePredicate predicate = processSimplePredicate(expr);
      clause.add(predicate);
      conjunctiveClauses.add(clause);
    }
    return conjunctiveClauses;
  }

  private static ConjunctiveClause processConjunctiveClause(ExprNodeGenericFuncDesc expr) {
    return null;
  }

  private static SimplePredicate processSimplePredicate(ExprNodeGenericFuncDesc expr) {
    SimplePredicate pred = generatePredicateFor(expr);
    return pred;
  }


  private static SimplePredicate generatePredicateFor(ExprNodeGenericFuncDesc expr) {
    GenericUDF udf = expr.getGenericUDF();
    BooleanWritable inverted = new BooleanWritable();
    PredicateSupport support = getPredicateSupportFor(udf, inverted);

    switch (support) {
    case EXACT_STRING_MATCH:
      ExactMatchPredicate emp = genExactMatchPredicate(expr);
      emp.setInverted(inverted.get());
      return emp;
    case CONTAINS_STRING:
    case CONTAINS_KEY:
    default:
      return null;
    }
  }

  private static PredicateSupport getPredicateSupportFor(GenericUDF udf, BooleanWritable inverted) {
    // Predicate: Exact string match
    // column = "foo"
    // column = 5
    if (udf instanceof GenericUDFOPEqual) {return PredicateSupport.EXACT_STRING_MATCH;}
    else if (udf instanceof GenericUDFOPNotEqual) {inverted.set(true); return PredicateSupport.EXACT_STRING_MATCH;}

    // Predicate: contains string
    // column like "%foobar%"

    return null;
  }

  private static ExactMatchPredicate genExactMatchPredicate(ExprNodeGenericFuncDesc expr) {
    PredicateKey key = genKeyForSparser(expr);
    ExprNodeDesc constantDesc = ExprNodeDescUtils.extractConstantExpr(expr);
    assert constantDesc != null;

    PredicateValue value = new PredicateValue(((ExprNodeConstantDesc) constantDesc).getValue());

    return new ExactMatchPredicate(key, value);
  }

  private static PredicateKey genKeyForSparser(ExprNodeDesc expr) {
    List<String> fields = new ArrayList<>();
    ExprNodeColumnDesc columnDesc = populateKey(expr, fields);
    String tableName = columnDesc.getTabAlias();
    String columnName = columnDesc.getColumn();
    return new PredicateKey(tableName, columnName, fields);
  }

  private static ExprNodeColumnDesc populateKey(ExprNodeDesc expr, List<String> fields) {
    if (expr instanceof ExprNodeColumnDesc) {
      return (ExprNodeColumnDesc) expr;
    } else if (expr instanceof ExprNodeFieldDesc) {
      fields.add(0, ((ExprNodeFieldDesc) expr).getFieldName());
      return populateKey(((ExprNodeFieldDesc) expr).getDesc(), fields);
    } else if (expr.getChildren() != null) {
      for (ExprNodeDesc nodeDesc : expr.getChildren()) {
        ExprNodeColumnDesc columnDesc = populateKey(nodeDesc, fields);
        if (columnDesc != null)
          return columnDesc;
      }
    }
    return null;
  }
}
