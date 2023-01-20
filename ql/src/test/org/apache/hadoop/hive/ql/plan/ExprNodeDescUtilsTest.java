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
package org.apache.hadoop.hive.ql.plan;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ExprNodeDescUtilsTest {
  private ExprNodeGenericFuncDesc equalString1, equalString2, equalInt1, greaterThan1;

  @Before
  public void setup() {
    ExprNodeDesc columnText = column(TypeInfoFactory.stringTypeInfo, "text");
    ExprNodeConstantDesc constantFoo = constant(columnText.getTypeInfo(), "foo");

    ExprNodeDesc columnText2 = column(TypeInfoFactory.stringTypeInfo, "text2");
    ExprNodeConstantDesc constantBar = constant(columnText2.getTypeInfo(), "bar");

    ExprNodeDesc columnInt = column(TypeInfoFactory.intTypeInfo, "int");
    ExprNodeConstantDesc constant5 = constant(columnInt.getTypeInfo(), 5);

    ExprNodeDesc columnNumber = column(TypeInfoFactory.intTypeInfo, "number");

    // (text = "foo")
    equalString1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Arrays.asList(columnText, constantFoo));

    // (text2 = "bar")
    equalString2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Arrays.asList(columnText2, constantBar));

    // (int = 5)
    equalInt1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Arrays.asList(columnInt, constant5));

    // (number > 5)
    greaterThan1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(columnNumber, constant5));

  }

  @Test
  public void testAndFlattening() {
    // (((text = 'foo') and (text2 = 'bar')) and (int = 5))
    ExprNodeDesc expr = and(Arrays.asList(and(Arrays.asList(equalString1, equalString2)), equalInt1));
    ExprNodeDesc flattened = and(ExprNodeDescUtils.flattenAnd(expr));

    assertEquals("((text = 'foo') and (text2 = 'bar') and (int = 5))", flattened.getExprString());
  }

  @Test
  public void testToDnf() throws UDFArgumentException {
    // (((text = 'foo') or (int = 5)) and ((text2 = 'bar') or (number > 5)))
    ExprNodeDesc expr = and(Arrays.asList(
        or(Arrays.asList(equalString1, equalInt1)),
        or(Arrays.asList(equalString2, greaterThan1))));
    ExprNodeDesc dnf = ExprNodeDescUtils.toDnf(expr);
    String expectedDnf = "("
        + "((text = 'foo') and (text2 = 'bar'))"
        + " or ((text = 'foo') and (number > 5))"
        + " or ((int = 5) and (text2 = 'bar'))"
        + " or ((int = 5) and (number > 5))"
        + ")";

    assertEquals(expectedDnf, dnf.getExprString());
  }

  @Test
  public void testToDnfWithNot() throws UDFArgumentException {
    // (not ((text = 'foo') or ((text2 = 'bar') and (number > 5))))
    ExprNodeDesc expr = not(
        or(Arrays.asList(
            equalString1,
            and(Arrays.asList(
                equalString2,
                greaterThan1)))));
    ExprNodeDesc dnf = ExprNodeDescUtils.toDnf(expr);
    String expectedDnf = "("
        + "((not (text = 'foo')) and (not (text2 = 'bar')))"
        + " or ((not (text = 'foo')) and (not (number > 5)))"
        + ")";
    assertEquals(expectedDnf, dnf.getExprString());

  }

  @Test
  public void testApplyNegatives() throws UDFArgumentException {
    // (not ((text = 'foo') or ((text2 = 'bar') and (number > 5))))
    ExprNodeDesc expr = not(
        or(Arrays.asList(
            equalString1,
            and(Arrays.asList(
                equalString2,
                greaterThan1)))));
    ExprNodeDesc dnf = ExprNodeDescUtils.toDnf(expr);
    String expectedDnf = "("
        + "((not (text = 'foo')) and (not (text2 = 'bar')))"
        + " or ((not (text = 'foo')) and (not (number > 5)))"
        + ")";
    assertEquals(expectedDnf, dnf.getExprString());

    ExprNodeDesc neg = ExprNodeDescUtils.applyNegatives(dnf.clone());
    String expectedNegAppliedDnf = "("
        + "((text <> 'foo') and (text2 <> 'bar'))"
        + " or ((text <> 'foo') and (number <= 5))"
        + ")";
    assertEquals(expectedNegAppliedDnf, neg.getExprString());
  }

  private static ExprNodeGenericFuncDesc or(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPOr(), children);
  }

  private static ExprNodeGenericFuncDesc and(List<ExprNodeDesc> children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(), children);
  }

  private static ExprNodeGenericFuncDesc not(ExprNodeDesc children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPNot(), Arrays.asList(children));
  }

  private static ExprNodeColumnDesc column(TypeInfo type, String columnName) {
    return new ExprNodeColumnDesc(type, columnName, "", false);
  }

  private static ExprNodeConstantDesc constant (TypeInfo type, Object value) {
    return new ExprNodeConstantDesc(type, value);
  }

}