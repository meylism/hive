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

import com.meylism.sparser.support.PredicateSupport;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;

import java.util.HashMap;
import java.util.Map;

public class SparserPredicateRegistry {
  private Map<String, PredicateSupport> registry;

  public SparserPredicateRegistry() {
    this.registry = new HashMap<>();
    initRegistry();
  }

  private void registerPredicate(String hiveUdf, PredicateSupport sparserPredicate) {
    this.registry.put(hiveUdf, sparserPredicate);
  }

  public PredicateSupport getPredicateFor(GenericUDF udf) {
    if (udf == null) return null;

    // if the predicate is equality predicate, Sparser can only filter if the comparison is done between ints and
    // strings
    if (udf instanceof GenericUDFOPEqual) {
      switch(((GenericUDFOPEqual) udf).getCompareType()){
      case COMPARE_INT:
      case COMPARE_STRING:
      case COMPARE_TEXT:
        return registry.get(udf.getUdfName());
      }
    }

    return registry.get(udf.getUdfName());
  }

  private void initRegistry() {
    registerPredicate(GenericUDFOPEqual.class.getName(), PredicateSupport.EXACT_STRING_MATCH);
  }




}
