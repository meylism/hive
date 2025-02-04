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

package org.apache.hadoop.hive.ql.filter.io;

import org.apache.hadoop.hive.ql.io.JsonFileStorageFormatDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FilteredInputFormatFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FilteredInputFormatFactory.class);

  private final Map<String, String> fileDescToInputFormat;

  public FilteredInputFormatFactory() {
    fileDescToInputFormat = new HashMap();
    // currently only json filtering is supported
    fileDescToInputFormat.put(JsonFileStorageFormatDescriptor.class.getName(),
        FilteredTextInputFormat.class.getName());
  }

  public String get(String fileFormat) {
    return fileDescToInputFormat.get(fileFormat);
  }

}