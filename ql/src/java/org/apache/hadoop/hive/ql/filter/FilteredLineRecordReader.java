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

import com.meylism.sparser.Sparser;
import com.meylism.sparser.predicate.ConjunctiveClause;
import com.meylism.sparser.predicate.ExactMatchPredicate;
import com.meylism.sparser.predicate.PredicateValue;
import com.meylism.sparser.support.FileFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

public class FilteredLineRecordReader extends LineRecordReader {
  private Sparser sparser = initializeSparser();
  private final Integer NUM_OF_SAMPLES = 50;
  private ArrayList<String> samples = new ArrayList<>();
  private Boolean isCalibrationDone = false;

  public FilteredLineRecordReader(Configuration job, FileSplit split) throws IOException {
    super(job, split);
  }

  public FilteredLineRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter) throws IOException {
    super(job, split, recordDelimiter);
  }

  public FilteredLineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
    super(in, offset, endOffset, maxLineLength);
  }

  public FilteredLineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength,
      byte[] recordDelimiter) {
    super(in, offset, endOffset, maxLineLength, recordDelimiter);
  }

  public FilteredLineRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
    super(in, offset, endOffset, job);
  }

  public FilteredLineRecordReader(InputStream in, long offset, long endOffset, Configuration job,
      byte[] recordDelimiter) throws IOException {
    super(in, offset, endOffset, job, recordDelimiter);
  }

  public synchronized boolean next(LongWritable key, Text value) throws IOException {
    while (super.next(key, value)) {
      // For the first NUM_OF_SAMPLES records, gather samples and don't filter
      if (samples.size() < NUM_OF_SAMPLES) {
        samples.add(value.toString());
        return true;
      } else if (!isCalibrationDone) {
        try {
          sparser.calibrate(samples);
        } catch (Exception e) {
          throw new RuntimeException("Exception while calibrating Sparser" + e.getMessage());
        } finally {
          isCalibrationDone = true;
        }
      }

      if (sparser.filter(value.toString()))
        return true;
    }
    return false;
  }

  private synchronized Sparser initializeSparser() {
    Sparser sparser = new Sparser.SparserBuilder(FileFormat.JSON).build();

    // predicate
    ConjunctiveClause cc1 = new ConjunctiveClause();
    ConjunctiveClause cc2 = new ConjunctiveClause();
    ConjunctiveClause cc3 = new ConjunctiveClause();

    ExactMatchPredicate emp1 = new ExactMatchPredicate("text", new PredicateValue("elon"));
    ExactMatchPredicate emp2 = new ExactMatchPredicate("text", new PredicateValue("musk"));
    ExactMatchPredicate emp3 = new ExactMatchPredicate("text", new PredicateValue("biden"));

    cc1.add(emp1);
    cc2.add(emp2);
    cc3.add(emp3);

    sparser.compile(Arrays.asList(cc1, cc2, cc3));
    return sparser;
  }
}