/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.batchWindowApplication;

import org.joda.time.Duration;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;

@ApplicationAnnotation(name = "BatchControlTupleExample")
/**
 * @since 3.7.0
 */
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LineByLineFileInputByteArrayOutputOperator fileInput = dag.addOperator("fileReader",
        LineByLineFileInputByteArrayOutputOperator.class);
    CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
    //PojoToWindowTimestampTupleTransformer transformer = dag.addOperator("transformer",
    //    PojoToWindowTimestampTupleTransformer.class);
    BatchedWindowedOperatorImpl<Customer, MutablePair<Double, Long>, Double> windowedOperator = new BatchedWindowedOperatorImpl<>();
    Accumulation<Customer, MutablePair<Double, Long>, Double> average = new AverageCustomerAge();

    windowedOperator.setAccumulation(average);
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<MutablePair<Double, Long>>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<Double>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.standardMinutes(10)));
    windowedOperator.setTriggerOption(TriggerOption.AtWatermark());

    dag.addOperator("batchWindowOperator", windowedOperator);

    BatchGenericFileOutputOperator fileOutput = dag.addOperator("fileOutput", BatchGenericFileOutputOperator.class);

    dag.addStream("FileReaderToParser", fileInput.output, csvParser.in);
    dag.addStream("ParserToTransformer", csvParser.out, new ControlAwareDefaultInputPort[] {windowedOperator.input});
//    dag.addStream("TransformerToWindowedOp", transformer.output, windowedOperator.input);
    dag.addStream("WindowToFileOut", windowedOperator.output, fileOutput.input);
  }
}
