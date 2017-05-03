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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.malhar.lib.batch.EndApplicationControlTuple;
import org.apache.apex.malhar.lib.batch.FileControlTuple.EndFileControlTuple;
import org.apache.apex.malhar.lib.batch.FileControlTuple.StartFileControlTuple;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.lib.window.impl.WatermarkImpl;

public class LineByLineFileInputByteArrayOutputOperator extends LineByLineFileInputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(LineByLineFileInputOperator.class);

  public final transient ControlAwareDefaultOutputPort<byte[]> output = new ControlAwareDefaultOutputPort<byte[]>();

  @Override
  protected void emit(String tuple)
  {
    if (tuple.startsWith("WM-")) {
      String time = tuple.substring(3);
      long timestamp = Long.parseLong(time);
      output.emitControl(new WatermarkImpl(timestamp));
    } else {
      output.emit(tuple.getBytes());
    }
  }

  @Override
  protected void emitStartBatchControlTuple()
  {
    LOG.info("Emitting start batch control tuple : {} : Wid : {}", this.currentFile, this.currentWindowId);
    this.output.emitControl(new StartFileControlTuple(this.currentFile));
  }

  @Override
  protected void emitEndBatchControlTuple()
  {
    LOG.info("Emitting end batch control tuple : {}, wID : {}", this.closedFileName, this.currentWindowId);
    this.output.emitControl(new EndFileControlTuple(this.closedFileName));
  }

  @Override
  protected void handleEndOfInputData()
  {
    LOG.info("Emitting end Application control Tuple : wID : {}", this.currentWindowId);
    output.emitControl(new EndApplicationControlTuple.EndApplicationControlTupleImpl());
  }
}
