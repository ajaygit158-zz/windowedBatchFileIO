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

import org.apache.apex.api.operator.ControlTuple;
import org.apache.apex.malhar.lib.batch.FileControlTuple;
import org.apache.apex.malhar.lib.window.WatermarkTuple;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

public class BatchedWindowedOperatorImpl<InputT, AccumT, OutputT> extends WindowedOperatorImpl<InputT, AccumT, OutputT>
{
  private static final Logger LOG = LoggerFactory.getLogger(BatchedWindowedOperatorImpl.class);

  private boolean receivedEndBatchTuple = false;
  @Override
  public boolean processControlTuple(ControlTuple controlTuple)
  {
    LOG.info("Received a control tuple");
    if (controlTuple instanceof WatermarkTuple) {
      LOG.info("Received Watermark Tuple");
      processWatermark((WatermarkTuple)controlTuple);
    } else if (controlTuple instanceof FileControlTuple.EndFileControlTuple) {
      receivedEndBatchTuple = true;
      LOG.info("Received end batch control tuple");
    } else if (controlTuple instanceof FileControlTuple.StartFileControlTuple) {
      LOG.info("Received start batch control tuple");
    }
    return false;
  }

  public void resetForNewBatch()
  {
    LOG.warn("Resetting for new batch");
    this.currentWatermark = -1;
    this.nextWatermark = -1;
    //Need to decide how will we purge daata storage
    //this.dataStorage.

  }

  @Override
  protected void processWatermarkAtEndWindow()
  {
    LOG.warn("Processing Watermark at end Window");
    super.processWatermarkAtEndWindow();
    if (receivedEndBatchTuple) {
      resetForNewBatch();
      receivedEndBatchTuple = false;
    }
  }
}
