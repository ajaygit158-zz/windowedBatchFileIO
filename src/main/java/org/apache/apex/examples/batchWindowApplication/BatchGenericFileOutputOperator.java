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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;
import org.apache.apex.malhar.lib.batch.EndApplicationControlTuple;
import org.apache.apex.malhar.lib.batch.FileControlTuple.EndFileControlTuple;
import org.apache.apex.malhar.lib.batch.FileControlTuple.StartFileControlTuple;
import org.apache.apex.malhar.lib.window.Tuple;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class BatchGenericFileOutputOperator extends AbstractFileOutputOperator<Tuple.WindowedTuple<Double>>
{

  private transient String defaultFileName = "default";
  private String currentFileName;
  private String tupleSeparator = "\n";

  public void setTupleSeparator(String tupleSeparator)
  {
    this.tupleSeparator = tupleSeparator;
  }

  public String getTupleSeparator()
  {
    return tupleSeparator;
  }

  public void setCurrentFileName(String currentFileName)
  {
    this.currentFileName = currentFileName;
  }

  public String getCurrentFileName()
  {
    return currentFileName;
  }

  private static final Logger LOG = LoggerFactory.getLogger(BatchGenericFileOutputOperator.class);

  @Override
  public boolean handleControlTuple(ControlTuple controlTuple)
  {
    if (controlTuple instanceof StartFileControlTuple) {
      StartFileControlTuple startFileControlTuple = (StartFileControlTuple)controlTuple;
      LOG.info("Received startFile control tuple {}", startFileControlTuple.getFilename());
      int index = startFileControlTuple.getFilename().lastIndexOf("/");
      currentFileName = startFileControlTuple.getFilename().substring(index + 1);
      LOG.info("Start CFN {}", currentFileName);
    } else if (controlTuple instanceof EndFileControlTuple) {
      EndFileControlTuple endFileControlTuple = (EndFileControlTuple)controlTuple;
      LOG.info("Received endFile control tuple {}", endFileControlTuple.getFilename());
      int index = endFileControlTuple.getFilename().lastIndexOf("/");
      String endFileName = endFileControlTuple.getFilename().substring(index + 1);
      LOG.info("End CFN {}", endFileName);
      if (!currentFileName.equals(endFileName)) {
        LOG.error("Did not receive End File CT same as startFile CT : " + endFileControlTuple.getFilename());
      }
      try {
        LOG.info("Finalize CFN {} {}", currentFileName, endFileName);
        finalizeFile(currentFileName);
      } catch (IOException e) {
        LOG.error("Error while finalizing file", e);
      }
    } else if (controlTuple instanceof EndApplicationControlTuple) {
      LOG.info("Received end appl control tuple");
      //throw new ShutdownException();
    }
    return false;
  }

  @Override
  protected String getFileName(Tuple.WindowedTuple<Double> tuple)
  {
    LOG.info("Current file name : {} {}", currentFileName, defaultFileName);
    if (currentFileName == null) {
      return defaultFileName;
    }
    return currentFileName;
  }

  @Override
  protected byte[] getBytesForTuple(Tuple.WindowedTuple<Double> tuple)
  {
    return (tuple.getValue().toString() + tupleSeparator).getBytes();
  }
}
