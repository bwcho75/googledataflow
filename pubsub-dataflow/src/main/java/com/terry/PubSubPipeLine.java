/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.terry;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.BlockingDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 */
public class PubSubPipeLine {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubPipeLine.class);

  public static void main(String[] args) {
	PipelineOptions option = PipelineOptionsFactory.fromArgs(args).withValidation().create();
	Pipeline p = Pipeline.create(option);
    
    p.apply(PubsubIO.Read.named("ReadFromPubSub").topic("projects/terrycho-sandbox/topics/dataflow")
    		.timestampLabel("mytimestamp")
    		.idLabel("myid"))
    .apply(ParDo.of(new DoFn<String, String>() {
      @Override
      public void processElement(ProcessContext c) {
    
        c.output(c.element().toUpperCase());
      }
    }))
    .apply(ParDo.of(new DoFn<String, Void>() {
      @Override
      public void processElement(ProcessContext c)  {
      	org.joda.time.Instant timeStamp =  c.timestamp();
        LOG.info("timestamp :"+timeStamp.toString()+":"+c.element());
      }
    }));

    p.run();
  }
}
