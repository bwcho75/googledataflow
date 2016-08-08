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

package com.terry.df.bq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
public class StarterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
	private static final String TWITTER_STATICS_TABLE =
			"useful-hour-138023:twitter.hillary";

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(
				PipelineOptionsFactory.fromArgs(args).withValidation().create());

		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("category").setType("STRING"));
		fields.add(new TableFieldSchema().setName("sum").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("avg").setType("FLOAT"));
		TableSchema schema = new TableSchema().setFields(fields);

		p.apply(Create.of("2016-08-01 14:18:00,1,-9.3,3,0.3"
				, "2016-08-01 14:18:00,2,-2.3,1,0.3"))
		.apply(ParDo.of(new DoFn<String, TableRow>() {
			@Override
			public void processElement(ProcessContext c) {
				// Parse
				String str = c.element();
				StringTokenizer st = new StringTokenizer(str,",");

				String str_date = st.nextToken();
				String str_key = st.nextToken();
				String str_sum = st.nextToken();
				String str_count = st.nextToken();
				String str_avg = st.nextToken();

				String key="";
				if(str_key.equals("1")) key = "Hillary";
				else key = "Trump";

				TableRow row = new TableRow()
						.set("date",str_date)	// time stamp expression https://cloud.google.com/bigquery/data-types
						.set("category",key)
						.set("sum",Float.parseFloat(str_sum))
						.set("count",Integer.parseInt(str_count))
						.set("avg",Float.parseFloat(str_avg));

				c.output(row);
			}
		})
				).apply(BigQueryIO.Write
						.named("Write")
						.to(TWITTER_STATICS_TABLE)
						.withSchema(schema)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


		p.run();
	}
}
