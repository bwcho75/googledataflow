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

package com.terry.dataflow;

import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.ParDo.Bound;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.terry.nl.NLAnalyze;
import com.terry.nl.NLAnalyzeVO;

import com.google.cloud.dataflow.sdk.values.PCollection;

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
public class TwitterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(TwitterPipeline.class);
	private static final String NOWN_TABLE=
			"useful-hour-138023:twitter.noun";
	private static final String ADJ_TABLE=
			"useful-hour-138023:twitter.adj";

	// Read Twitter feed as a JSON format
	// extract twitt feed string and pass into next pipeline
	static class ParseTwitterFeedDoFn extends DoFn<String,String>{

		private static final long serialVersionUID = 3644510088969272245L;

		@Override
		public void processElement(ProcessContext c){
			String text = null;
			String lang = null;
			try {
				JsonReader reader = Json.createReader(new StringReader(c.element()));
				JsonObject json = reader.readObject();
				text = (String) json.getString("text");
				lang = (String) json.getString("lang");

				if(lang.equals("en")){
					c.output(text.toLowerCase());
				}

			} catch (Exception e) {
				LOG.debug("No text element");
				LOG.debug("original message is :" + c.element());
			}		  
		}
	}

	// Parse Twitter string into 
	// - list of nouns
	// - list of adj
	// - list of emoticon

	static class NLAnalyticsDoFn extends DoFn<String,KV<String,Iterable<String>>>{		/**
	 * 
	 */
		private static final long serialVersionUID = 3013780586389810713L;

		// return list of NOUN,ADJ,Emoticon
		@Override
		public void processElement(ProcessContext c) throws IOException, GeneralSecurityException{
			String text = (String)c.element();

			NLAnalyze nl = NLAnalyze.getInstance();
			NLAnalyzeVO vo = nl.analyze(text);

			List<String> nouns = vo.getNouns();
			List<String> adjs = vo.getAdjs();

			KV<String,Iterable<String>> kv_noun=  KV.of("NOUN", (Iterable<String>)nouns);
			KV<String,Iterable<String>> kv_adj =  KV.of("ADJ", (Iterable<String>)adjs);

			c.output(kv_noun);
			c.output(kv_adj);
		}

	}


	static class NounFilter extends DoFn<KV<String,Iterable<String>>,String>{
		@Override
		public void processElement(ProcessContext c) {
			String key = c.element().getKey();
			if(!key.equals("NOUN")) return;
			List<String> values = (List<String>) c.element().getValue();
			for(String value:values){
				// Filtering #
				if(value.equals("#")) continue;
				else if(value.startsWith("http")) continue;
				c.output(value);
			}
	
		}
	}

	static class AddTimeStampNoun extends DoFn<KV<String,Long>,TableRow>
	implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess
	{
		@Override
		public void processElement(ProcessContext c) {
			String key = c.element().getKey();	// get Word
			Long value = c.element().getValue();// get count of the word
			IntervalWindow w = (IntervalWindow) c.window();
			Instant s = w.start();
			DateTime sTime = s.toDateTime(org.joda.time.DateTimeZone.forID("Asia/Seoul"));
			DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
			String str_stime = sTime.toString(dtf);

			TableRow row =  new TableRow()
					.set("date", str_stime)
					.set("noun", key)
					.set("count", value);

			c.output(row);
		}

	}

	static class AddTimeStampAdj extends DoFn<KV<String,Long>,TableRow>
	implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess
	{
		@Override
		public void processElement(ProcessContext c) {
			String key = c.element().getKey();	// get Word
			Long value = c.element().getValue();// get count of the word
			IntervalWindow w = (IntervalWindow) c.window();
			Instant s = w.start();
			DateTime sTime = s.toDateTime(org.joda.time.DateTimeZone.forID("Asia/Seoul"));
			DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
			String str_stime = sTime.toString(dtf);

			TableRow row =  new TableRow()
					.set("date", str_stime)
					.set("adj", key)
					.set("count", value);

			c.output(row);
		}

	}
	static class AdjFilter extends DoFn<KV<String,Iterable<String>>,String>{
		@Override
		public void processElement(ProcessContext c) {
			String key = c.element().getKey();
			if(!key.equals("ADJ")) return;
			List<String> values = (List<String>) c.element().getValue();
			for(String value:values){
				c.output(value);
			}
		}
	}

	static class Echo extends DoFn<KV<String,Iterable<String>>,Void>{
		@Override
		public void processElement(ProcessContext c) {
			String key = c.element().getKey();
			List<String> values = (List<String>) c.element().getValue();
			for(String value:values){
			}
		}

	}
	public static void main(String[] args) {
		Pipeline p = Pipeline.create(
				PipelineOptionsFactory.fromArgs(args).withValidation().create());

		@SuppressWarnings("unchecked")
		PCollection <KV<String,Iterable<String>>> nlprocessed 
		=  (PCollection<KV<String,Iterable<String>>>) p.apply(PubsubIO.Read.named("ReadFromPubSub").topic("projects/useful-hour-138023/topics/twitter"))
		.apply(ParDo.named("Parse Twitter").of(new ParseTwitterFeedDoFn()))
		.apply(ParDo.named("NL Processing").of(new NLAnalyticsDoFn()));


		// Noun handling sub-pipeline
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("noun").setType("STRING"));
		fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);

		nlprocessed.apply(ParDo.named("NounFilter").of(new NounFilter()))
		.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))))
		.apply(Count.<String>perElement())
		.apply(ParDo.named("Noun Formating").of(new AddTimeStampNoun()) )
		.apply(BigQueryIO.Write
				.named("Write Noun Count to BQ")
				.to( NOWN_TABLE)
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		// Adj handling sub-pipeline
		fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("adj").setType("STRING"));
		fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
		schema = new TableSchema().setFields(fields);

		nlprocessed.apply(ParDo.named("AdjFilter").of(new AdjFilter()))
		.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))))
		.apply(Count.<String>perElement())
		.apply(ParDo.named("Adj Formating").of(new AddTimeStampAdj()) )
		.apply(BigQueryIO.Write
				.named("Write Adj Count to BQ")
				.to( ADJ_TABLE)
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));




		p.run();
	}

}
