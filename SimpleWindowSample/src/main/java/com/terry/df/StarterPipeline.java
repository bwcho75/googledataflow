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

package com.terry.df;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Lists;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.avro.reflect.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
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

	@DefaultCoder(AvroCoder.class)
	static class Stat{
		Float sum;
		Float avg;
		Integer count;
		Integer key;
		Instant wStart;	// windowStartTime
		Instant wEnd;	// windowEndTime
		public Instant getwStart() {
			return wStart;
		}
		public Instant getwEnd() {
			return wEnd;
		}

		public Float getSum() {
			return sum;
		}
		public Float getAvg() {
			return avg;
		}
		public Integer getCount() {
			return count;
		}

		public Integer getKey(){
			return key;
		}

		public Stat(){}
		public Stat(Integer k,Instant start,Instant end,Integer c,Float s,Float a){
			this.key = k;
			this.count = c;
			this.sum = s;
			this.avg = a;
			this.wStart = start;
			this.wEnd = end;
		}

	}
	@DefaultCoder(AvroCoder.class)
	static class DataClass{
		public Integer getCategory() {
			return category;
		}

		public Float getPoint() {
			return point;
		}


		public String getText() {
			return text;
		}

		@Nullable Integer category;
		@Nullable Float point;
		@Nullable String text;

		public DataClass(){}

		public DataClass(Integer c,Float p,String t){
			this.category = c;
			this.point = p;
			this.text = t;
		}


	}

	static class StatDoFn extends DoFn <KV<Integer,Iterable<DataClass>>, KV<Integer,Stat> > 
	implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess
	{
		@Override
		public void processElement(ProcessContext c)  {
			Integer key = c.element().getKey();
			List<DataClass> dtList = Lists.newArrayList(c.element().getValue());

			float sum = 0;
			int cnt = 0;
			for(DataClass item : dtList){
				cnt++;
				sum+= item.getPoint();
			}
			float avg = 0;
			if(cnt!=0) avg = sum / cnt;

			LOG.info("key:"+key+" count:"+cnt+" sum:"+sum+" avg:"+avg);
			IntervalWindow w = (IntervalWindow) c.window();
			Instant s = w.start();
			Instant e = w.end();

			Stat st = new Stat(key,s,e,new Integer(cnt),new Float(sum),new Float(avg));
			KV<Integer, Stat> outputValue = KV.of(key, st);
			c.output(outputValue);

		}// processElement
	}

	static class PrintResultDoFn extends DoFn<KV<Integer,Stat>,Void> 
	implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess

	{
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			// 현재 윈도우에 대한 정보를 읽어온다. c.window()는 현재 윈도우를 BoundedWindow라는 Abstract class로 리턴한다.
			// Bounded Window는 Global Window (배치)와, IntervalWindow가 있다. 
			Integer key = c.element().getKey();
			Stat st = c.element().getValue();

			String text;
			Float sum = st.getSum();
			Float avg = st.getAvg();
			Integer count = st.getCount();
			IntervalWindow w = (IntervalWindow) c.window();
			Instant s = st.getwStart();
			Instant e = st.getwEnd();
			org.joda.time.format.DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy,MM,dd HH:mm:ss");
			LOG.info("Key:"+key
					+"("+s.toString(dtf)+" ~ "+e.toString(dtf)+")" 
					+" Count:"+count+" Sum:"+sum+" Avg:"+avg);

		}

	}
	public static void main(String[] args) {
		Pipeline p = Pipeline.create(
				PipelineOptionsFactory.fromArgs(args).withValidation().create());

		// 1.  여러 필드를 가지 데이타 포맷 정의하기
		// 2. 그 필드에서, 윈도우로 계산하기 

		p.apply(Create.of(
				"1,0.1,Hello", 
				"2,0.9,World",
				"2,0.2,World",
				"2,0.2,World",
				"2,-0.9,World",
				"1,-0.1,World",
				"1,0.3,World",
				"1,-0.1,World",
				"1,0.7,World"
				))
		.apply(ParDo.of(new DoFn<String, KV<Integer, DataClass >>() {
			@Override
			public void processElement(ProcessContext c) {
				String str = c.element();
				StringTokenizer st = new StringTokenizer(str,",");

				String str_category = st.nextToken();
				String str_point = st.nextToken();
				String text = st.nextToken();

				Integer category = Integer.parseInt(str_category);
				Float point = Float.parseFloat(str_point);

				DataClass dt = new DataClass(category,point,text);
				KV<Integer,DataClass> outputValue = KV.of(category, dt);
				c.output(outputValue);
			}
		}))
		.apply(Window.<KV<Integer, DataClass >>into(FixedWindows.of(Duration.standardSeconds(5))))
		.apply(ParDo.named("Print for debug").of( new DoFn<KV<Integer,DataClass>,KV<Integer,DataClass> >(){

			@Override
			public void processElement(ProcessContext c)  {
				Integer key = c.element().getKey();
				DataClass value = c.element().getValue();

				LOG.info("key :"+key+"point:"+value.getPoint()+" text:"+value.getText());

				c.output(c.element());
			}

		}))
		.apply(GroupByKey.<Integer,DataClass>create())		
		.apply(ParDo.named("Caculate Stat").of(new StatDoFn()))
		.apply(ParDo.named("Print Result").of(new PrintResultDoFn())
				);

		p.run();
	}
}
