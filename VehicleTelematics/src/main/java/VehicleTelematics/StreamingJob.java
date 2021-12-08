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

package VehicleTelematics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {

//		String inFilePath = args[0];
//		String outFilePath = args[1];

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.readTextFile("/Users/AzeezA/Google Drive/Life/2021/UPM/CloudComputing/FlinkProject/sample-traffic-3xways.csv");

		DataStream<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>> streamTuple = text.map(new MapFunction<String, Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> map(String str) throws Exception {
				String[] temp = str.split(",");
				return new Tuple8<>(
						Integer.parseInt(temp[0]),
						Long.parseLong(temp[1]),
						Integer.parseInt(temp[2]),
						Integer.parseInt(temp[3]),
						Integer.parseInt(temp[4]),
						Integer.parseInt(temp[5]),
						Integer.parseInt(temp[6]),
						Integer.parseInt(temp[7])
				);
			}
		});

		KeyedStream<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> vidKeyStream = streamTuple.keyBy(1);

		//Max Speed
		SingleOutputStreamOperator<Tuple6<Integer, Long, Integer, Integer, Integer, Integer>> maxSpeed = vidKeyStream.max(2).filter(new FilterFunction<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public boolean filter(Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
				if (in.f2 > 60) {
					return true;
				} else {
					return false;
				}
			}
		}).map(new MapFunction<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Long, Integer, Integer, Integer, Integer>>() {
			@Override
			public Tuple6<Integer, Long, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
				return new Tuple6<>(
						in.f0,
						in.f1,
						in.f3,
						in.f6,
						in.f5,
						in.f2
				);
			}
		});

		maxSpeed.writeAsCsv("SpeedRadar.csv");


		SingleOutputStreamOperator<Tuple7<Integer, Integer, Long, Integer, Integer, Integer, Integer>> stoppedCars = vidKeyStream.countWindow(4,1).process(new ProcessWindowFunction<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Long, Integer, Integer, Integer, Integer>, Integer, GlobalWindow>() {
			@Override
			public void process(Integer key, Context context, Iterable<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple7<Integer, Integer, Long, Integer, Integer, Integer, Integer>> out) throws Exception {
				int count = 0;
				int last_position = -1;
				Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> first_seg;
				Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> last_seg;
				for (Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> in: input) {
					if (count == 0) {
						first_seg = in;
						last_position = in.f7;
						count++;
					} else if (last_position == in.f7) {
						last_position = in.f7;
						count++;
					}
					last_seg = in;
				}
				if (count == 4) {
					out.collect(new Tuple7<>(first_seg.f0, last_seg.f0, first_seg.f1, first_seg.f3, first_seg.f6, first_seg.f5, first_seg.f7));
				}
			}
		});
		stoppedCars.writeAsCsv("AccidentReporter.csv");

		SingleOutputStreamOperator<Tuple7<Integer, Integer, Long, Integer, Integer, Integer, Integer>> stoppedCars = vidKeyStream.countWindow(4,1).process(new MyProcessWindowFunction());



		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}

