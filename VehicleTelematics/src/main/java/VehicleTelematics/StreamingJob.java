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
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.LinkedList;

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

		/////////////////
		//  Max Speed //
		///////////////
		SingleOutputStreamOperator<Tuple6<Integer, Long, Integer, Integer, Integer, Integer>> maxSpeed = vidKeyStream.max(2).filter(new FilterFunction<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public boolean filter(Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
				if (in.f2 > 90) {
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

		maxSpeed.writeAsCsv("Output/SpeedRadar.csv");

		///////////////////////////////
		// Average Speed Seg 52-56  //
		/////////////////////////////
		// return format: Time1, Time2, VID, XWay, Dir, AvgSpd
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		SingleOutputStreamOperator<Tuple6<Integer, Integer, Long, Integer, Integer, Double>> avgSpeed = vidKeyStream.filter(new FilterFunction<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public boolean filter(Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
				if ((in.f6 >= 52) && (in.f6 <= 56)) {
					return true;
				} else {
					return false;
				}
			}
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>>(){
			@Override
			public long extractAscendingTimestamp(Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> element) {
				return element.f0;
			}
		})
				.keyBy(1)
				.window(EventTimeSessionWindows.withGap(Time.seconds(31)))
				.process(new ProcessWindowFunction<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Long, Integer, Integer, Double>, Tuple, TimeWindow>() {
			@Override
			public void process(Tuple key, Context context, Iterable<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Double>> out) throws Exception {
				int count = 0;
				Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> first_segment = new Tuple8<>(Integer.parseInt("0"),Long.parseLong("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"));
				Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> last_segment = new Tuple8<>(Integer.parseInt("0"),Long.parseLong("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"));
				//Segment is f6 Position is f7 Direction is f5
				ArrayList<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>> journey = new ArrayList<>();
				for (Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> in: input) {
					journey.add(in);
					if (count == 0) {
						first_segment = in;
						last_segment = in;
					}
					if (in.f5 == 0) {
						if((in.f6 <= first_segment.f6) && (in.f7 < first_segment.f7)) {
							first_segment = in;
						}
						else if((in.f6 >= last_segment.f6) && (in.f7 > last_segment.f7)) {
							last_segment = in;
						}
					} else {
						if((in.f6 >= first_segment.f6) && (in.f7 > first_segment.f7)) {
							first_segment = in;
						}
						else if((in.f6 <= last_segment.f6) && (in.f7 < last_segment.f7)) {
							last_segment = in;
						}
					}
					count++;
				}
				boolean finished_segment = ((first_segment.f6 == 52) && (last_segment.f6 == 56) || (first_segment.f6 == 56) && (last_segment.f6 == 52));

				if(finished_segment) {
					int meters_driven = Math.abs((first_segment.f7 - last_segment.f7));
					double seconds_driven = count * 30.0;
					// 1 m/s is equivalent to 2.236936 mph
					double avg_speed_mph = (meters_driven / seconds_driven) * 2.236936;
					Tuple6<Integer, Integer, Long, Integer, Integer, Double> output = new Tuple6<>(first_segment.f0, last_segment.f0, first_segment.f1, first_segment.f3, first_segment.f5, avg_speed_mph);
					if (avg_speed_mph > 60) {
						out.collect(output);
					}
				}
			}
		});

		avgSpeed.writeAsCsv("Output/AverageSpeedControl.csv");


		///////////////////
		//  Stopped Car //
		/////////////////
		SingleOutputStreamOperator<Tuple7<Integer, Integer, Long, Integer, Integer, Integer, Integer>> stoppedCars = vidKeyStream.countWindow(4,1).process(new ProcessWindowFunction<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Long, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>() {
			@Override
			public void process(Tuple key, Context context, Iterable<Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple7<Integer, Integer, Long, Integer, Integer, Integer, Integer>> out) throws Exception {
				int count = 0;
				int last_position = -1;
				Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> first_segment = new Tuple8<>(Integer.parseInt("0"),Long.parseLong("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"));
				Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> last_segment = new Tuple8<>(Integer.parseInt("0"),Long.parseLong("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"),Integer.parseInt("0"));

				for (Tuple8<Integer, Long, Integer, Integer, Integer, Integer, Integer, Integer> in: input) {
					if (count == 0) {
						// For the first step of processing the window, set the starting position to 'last_position' and starting tuple to 'first_segment'
						first_segment = in;
						last_position = in.f7;
						count++;
					} else if (last_position == in.f7) {
						// As you iterate through the window, check that the current tuple's position is the same as the 'last_position'
						// If this is the case, then the car has not moved
						last_position = in.f7;
						count++; //increment the counter keeping track of how long the same position has been maintaned
					}
					last_segment = in;
				}
				if (count == 4) { //if the counter reaches 4, then the car has been in the same postion for 4 time steps, triggering a recorded event
					out.collect(new Tuple7<>(first_segment.f0, last_segment.f0, first_segment.f1, first_segment.f3, first_segment.f6, first_segment.f5, first_segment.f7));
				}
			}
		});
		stoppedCars.writeAsCsv("Output/AccidentReporter.csv");






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
