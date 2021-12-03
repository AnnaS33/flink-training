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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.training.solutions.hourlytips.HourlyTipsSolution;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // добавляем данные
        DataStream<TaxiFare> fares =
                //Устанавливаем временные и водные метки
                env.addSource(source).assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((fare,t) -> fare.getEventTimeMillis())); // В качестве меток времени устанавливаем время начала поедки

        //Группируем по id водителя
        DataStream<Tuple3<Long, Long, Float>> Tips = fares.keyBy((TaxiFare fare) -> fare.driverId)
                //разбиваем по временным окнам
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //считаем сумму чаевых для каждого водителя за каждый час
                .process(new HourlyTipsExercise.SumTips());


        DataStreamSink<Tuple3<Long, Long, Float>> MaxTips =
                //Разбиваем по тем же временным окнам
                Tips.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                        //и среди них ищем максимум по значению суммы чаевых
                        .maxBy(2).addSink(sink);//записываем в приёмник

        return env.execute("Hourly Tips");
    }

    public static class SumTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context context, Iterable<TaxiFare> fares,
                            Collector<Tuple3<Long, Long, Float>> out) {
            float sum = 0;
            for (TaxiFare fare : fares) sum += fare.tip;
            out.collect(Tuple3.of(context.window().getEnd(), key, sum));
        }
    }
}
