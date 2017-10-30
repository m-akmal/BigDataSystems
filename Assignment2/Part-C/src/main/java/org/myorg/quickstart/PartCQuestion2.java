package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

class TweetWaterMarkAssignerWithDelay implements org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<Tuple5<Integer, Integer, Integer, Long, String>> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple5<Integer, Integer, Integer, Long, String> tuple, long l) {
        return new Watermark(l - 500 * 1000);
    }

    @Override
    public long extractTimestamp(Tuple5<Integer, Integer, Integer, Long, String> tuple5, long l) {
        //timestamp in file is in seconds, flink expects it in milliseconds
        return tuple5.f3 * 1000;
    }
}


public class PartCQuestion2 {

    public static void main(String[] args) throws Exception {

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple4<Integer, Integer, Long, String>> text = env.addSource(StreamingEmulator.create("/home/pavan/bigdata/flink/higgs-activity_time_late_arrive.txt"));

        DataStream<TweetTimeWindow> answer = text
                .flatMap(new FlatMapFunction<Tuple4<Integer, Integer, Long, String>, Tuple5<Integer, Integer, Integer, Long, String>>() {
                    @Override
                    public void flatMap(Tuple4<Integer, Integer, Long, String> tuple4, Collector<Tuple5<Integer, Integer, Integer, Long, String>> collector) throws Exception {
                        collector.collect(new Tuple5(1, tuple4.f0, tuple4.f1, tuple4.f2, tuple4.f3));
                    }
                })
                .assignTimestampsAndWatermarks(new TweetWaterMarkAssignerWithDelay()).keyBy(4)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).apply(new WindowFunction5())
                .filter(tweetTimeWindow -> tweetTimeWindow.count > 100);
        answer.print().setParallelism(1);
//        answer.writeAsText("~/TweetCountOutOfOrder500s.txt").setParallelism(1);

        env.execute("Stream global WC");
    }


}

