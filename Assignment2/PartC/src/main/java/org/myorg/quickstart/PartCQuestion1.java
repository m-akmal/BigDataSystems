package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

class WindowFunction5 implements org.apache.flink.streaming.api.functions.windowing.WindowFunction<Tuple5<Integer, Integer, Integer, Long, String>, TweetTimeWindow, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Long, String>> iterable, Collector<TweetTimeWindow> collector) throws Exception {
        int count = 0;
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (Tuple5<Integer, Integer, Integer, Long, String> tweet : iterable) {
            start = Math.min(start, tweet.f3);
            end = Math.max(end, tweet.f3);
            count++;
        }
        collector.collect(new TweetTimeWindow(start, end, count, tuple.toString(), (end - start) / 60));
    }
}

class TweetWaterMarkAssigner implements org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<Tuple5<Integer, Integer, Integer, Long, String>> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple5<Integer, Integer, Integer, Long, String> tuple, long l) {
        return new Watermark(l);
    }

    @Override
    public long extractTimestamp(Tuple5<Integer, Integer, Integer, Long, String> tuple5, long l) {
        //timestamp in file is in seconds, flink expects it in milliseconds
        return tuple5.f3 * 1000;
    }
}

class TweetTimeWindow {
    long start;
    long end;
    int count;
    String tweetType;
    long interval;

    public TweetTimeWindow(long start, long end, int count, String tweetType, long interval) {
        this.start = start;
        this.end = end;
        this.count = count;
        this.tweetType = tweetType;
        this.interval = interval;
    }

    @Override
    public String toString() {
        return String.format("TimeWindow{start=%d, end=%d} interval=%d Count: %d Type: %s", start, end, interval, count, tweetType);
    }
}


public class PartCQuestion1 {

    public static void main(String[] args) throws Exception {

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple4<Integer, Integer, Long, String>> text = env.addSource(StreamingEmulator.create("/home/pavan/bigdata/flink/higgs-activity_time.txt"));

        DataStream<TweetTimeWindow> answer = text
                .flatMap(new FlatMapFunction<Tuple4<Integer, Integer, Long, String>, Tuple5<Integer, Integer, Integer, Long, String>>() {
                    @Override
                    public void flatMap(Tuple4<Integer, Integer, Long, String> tuple4, Collector<Tuple5<Integer, Integer, Integer, Long, String>> collector) throws Exception {
                        collector.collect(new Tuple5(1, tuple4.f0, tuple4.f1, tuple4.f2, tuple4.f3));
                    }
                })
                .assignTimestampsAndWatermarks(new TweetWaterMarkAssigner()).keyBy(4)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(1))).apply(new WindowFunction5())
//                .timeWindowAll(Time.minutes(1))
//                .apply(new WindowFunction4());
                .filter(tweetTimeWindow -> tweetTimeWindow.count > 100);
        answer.print().setParallelism(1);

//        answer.writeAsText("~/TweetCountRealTime_sliding1s.txt").setParallelism(1);
        env.execute("Stream global WC");
    }


}

