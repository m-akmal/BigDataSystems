package org.myorg.quickstart;

/**
 * Created by pavan on 10/25/17.
 */

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * streaming simulation part
 */
class StreamingEmulator extends RichSourceFunction<Tuple4<Integer, Integer, Long, String>> {

    private volatile boolean running = true;
    private final String filename;

    private StreamingEmulator(String filename) {
        this.filename = filename;
    }

    public static StreamingEmulator create(String filename) {
        return new StreamingEmulator(filename);
    }

    @Override
    public void run(SourceContext<Tuple4<Integer, Integer, Long, String>> ctx) throws Exception {

        try {
            final File file = new File(filename);
            final BufferedReader br = new BufferedReader(new FileReader(file));

            String line = "";

            System.out.println("Start read data from \"" + filename + "\"");
            long count = 0L;
            while (running && (line = br.readLine()) != null) {
                if ((count++) % 10 == 0) {
                    Thread.sleep(1);
                }
                ctx.collect(genTuple(line));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private Tuple4<Integer, Integer, Long, String> genTuple(String line) {
        String[] item = line.split(" ");
        Tuple4<Integer, Integer, Long, String> record = new Tuple4<>();

        record.setField(Integer.parseInt(item[0]), 0);
        record.setField(Integer.parseInt(item[1]), 1);
        record.setField(Long.parseLong(item[2]), 2);
        record.setField(item[3], 3);

        return record;
    }
}