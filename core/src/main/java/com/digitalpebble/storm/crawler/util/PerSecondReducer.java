package com.digitalpebble.storm.crawler.util;

import backtype.storm.metric.api.IReducer;

/** Used to return an average value per second **/
public class PerSecondReducer implements IReducer<TimeReducerState> {

    @Override
    public TimeReducerState init() {
        return new TimeReducerState();
    }

    @Override
    public TimeReducerState reduce(TimeReducerState accumulator, Object input) {
        if (input instanceof Double) {
            accumulator.sum += (Double) input;
        } else if (input instanceof Long) {
            accumulator.sum += ((Long) input).doubleValue();
        } else if (input instanceof Integer) {
            accumulator.sum += ((Integer) input).doubleValue();
        } else {
            throw new RuntimeException(
                    "MeanReducer::reduce called with unsupported input type `"
                            + input.getClass()
                            + "`. Supported types are Double, Long, Integer.");
        }
        return accumulator;
    }

    @Override
    public Object extractResult(TimeReducerState accumulator) {
        // time spent
        double msec = System.currentTimeMillis() - accumulator.started;
        if (msec == 0)
            return 0;
        double permsec = accumulator.sum / msec;
        return new Double(permsec * 1000d);
    }

}

class TimeReducerState {
    public long started = System.currentTimeMillis();
    public double sum = 0.0;
}
