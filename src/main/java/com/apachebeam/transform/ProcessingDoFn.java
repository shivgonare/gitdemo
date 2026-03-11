package com.apachebeam.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class ProcessingDoFn extends DoFn<String, String> {

    private final Counter processedCounter =
            Metrics.counter(ProcessingDoFn.class, "processed_records");

    @ProcessElement
    public void processElement(ProcessContext c)
    {

        String data = c.element();

        // Transform: trim, uppercase, and tag
        String processed = "PROCESSED: " + data.trim().toUpperCase();

        processedCounter.inc();
        System.out.println("PROCESSED " + processed);
        c.output(processed);
    }
}