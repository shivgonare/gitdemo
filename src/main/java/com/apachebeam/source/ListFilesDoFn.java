package com.apachebeam.source;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class ListFilesDoFn extends DoFn<String, String>
{
    private final Counter fileCounter =
            Metrics.counter(ListFilesDoFn.class, "files_listed");

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        String line = c.element();
        fileCounter.inc();
        System.out.println("SOURCE Reading line: " + line);
        c.output(line);
    }

}