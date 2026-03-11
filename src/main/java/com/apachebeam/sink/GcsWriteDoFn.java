package com.apachebeam.sink;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class GcsWriteDoFn extends DoFn<String, String> {

    private final Counter gcsWriteCounter =
            Metrics.counter(GcsWriteDoFn.class, "gcs_writes");

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        String data = c.element();

        // Simulate writing to GCS
        boolean success = writeToGcs(data);

        if (success) {
            gcsWriteCounter.inc();
            System.out.println("GCS Successfully written: " + data);
            c.output(data);
        } else {
            System.err.println("GCS Failed to write (simulated): " + data);
        }
    }

    private boolean writeToGcs(String data)
    {
        // In real world: use Google Cloud Storage client library
        // StorageBucketClient.write("gs://my-bucket/output/", data);
        System.out.println("GCS Simulating upload to gs://demo-bucket/output/ ...");
        return true; // simulate success
    }
}