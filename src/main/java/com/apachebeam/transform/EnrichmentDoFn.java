package com.apachebeam.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import java.time.Instant;
import java.util.UUID;

public class EnrichmentDoFn extends DoFn<String, String>
{

    private final Counter enrichedCounter =
            Metrics.counter(EnrichmentDoFn.class, "enriched_records");

    private final Distribution recordLength =
            Metrics.distribution(EnrichmentDoFn.class, "record_length_bytes");

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        String data = c.element();

        // Add enrichment metadata
        String recordId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String timestamp = Instant.now().toString();
        String source = "BEAM-PIPELINE-V2";

        String enriched = String.format(
                "[ID:%s] [TS:%s] [SRC:%s] [DATA:%s]",
                recordId, timestamp, source, data
        );

        enrichedCounter.inc();
        recordLength.update(enriched.length());

        System.out.println("ENRICHED " + enriched);
        c.output(enriched);
    }
}