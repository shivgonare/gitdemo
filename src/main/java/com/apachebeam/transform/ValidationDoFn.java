package com.apachebeam.transform;

// Validates records, routes bad ones to DLQ

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class ValidationDoFn extends DoFn<String, String>
{
    private final TupleTag<String> validTag;
    private final TupleTag<String> deadLetterTag;


    // Beam Metrics counters — show up in runner logs!
    private final Counter validCounter
            = Metrics.counter(ValidationDoFn.class, "valid_records");

    private final Counter invalidCounter
            = Metrics.counter(ValidationDoFn.class, "invalid_records");

    public ValidationDoFn(TupleTag<String> validTag, TupleTag<String> deadLetterTag)
    {
        this.validTag = validTag;
        this.deadLetterTag = deadLetterTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        String data = c.element();

        if (isValid(data)) {
            validCounter.inc();
            c.output(validTag, data);
            System.out.println("VALID  " + data);
        } else {
            invalidCounter.inc();
            String dlqRecord = "DLQ " + System.currentTimeMillis() + "REASON_invalid_record_DATA:" + data;
            c.output(deadLetterTag, dlqRecord);
            System.out.println("DLQ Sent to Dead Letter: " + data);
        }
    }

    private boolean isValid(String data)
    {
        if (data == null || data.trim().isEmpty())
        {
            return false;
        }
        if (data.length() < 3)
        {
            return false;
        }
        if (data.matches(".*[\\$\\#\\@\\!]{2,}.*"))
        {
            return false; // reject garbage chars
        }

        return true;
    }


}
