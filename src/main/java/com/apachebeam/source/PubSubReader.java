package com.apachebeam.source;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class PubSubReader extends DoFn<String, String> {

    private final Counter pubSubCounter =
            Metrics.counter(PubSubReader.class, "pubsub_messages_read");

    @ProcessElement
    public void processElement(ProcessContext c) {
        String message = c.element();
        pubSubCounter.inc();

        System.out.println("PubSub Received message: " + message);

        // Simulate deserializing a PubSub JSON message
        String deserializedPayload = deserialize(message);
        c.output(deserializedPayload);
    }

    private String deserialize(String raw)
    {
        // In real world: parse JSON payload from PubSub message
        return "PUBSUB_EVENT: " + raw;
    }
}