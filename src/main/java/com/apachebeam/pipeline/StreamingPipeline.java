package com.apachebeam.pipeline;

import com.apachebeam.config.PipelineOptionsConfig;
import com.apachebeam.source.PubSubReader;
import com.apachebeam.transform.EnrichmentDoFn;
import com.apachebeam.transform.ProcessingDoFn;
import com.apachebeam.transform.ValidationDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Arrays;
import java.util.List;



public class StreamingPipeline
{

    public static void buildPipeline(Pipeline pipeline , PipelineOptionsConfig options)
    {
        // Simulated streaming messages (in real use, replace with PubSubIO.readStrings())

        List<String> simulatedMessages = Arrays.asList
                ( "streaming-event-001: user_login",
                "streaming-event-002: purchase_made",
                "",   // intentionally bad record for DLQ demo
                "streaming-event-003: page_view",
                "INVALID$$#",
                "streaming-event-004: logout"
        );

        System.out.println("StreamingPipeline Simulating " + simulatedMessages.size() + " streaming events...");

        pipeline
                .apply(" Read PubSub Events ", Create.of(simulatedMessages))
                .apply("Log PubSub", ParDo.of(new PubSubReader()))
                .apply("Validate Stream", ParDo.of(
                                new ValidationDoFn(BatchPipeline.VALID_RECORDS, BatchPipeline.DEAD_LETTER))
                        .withOutputTags(BatchPipeline.VALID_RECORDS,
                                TupleTagList.of(BatchPipeline.DEAD_LETTER)))
                .get(BatchPipeline.VALID_RECORDS)
                .apply("Process Stream", ParDo.of(new ProcessingDoFn()))
                .apply("Enrich Stream", ParDo.of(new EnrichmentDoFn()));

    }

}
