package com.apachebeam.pipeline;

import com.apachebeam.config.PipelineOptionsConfig;
import com.apachebeam.sink.DbStorageWriter;
import com.apachebeam.sink.GcsWriteDoFn;
import com.apachebeam.source.ListFilesDoFn;
import com.apachebeam.transform.EnrichmentDoFn;
import com.apachebeam.transform.ProcessingDoFn;
import com.apachebeam.transform.ValidationDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.w3c.dom.Text;

import static org.apache.beam.repackaged.core.org.apache.commons.lang3.Functions.apply;

public class BatchPipeline
{
     // Tags for main output and dead-letter queue
    public static final TupleTag<String> VALID_RECORDS = new TupleTag<String>(){};
    public static final TupleTag<String> DEAD_LETTER = new TupleTag<String>(){};

    public static Pipeline buildPipeline(Pipeline pipeline, PipelineOptionsConfig options)
    {
        //1. Read input file
        PCollection<String> rawLines =
                pipeline.apply(" read input file ", TextIO.read().from(options.getInputFilePath()));

        //2. List / log files being processed
        PCollection<String> listedFiles  =
                rawLines.apply(" list files ", ParDo.of(new ListFilesDoFn()));

        //3.Validate and split into valid vs dead-letter
        PCollectionTuple validatedRecords =
                listedFiles
            .apply("Validate Records", ParDo.of(new ValidationDoFn(VALID_RECORDS, DEAD_LETTER))
            .withOutputTags(VALID_RECORDS, TupleTagList.of(DEAD_LETTER)));


        PCollection<String> validRecords = validatedRecords.get(VALID_RECORDS);
        PCollection<String> deadLetterRecords = validatedRecords.get(DEAD_LETTER);


        //4. Process valid records ( transform/uppercase etc. )
        PCollection<String> processedRecords =
                validRecords.apply("Process Data", ParDo.of(new ProcessingDoFn()));

        //5. Enrich records ( add metadata, timestamps etc. )
        PCollection<String> enrichedRecords =
                processedRecords.apply("Enrich Records", ParDo.of(new EnrichmentDoFn()));


        //6. Write enriched records to GCS
        enrichedRecords.apply("Write to GCS", ParDo.of(new GcsWriteDoFn()));

        //7. Write enriched records to DB
        enrichedRecords.apply("Write to DB", ParDo.of(new DbStorageWriter()));

        //8. Write valid output to file
        enrichedRecords.apply("Write Output File", TextIO.write()
                .to(options.getOutputFilePath())
                .withSuffix(".txt")
                .withoutSharding());

        //9. Write dead-letter records to separate file
        deadLetterRecords
                .apply("Write Dead Letter", TextIO.write()
                        .to(options.getDeadLetterPath())
                        .withSuffix(".txt")
                        .withoutSharding());

        System.out.println("BatchPipeline Pipeline graph built successfully.");
        return pipeline;
    }

}
