package com.apachebeam.main;

import com.apachebeam.config.PipelineOptionsConfig;
import com.apachebeam.pipeline.BatchPipeline;
import com.apachebeam.pipeline.StreamingPipeline;
import com.apachebeam.reporter.PipelineReporter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PipelineApplication {

    public static void main(String[] args) {

        System.out.println("Running on MAIN branch");

        PipelineOptionsConfig options =
                PipelineOptionsFactory
                        .fromArgs(args)
                        .withValidation()
                        .as(PipelineOptionsConfig.class);


        System.out.println(" ------------------------ ");

        System.out.println(" Apache Beam pipeline starts ");
        System.out.println( "mode : " + options.getPipelineMode() );
        System.out.println( "Input : " + options.getInputFilePath() );
        System.out.println(" output : " + options.getOutputFilePath() );

        System.out.println(" ------------------------ ");


        System.out.println("We are in the feature branch ");

        System.out.println("We are in the feature branch  demo2  ");


        Pipeline pipeline = Pipeline.create(options);


        if("STREAMING".equalsIgnoreCase(options.getPipelineMode()))
        {
            // Build streaming pipeline
             StreamingPipeline.buildPipeline(pipeline,options);
        }
        else
        {
            // Build batch pipeline
            BatchPipeline.buildPipeline(pipeline,options);
        }

        PipelineResult result = pipeline.run();
         result.waitUntilFinish();

         // Print summary report after run
        PipelineReporter.printReport(result, options);



    }
}