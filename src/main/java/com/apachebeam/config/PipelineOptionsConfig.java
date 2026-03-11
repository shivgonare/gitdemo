package com.apachebeam.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface PipelineOptionsConfig extends PipelineOptions
{
    @Description("Input file path")
    @Default.String("input/sample-file.txt")
    String getInputFilePath();
    void setInputFilePath(String value);

    @Description("Output file path")
    @Default.String("output/sample-file.txt")
    String getOutputFilePath();
    void setOutputFilePath(String value);

    @Description("Enable Data Validation(true/false)")
    @Default.Boolean(true)
    boolean getEnableDataValidation();
    void setEnableDataValidation(boolean value);

    @Description("Pipeline mode: BATCH or STREAMING")
    @Default.String("BATCH")
    String getPipelineMode();
    void setPipelineMode(String value);

    @Description("Dead letter queue output path")
    @Default.String("output/dead-letter.txt")
    String getDeadLetterPath();
    void setDeadLetterPath(String value);










}
