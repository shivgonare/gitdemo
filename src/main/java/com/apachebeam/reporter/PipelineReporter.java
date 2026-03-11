package com.apachebeam.reporter;

import com.apachebeam.config.PipelineOptionsConfig;
import org.apache.beam.sdk.PipelineResult;

public class PipelineReporter
{
    public static void printReport(PipelineResult result, PipelineOptionsConfig options)
    {
        System.out.println(" ------------------------ ");
        System.out.println(" Pipeline Execution Summary ");

        System.out.println("sstatus "+padRight(result.getState().toString(), 20));
        System.out.println("mode "+padRight(options.getPipelineMode(), 20));
        System.out.println("input "+padRight(options.getInputFilePath(), 20));
        System.out.println("output "+padRight(options.getOutputFilePath(), 20));
        System.out.println("DLQ "+padRight(options.getOutputFilePath(), 20));
        System.out.println("TimeStamp "+padRight(options.getOutputFilePath(), 20));
        System.out.println(" ------------------------ ");

    }

    private static String padRight(String s, int n)
    {
        if (s.length() > n) s = s.substring(0, n - 3) + "...";
        return String.format("%-" + n + "s", s);
    }
}
