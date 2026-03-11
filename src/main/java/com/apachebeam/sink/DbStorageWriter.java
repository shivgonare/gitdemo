package com.apachebeam.sink;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.FileWriter;
import java.io.IOException;

public class DbStorageWriter extends DoFn<String, String>
{

    private final Counter dbWriteCounter =
            Metrics.counter(DbStorageWriter.class, "db_writes");

    private static final String DB_OUTPUT_FILE = "output/db-simulation.txt";

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        String record = c.element();

        boolean written = writeToDatabase(record);

        if (written)
        {
            dbWriteCounter.inc();
            System.out.println("DB Record inserted: " + record);
            c.output(record);
        }
    }

    private boolean writeToDatabase(String record)
    {
        // In real world: use JDBC / Hibernate / Cloud Spanner client
        try (FileWriter fw = new FileWriter(DB_OUTPUT_FILE, true)) {
            fw.write(record + "\n");
            return true;
        } catch (IOException e)
        {
            System.err.println("DB Write failed: " + e.getMessage());
            return false;
        }
    }
}