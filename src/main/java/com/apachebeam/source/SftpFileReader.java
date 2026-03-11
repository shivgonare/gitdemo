package com.apachebeam.source;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.Arrays;
import java.util.List;

public class SftpFileReader extends DoFn<String, String>
{

    @ProcessElement
    public void processElement(ProcessContext c) {
        String remotePath = c.element();

        System.out.println("[SFTP] Connecting to SFTP for path: " + remotePath);

        // Simulate reading lines from a remote SFTP file
        List<String> simulatedLines = simulateSftpRead(remotePath);

        for (String line : simulatedLines) {
            System.out.println("SFTP Read line: " + line);
            c.output(line);
        }
    }

    private List<String> simulateSftpRead(String path)
    {
        // In real world: use JSch or Apache Commons VFS to read remote files
        return Arrays.asList(
                "sftp-record-1 from " + path,
                "sftp-record-2 from " + path,
                "sftp-record-3 from " + path
        );
    }
}