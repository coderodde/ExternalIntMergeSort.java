package io.github.coderodde.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * This class provides a method for external sorting a binary file containing 
 * {@code int} values in little-endian order. The algorithm used is an in-place
 * radix sort.
 */
public final class ExternalIntRadixSort {

    private static final int RADIX = 256;
    private static final int PASSES = 4;
    private static final int BUFFER_SIZE = 1 << 20; // 1 MiB.
    
    private ExternalIntRadixSort() {
        
    }
    
    public static void sort(Path inputPath,
                            Path outputPath) {
        
        Objects.requireNonNull(inputPath,  "The input path is null.");
        Objects.requireNonNull(outputPath, "The output path is null.");
        
        if (!Files.exists(inputPath)) {
            throw new RuntimeException(
                    "Input file \"" 
                            + inputPath.getFileName() 
                            + "\" does not exist.");
        }
        
        try {
            Path parent = outputPath.getParent();
            
            if (parent != null) {
                Files.createDirectories(parent);
            }
            
            if (!Files.exists(outputPath)) {
                Files.createFile(outputPath);
            }
        } catch (IOException ex) {
            throw new RuntimeException("I/O failed: " + ex.getMessage(), ex);
        }
        
        Path workDirectoryPath = Path.of("radix_tmp");
        
        try {
            Files.createDirectories(workDirectoryPath);
        } catch (IOException ex) {
            throw new RuntimeException("I/O failed: " + ex.getMessage(), ex);
        }
        
        Path a = inputPath.toAbsolutePath();
        Path b = outputPath.toAbsolutePath();
        
        for (int pass = 0; pass < PASSES; ++pass) {
            Path[] bucketFiles = new Path[RADIX];
            DataOutputStream[] bucketOutputStreams =
                    new DataOutputStream[RADIX];
            
            try {
                for (int i = 0; i < RADIX; ++i) {
                    bucketFiles[i] = workDirectoryPath
                            .resolve(String.format("bucket_%02x.bin", i));
                    
                    bucketOutputStreams[i] = 
                        new DataOutputStream(
                            new BufferedOutputStream(
                                Files.newOutputStream(
                                    bucketFiles[i], 
                                    StandardOpenOption.CREATE,
                                    StandardOpenOption.TRUNCATE_EXISTING, 
                                    StandardOpenOption.WRITE), 
                                1 << 20));
                }
                
                try (DataInputStream in = 
                    new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(a), 
                                                1 << 20))) {
                    while (true) {
                        int x;
                        
                        try {
                            x = readIntLittleEndian(in);
                        } catch (EOFException ex) {
                            break;
                        }
                        
                        int bucketIndex = getBucketIndex(x, pass);
                        
                        writeIntLittleEndian(
                            bucketOutputStreams[bucketIndex], 
                            x);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(
                    "I/O failed: " + ex.getMessage(), 
                    ex);
            } finally {
                for (int i = 0; i < RADIX; ++i) {
                    if (bucketOutputStreams[i] != null) {
                        try {
                            bucketOutputStreams[i].close();
                        } catch (IOException ignored) {
                            
                        }
                    }
                }
            }
            
            try (OutputStream out = new BufferedOutputStream(
                Files.newOutputStream(
                        b, 
                        StandardOpenOption.CREATE, 
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE))) {
                
                for (int digit = 0; digit < RADIX; ++digit) {
                    Path bucket = bucketFiles[digit];
                    
                    if (!Files.exists(bucket)) {
                        continue;
                    }
                    
                    try (DataInputStream bin = 
                            new DataInputStream(
                                new BufferedInputStream(
                                    Files.newInputStream(bucket), 1 << 20))) {
                        
                        while (true) {
                            int x;
                            
                            try {
                                x = readIntLittleEndian(bin);
                            } catch (EOFException ex) {
                                break;
                            }
                            
                            writeIntLittleEndian(out, x);
                        }
                    }
                    
                    Files.deleteIfExists(bucket);
                }
                
            } catch (IOException ex) {
                throw new RuntimeException(
                        "I/O failed: " + ex.getMessage(),
                        ex);
            }
            
            Path tmp = a;
            a = b;
            b = tmp;
        }
        
        if (!a.equals(outputPath.toAbsolutePath())) {
            try {
                Files.copy(a, outputPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                throw new RuntimeException(
                        "I/O failed: " + ex.getMessage(),
                        ex);
            }
        }
    }
    
    private static int getBucketIndex(int x, int pass) {
        int d = (x >>> (pass * 8)) & 0xff;
        
        if (pass == 3) {
            d ^= 0x80;
        }
        
        return d;
    }
    
    private static int readIntLittleEndian(InputStream in) throws IOException {
        int b0 = in.read();
        int b1 = in.read();
        int b2 = in.read();
        int b3 = in.read();
        
        return (b0 & 0xff) 
            | ((b1 & 0xff) << 8) 
            | ((b2 & 0xff) << 16)
            | ((b3 & 0xff) << 24);
    }
    
    private static void writeIntLittleEndian(OutputStream out, int x) 
            throws IOException { 
            
        out.write(x & 0xff);
        out.write((x >>> 8) & 0xff);
        out.write((x >>> 16) & 0xff);
        out.write((x >>> 24) & 0xff);
    }
}
