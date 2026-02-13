package io.github.coderodde.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * This class provides a method for external sorting a binary file containing 
 * {@code int} values in little-endian order.
 */
public class ExternalIntMergeSort {
    
    private static final class HeapEntry {
        final int key;
        final int streamIndex;
        
        HeapEntry(int key, int streamIndex) {
            this.key = key;
            this.streamIndex = streamIndex;
        }
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
        
        if (!Files.exists(outputPath)) {
            throw new RuntimeException(
                    "Output file \"" 
                            + outputPath.getFileName() 
                            + "\" does not exist.");
        }
        
        long freeMem = Runtime.getRuntime().freeMemory();
        long memThreshold = (3 * freeMem) / 4;
        long inputFileSize; 
        
        try {
            inputFileSize = getInputFileSize(inputPath);
        } catch (IOException ex) {
            throw new RuntimeException(
                    "Could not obtain the size of the input file: " 
                            + ex.getMessage(), 
                    ex);
        }
        
        if (inputFileSize % Integer.BYTES != 0) {
            throw new RuntimeException(
                    "The number of bytes in the input file is not divisible " + 
                    "by 4.");
        }
        
        try {
            if (inputFileSize < memThreshold) {
                sortInMainMemory(inputPath, 
                                 outputPath, 
                                 (int) (inputFileSize / Integer.BYTES));
                return;
            }
        } catch (IOException ex) {
            throw new RuntimeException(
                    "IO failed in main memory sorting: " + ex.getMessage(),
                    ex);
        }
        
        // Once here, do the external sorting:
        sortExternally(inputPath,
                       outputPath,
                       inputFileSize);
    }
    
    private static void sortExternally(Path inputPath,
                                       Path outputPath,
                                       long inputFileSize) {
        
        Path temporaryDirectory;
        
        try {
            temporaryDirectory = Files.createTempDirectory("ext-merge-int32");
        } catch (IOException ex) {
            throw new RuntimeException(
                "Could not create a temporary directory: " + ex.getMessage(),
                ex);
        }
        
        List<Path> runs;
        
        try {
            runs = createSortedRuns(inputPath, 
                                    temporaryDirectory,
                                    1000000);
        } catch (IOException ex) {
            throw new RuntimeException(
                    "createSortedRuns failed: " + ex.getMessage(), ex);
        }
        
        try {
            mergeRuns(runs, outputPath);
        } catch (IOException ex) {
            throw new RuntimeException(
                    "mergeRuns failed: " + ex.getMessage(), ex);
        }
        
        for (Path p : runs) {
            try {
                Files.deleteIfExists(p);
            } catch (IOException ignored) {
                
            }
        }
        
        try {
            Files.deleteIfExists(temporaryDirectory);
        } catch (IOException ignored) {
            
        }
    }
    
    private static List<Path> 
        createSortedRuns(Path inputPath, 
                         Path temporaryPath,
                         int maxIntsInMem) throws IOException {
        
            List<Path> runPaths = new ArrayList<>();
            
            int[] buffer = new int[maxIntsInMem];
            int runIndex = 0;
            
            try (DataInputStream in = 
                    new DataInputStream(
                            new BufferedInputStream(
                                    Files.newInputStream(inputPath)))) {
                
                while (true) {
                    int count = 0;
                    
                    for (; count < maxIntsInMem; ++count) {
                        buffer[count] = in.readInt();
                    }
                    
                    if (count == 0) {
                        break;
                    }
                    
                    Arrays.sort(buffer);
                    
                    Path runFile = 
                            temporaryPath
                                    .resolve("run-" + (runIndex++) + ".bin");
                    
                    try (DataOutputStream out = 
                        new DataOutputStream(
                            new BufferedOutputStream(
                                Files.newOutputStream(
                                    runFile, 
                                    StandardOpenOption.CREATE, 
                                    StandardOpenOption.TRUNCATE_EXISTING)))) {
                        
                        for (int i = 0; i < count; ++i) {
                            out.writeInt(buffer[i]);
                        }
                    }
                    
                    runPaths.add(runFile);
                }
            }
            
            return runPaths;
    }
    
    private static void mergeRuns(List<Path> inputPaths, Path outputPath) 
            throws IOException {
        
        if (inputPaths.isEmpty()) {
            Files.write(outputPath, 
                        new byte[0], 
                        StandardOpenOption.CREATE, 
                        StandardOpenOption.TRUNCATE_EXISTING);
            return;
        }
        
        DataInputStream[] streams = new DataInputStream[inputPaths.size()];
        
        int i = 0;
        
        for (Path p : inputPaths) {
            streams[i++] =
                    new DataInputStream(
                            new BufferedInputStream(Files.newInputStream(p)));
        }
        
        Queue<HeapEntry> q = 
                new PriorityQueue<>(Comparator.comparingInt(e -> e.key));
        
        for (i = 0; i < streams.length; ++i) {
            q.add(new HeapEntry(streams[i].readInt(), i));
        }
        
        try (DataOutputStream out =
                new DataOutputStream(
                        new BufferedOutputStream(
                            Files.newOutputStream(
                                    outputPath, 
                                    StandardOpenOption.CREATE, 
                                    StandardOpenOption.TRUNCATE_EXISTING)))) {
            
            while (!q.isEmpty()) {
                HeapEntry e = q.poll();
                out.writeInt(e.key);
                int next = streams[e.streamIndex].readInt();
                q.add(new HeapEntry(next, e.streamIndex));
            }
            
        } finally {
            for (DataInputStream s : streams) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    
                }
            }
        }
    }
    
    private static List<Path> createSortedRuns(Path input,
                                               int maximumIntsInMemory) {
        List<Path> runs = new ArrayList<>();
        
        try (DataInputStream in = 
                new DataInputStream(
                        new BufferedInputStream(
                                new FileInputStream(input.toFile())))) {
            
            long heapBytesFree = Runtime.getRuntime().freeMemory();
            
            int[] buffer = new int[maximumIntsInMemory];
            int end = 0;
            
            for (; end < maximumIntsInMemory; ++end) {
                buffer[end] = in.readInt();
            }
            
            if (end == 0)  {
                
            }
            
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(
                    "File not found: " + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new RuntimeException("IO exception: " + ex.getMessage(), ex);
        }
        
        return runs;
    }

    public static void main(String[] args) {
        System.out.println("Hello World!");
    }
    
    private static long getInputFileSize(Path inputPath) throws IOException {
        return Files.size(inputPath);
    }
    
    /**
     * Sorts the input file in the main memory delegating to 
     * {@link Arrays#sort(int[])}.
     * 
     * @param inputPath    the source file path to be sorted.
     * @param outputPath   the target file path.
     * @param capacity     the number of {@code int} keys in {@code inputPath}.
     * @throws IOException if I/O fails.
     */
    private static void sortInMainMemory(Path inputPath,
                                         Path outputPath,
                                         int capacity) 
        throws IOException {
        
        try (FileChannel inputChannel = 
                FileChannel.open(inputPath, StandardOpenOption.READ);
                
             FileChannel outputChannel =
                FileChannel.open(outputPath,
                                 StandardOpenOption.CREATE,
                                 StandardOpenOption.TRUNCATE_EXISTING,
                                 StandardOpenOption.WRITE)) {
            
            ByteBuffer buffer = ByteBuffer.allocate(capacity);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            while (buffer.hasRemaining()) {
                // TODO: Add debuging output
                //System.out.println("A");
                if (inputChannel.read(buffer) == -1) {
                    break;
                }
            }
            
            buffer.flip();
            
            int[] array = new int[capacity];
            
            for (int i = 0; i < capacity; ++i) {
                array[i] = buffer.getInt();
            }
            
            // Sort in the main memory:
            Arrays.sort(array);
            
            // Store array[] in outputChannel:
            buffer.clear();
            
            for (int value : array) {
                buffer.putInt(value);
            }
            
            buffer.flip();
            
            while (buffer.hasRemaining()) {
                outputChannel.write(buffer);
            }
        }
    }
    
    private static long normalizeInputFileSize(long inputFileSize) {
        return Long.min(inputFileSize, Integer.MAX_VALUE);
    }
}
