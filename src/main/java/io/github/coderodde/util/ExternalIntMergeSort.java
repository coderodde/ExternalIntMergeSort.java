package io.github.coderodde.util;

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
public final class ExternalIntMergeSort {
    
    private static final int BUFFER_SIZE = 64 * 1024;
    
    private ExternalIntMergeSort() {
        
    }
    
    private static final class HeapEntry {
        final int key;
        final int runIndex;
        
        HeapEntry(int key, int runIndex) {
            this.key = key;
            this.runIndex = runIndex;
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
        
        long freeMem = Runtime.getRuntime().freeMemory();
        long memThreshold = (3L * freeMem) / 4L;
        
        try {
            // TODO: inputFileSize >= Integer.MAX_VALUE -> extenral sort!
            if (inputFileSize <= memThreshold) {
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
       
        System.out.println("external");
        
        // Once here, do the external sorting:
        sortExternally(inputPath,
                       outputPath,
                       inputFileSize);
        
        System.out.println("done");
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
            int memorySize = computeMaximumIntsInMemory();
            
            runs = createSortedRuns(inputPath, 
                                    temporaryDirectory,
                                    memorySize);
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
         
    private static void mergeRuns(List<Path> inputPaths, Path outputPath) 
            throws IOException {
        
        if (inputPaths.isEmpty()) {
            Files.write(outputPath, 
                        new byte[0], 
                        StandardOpenOption.CREATE, 
                        StandardOpenOption.TRUNCATE_EXISTING);
            return;
        }
        
        FileChannel[] channels = new FileChannel[inputPaths.size()];
        ByteBuffer[]  buffers  = new ByteBuffer [inputPaths.size()];
        
        Queue<HeapEntry> heap =
                new PriorityQueue<>(Comparator.comparingInt(e -> e.key));
        
        int[] temp = new int[1];
        
        try {
            for (int i = 0; i < inputPaths.size(); ++i) {
                channels[i] = 
                    FileChannel.open(inputPaths.get(i),
                                     StandardOpenOption.READ);
                
                buffers[i] = ByteBuffer.allocateDirect(BUFFER_SIZE);
                buffers[i].order(ByteOrder.LITTLE_ENDIAN);
                buffers[i].limit(0);
                
                boolean fine = readNextInt(channels[i],
                                           buffers[i],
                                           temp, 
                                           0);
                
                
                if (fine) {
                    heap.add(new HeapEntry(temp[0], i));
                }
            }
            
            try (FileChannel out = 
                 FileChannel.open(outputPath, 
                                  StandardOpenOption.CREATE, 
                                  StandardOpenOption.TRUNCATE_EXISTING, 
                                  StandardOpenOption.WRITE)) {
                
                ByteBuffer outBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
                
                outBuffer.order(ByteOrder.LITTLE_ENDIAN);
                
                while (!heap.isEmpty()) {
                    HeapEntry e = heap.poll();
                    
                    if (outBuffer.remaining() < Integer.BYTES) {
                        outBuffer.flip();
                        
                        while (outBuffer.hasRemaining()) {
                            out.write(outBuffer);
                        }
                        
                        outBuffer.clear();
                    }
                    
                    outBuffer.putInt(e.key);
                    
                    boolean fine = readNextInt(channels[e.runIndex],
                                               buffers [e.runIndex], 
                                               temp, 
                                               0);
                    
                    if (fine) {
                        heap.add(new HeapEntry(temp[0], e.runIndex));
                    } 
                }
                
                outBuffer.flip();
                
                while (outBuffer.hasRemaining()) {
                    out.write(outBuffer);
                }
            }
            
            
        } finally {
            for (FileChannel channel : channels) {
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (IOException ignored) {
                        
                    }
                }
            }
        }
    }
    
    private static int computeMaximumIntsInMemory() {
        long max   = Runtime.getRuntime().maxMemory();    // heap limit
        long total = Runtime.getRuntime().totalMemory(); // currently committed heap
        long free  = Runtime.getRuntime().freeMemory();   // free within committed heap

        // Heap we can still grow into:
        long heapAvailable = (max - total) + free;

        // Spend, say, 50% of available heap on the int[].
        // (Input buffer is direct memory; still keep headroom.)
        long bytesForIntArray = heapAvailable / 2;

        long ints = bytesForIntArray / Integer.BYTES;

        // Some sanity caps:
        ints = Math.min(ints, 20_000_000L); // e.g. cap at 20M (~80MB int[])
        ints = Math.max(ints, 1_000_000L);  // ensure not too tiny

        return (int) Math.min(ints, Integer.MAX_VALUE);
    }

    private static List<Path> createSortedRuns(Path inputPath,
                                               Path temporaryPath,
                                               int maximumIntsInMemory) throws IOException {
        List<Path> runs = new ArrayList<>();
        int[] array = new int[maximumIntsInMemory];
        int runIndex = 0;
        
        try (FileChannel in = 
             FileChannel.open(inputPath, StandardOpenOption.READ)) {
            
            ByteBuffer inputBuffer = 
                ByteBuffer.allocateDirect(maximumIntsInMemory * Integer.BYTES);
            
            ByteBuffer outputBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
            
            inputBuffer .order(ByteOrder.LITTLE_ENDIAN);
            outputBuffer.order(ByteOrder.LITTLE_ENDIAN);
            
            while (true) {
                inputBuffer.clear();
                
                int bytesReadTotal = 0;
                
                while (inputBuffer.hasRemaining()) {
                    int r = in.read(inputBuffer);
                    
                    if (r == -1) {
                        break;
                    }
                    
                    if (r == 0) {
                        continue;
                    }
                    
                    bytesReadTotal += r;
                }
                
                if (bytesReadTotal == 0) {
                    break;
                }
                
                inputBuffer.flip();
                
                int intsRead = bytesReadTotal / Integer.BYTES;
                
                for (int i = 0; i < intsRead; ++i) {
                    array[i] = inputBuffer.getInt();
                }
                
                Arrays.sort(array, 0, intsRead);
                
                Path runFile = 
                        temporaryPath.resolve("run-" + (runIndex++) + ".bin");
                
                try (FileChannel out = 
                     FileChannel.open(runFile,
                                      StandardOpenOption.CREATE,
                                      StandardOpenOption.TRUNCATE_EXISTING, 
                                      StandardOpenOption.WRITE)) {
                    
                    outputBuffer.clear();
                    
                    for (int i = 0; i < intsRead; ++i) {
                        if (outputBuffer.remaining() < Integer.BYTES) {
                            outputBuffer.flip();
                            
                            while (outputBuffer.hasRemaining()) {
                                out.write(outputBuffer);
                            }
                            
                            outputBuffer.clear();
                        }
                        
                        outputBuffer.putInt(array[i]);
                    }
                    
                    outputBuffer.flip();
                    
                    while (outputBuffer.hasRemaining()) {
                        out.write(outputBuffer);
                    }
                }
                
                runs.add(runFile);
                
                if (bytesReadTotal < 
                        maximumIntsInMemory * (long) Integer.BYTES) {
                    break;
                }
            }
        }
        
        return runs;
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
            
            ByteBuffer buffer = ByteBuffer.allocate(capacity * Integer.BYTES);
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
    
    private static boolean readNextInt(FileChannel channel,
                                       ByteBuffer buffer,
                                       int[] out,
                                       int index) throws IOException {
        while (buffer.remaining() < Integer.BYTES) {
            buffer.compact();
            
            int r;
            
            do {
                r = channel.read(buffer);
            } while (r == 0);
            
            buffer.flip();
            
            if (r == -1) {
                return false;
            }
            
            if (buffer.remaining() < Integer.BYTES) {
                return false;
            }
        }
        
        out[index] = buffer.getInt();
        return true;
    }
}
