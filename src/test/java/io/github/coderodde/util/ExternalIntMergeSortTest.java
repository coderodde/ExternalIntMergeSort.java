package io.github.coderodde.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class ExternalIntMergeSortTest {

    @Test
    public void sortLarger() throws IOException {
        String inputFileName  = "C:\\Users\\rodio\\Downloads\\R-4.5.2-win.exe";
        String outputFileName = "C:\\Temp\\sorted.test.bin";
        
        Path inputPath  = Path.of(inputFileName);
        Path outputPath = Path.of(outputFileName);
        
        ExternalIntMergeSort.sort(inputPath, 
                                  outputPath);
        
        assertTrue(isSorted(outputPath));
    }
    
    @Test
    public void sortInMainMemory() throws IOException {
        int[] array  = { 29, 8, 26 };
        int[] sorted = { 8, 26, 29 }; 
        
        writeArray("small-arr.in", array);
        
        ExternalIntMergeSort.sort(Path.of("small-arr.in"), 
                                  Path.of("small-arr.out"));
        
        int[] result = readLittleEndianInts("small-arr.out");
        
        assertTrue(Arrays.equals(sorted, result));
    }
    
    private static void writeArray(String fileName, int[] array) throws IOException {
        Path p = Path.of(fileName);
        
        try (FileChannel ch = 
                FileChannel.open(
                        p,
                        StandardOpenOption.CREATE, 
                        StandardOpenOption.TRUNCATE_EXISTING, 
                        StandardOpenOption.WRITE)) {
         
            ByteBuffer bb = ByteBuffer.allocate(array.length * Integer.BYTES);
            
            bb.order(ByteOrder.LITTLE_ENDIAN);
            
            for (int value : array) {
                bb.putInt(value);
            }
            
            bb.flip();
            ch.write(bb);
        }
    }
   
    public static int[] readLittleEndianInts(String fileName) throws IOException {

        Path path = Path.of(fileName);
        
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {

            long fileSize = channel.size();

            if (fileSize % Integer.BYTES != 0) {
                throw new IllegalStateException("File size not divisible by 4.");
            }

            int intCount = (int) (fileSize / Integer.BYTES);

            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            while (buffer.hasRemaining()) {
                channel.read(buffer);
            }

            buffer.flip();

            int[] result = new int[intCount];

            for (int i = 0; i < intCount; i++) {
                result[i] = buffer.getInt();
            }

            return result;
        }
    }
    
    private static boolean isSorted(Path p) throws IOException {
        int[] keys = readIntsFast(p);
        
        for (int i = 0; i < keys.length - 1; ++i) {
            if (keys[i] > keys[i + 1]) {
                return false;
            }
        }
        
        return true;
    }
    
    public static int[] readIntsFast(Path path) throws IOException {
        long size = Files.size(path);
        
        if ((size & 3L) != 0L) {
            throw new IllegalArgumentException("File size is not multiple of 4 bytes: " + size);
        }
        
        if (size > (long) Integer.MAX_VALUE * 4L) {
            throw new IllegalArgumentException("File too large for int[]: " + size);
        }

        int n = (int) (size >>> 2);
        int[] out = new int[n];

        // 8–64 MiB is usually a good range; tune if you want.
        final int CHUNK_BYTES = 16 * 1024 * 1024;
        ByteBuffer bb = ByteBuffer.allocateDirect(
                CHUNK_BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);

        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            int outPos = 0;

            while (outPos < n) {
                bb.clear();

                // Read up to CHUNK_BYTES, but avoid ending on non-4-byte boundary.
                int maxBytes = Math.min(CHUNK_BYTES, (n - outPos) * 4);
                bb.limit(maxBytes);

                while (bb.hasRemaining()) {
                    if (ch.read(bb) < 0) break;
                }

                bb.flip();
                IntBuffer ib = bb.asIntBuffer();
                int k = ib.remaining();
                ib.get(out, outPos, k);
                outPos += k;
            }
        }

        return out;
    }
}
