package io.github.coderodde.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class ExternalIntMergeSortTest {

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
}
