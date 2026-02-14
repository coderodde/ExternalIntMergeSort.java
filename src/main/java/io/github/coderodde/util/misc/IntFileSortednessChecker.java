package io.github.coderodde.util.misc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class IntFileSortednessChecker {

    // 8 MiB direct buffer is usually a good start; tune if needed.
    private static final int BUF_BYTES = 8 * 1024 * 1024;

    /**
     * @return -1 if sorted; otherwise returns the 0-based index of the first inversion
     *         (i.e., where a[i] < a[i-1]).
     */
    public static long firstInversionIndex(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {

            long size = ch.size();
            
            if ((size % Integer.BYTES) != 0) {
                throw new IllegalArgumentException("File size not divisible by 4: " + size);
            }

            ByteBuffer bb = ByteBuffer.allocateDirect(BUF_BYTES);
            bb.order(ByteOrder.LITTLE_ENDIAN);

            boolean havePrev = false;
            int prev = 0;
            long index = 0; // current element index in the file

            while (true) {
                int r = ch.read(bb);
                if (r == -1) break;
                if (r == 0) continue;

                bb.flip();

                while (bb.remaining() >= Integer.BYTES) {
                    int cur = bb.getInt();
                    if (havePrev && cur < prev) {
                        return index; // inversion at a[index] (since a[index] < a[index-1])
                    }
                    prev = cur;
                    havePrev = true;
                    index++;
                }

                // Keep any leftover bytes (0..3) for next read (rare but correct):
                bb.compact();
            }

            // If file is well-formed (size%4==0), there should be no leftovers:
            if (bb.position() != 0) {
                throw new IOException("Unexpected leftover bytes at end: " + bb.position());
            }

            return -1;
        }
    }

    public static boolean isSorted(Path file) throws IOException {
        return firstInversionIndex(file) == -1;
    }

    // CLI usage: java CheckSortedInt32LE <path>
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java -jar APP.jar <path-to-file>");
            System.exit(2);
        }
        
        Path p = Path.of(args[0]);
        long inv = firstInversionIndex(p);
        
        if (inv == -1) {
            System.out.println("SORTED");
        } else {
            System.out.println("NOT SORTED. First inversion at index " + inv +
                               " (a[" + inv + "] < a[" + (inv - 1) + "])");
            System.exit(1);
        }
    }
}
