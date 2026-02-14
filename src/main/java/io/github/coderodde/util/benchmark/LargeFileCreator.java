package io.github.coderodde.util.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 *
 * @author Rodion "rodde" Efremov
 * @version 1.0.0 ()
 * @since 1.0.0 ()
 */
public class LargeFileCreator {

    private final Random random = new Random();

    public static void main(String[] args) throws IOException {
        Path out = Path.of(args.length > 0 ? args[0] : "ints-4gb.bin");

        final long targetBytes = 4L * 1024 * 1024 * 1024; // 4 GiB
        final long totalInts   = targetBytes / Integer.BYTES; // 2^30

        // 8 MiB buffer (must be multiple of 4)
        final int bufferBytes = 8 * 1024 * 1024;
        Random random = new Random();
        ByteBuffer buf = ByteBuffer.allocateDirect(bufferBytes)
                                   .order(ByteOrder.LITTLE_ENDIAN);

        try (FileChannel ch = FileChannel.open(out,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

            for (long i = 0; i < totalInts; ++i) {
                buf.clear();
                buf.putInt((int) -i);
                buf.flip();
                
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
            }
            
            ch.force(true);
        }
    }
}

