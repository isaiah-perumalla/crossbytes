package org.example;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TAIL_COUNTER_OFFSET;

public class BroadcastPublisher {

    public static final int BUFFER_CAPACITY = 1024;

    public static void main(String[] args) throws IOException {

        String filePath = "/dev/shm/broadcast-test.dat";
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        int size = BUFFER_CAPACITY + BroadcastBufferDescriptor.TRAILER_LENGTH;
        fileChannel.truncate(size);
        MappedByteBuffer byteBuff = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.address(byteBuff), size);
        final long initialVal = buffer.getLongVolatile(BUFFER_CAPACITY + TAIL_COUNTER_OFFSET);
        BroadcastTransmitter transmitter = new BroadcastTransmitter(buffer);
        UnsafeBuffer scratch = new UnsafeBuffer(new byte[64]);
        System.out.println("publishing to shm file [" + filePath + "], press any key to start publishing");
        System.out.println("publish in progress.....");
        int MAX_VAL = Integer.MAX_VALUE;
        int msg_size = 25;
        long timeNow = System.nanoTime();
        long endTime = timeNow + TimeUnit.SECONDS.toNanos(60);
        long i = 0;
        while(System.nanoTime() < endTime) {
            scratch.putLong(0, i + initialVal);
            scratch.putLong(8 ,i + initialVal + 1);
            transmitter.transmit((int) i, scratch, 0, msg_size);
            i++;
        }
        System.out.println("publish completed [" + MAX_VAL + "] message published");
    }

}