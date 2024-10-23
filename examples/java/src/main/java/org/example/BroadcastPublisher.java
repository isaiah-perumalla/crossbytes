package org.example;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

public class BroadcastPublisher {
    public static void main(String[] args) throws IOException {

        String filePath = "/dev/shm/broadcast-test.dat";
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        int size = 1024 + BroadcastBufferDescriptor.TRAILER_LENGTH;
        fileChannel.truncate(size);
        MappedByteBuffer byteBuff = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.address(byteBuff), size);
        BroadcastTransmitter transmitter = new BroadcastTransmitter(buffer);
        UnsafeBuffer scratch = new UnsafeBuffer(new byte[64]);
        System.out.println("publishing to shm file [" + filePath + "], press any key to start publishing");
        int unused = System.in.read();
        System.out.println("publish in progress.....");
        int MAX_VAL = Integer.MAX_VALUE;
        int msg_size = 25;
        for (int i = 1; i < MAX_VAL; i++) {
            scratch.putLong(0, i);
            scratch.putLong(8 ,i + 1);
            transmitter.transmit(i, scratch, 0, msg_size);
        }
        System.out.println("publish completed [" + MAX_VAL + "] message published");
    }

}