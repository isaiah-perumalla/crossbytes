package org.example;

import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.broadcast.BroadcastReceiver;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

public class BroadcastReceiverTest {
    public static void main(String[] args) throws IOException {

        String filePath = "/dev/shm/broadcast-test.dat";
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        int size = 1024 + BroadcastBufferDescriptor.TRAILER_LENGTH;
        fileChannel.truncate(size);
        MappedByteBuffer byteBuff = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.address(byteBuff), size);
        BroadcastReceiver receiver = new BroadcastReceiver(buffer);
        UnsafeBuffer scratch = new UnsafeBuffer(new byte[64]);
        System.out.println("Receiving from shm file [" + filePath + "]");
        int count = 0;
        long lastVal = 0;
        int lastMsgId = 0;
        long timeNow = System.nanoTime();
        long endTime = timeNow + TimeUnit.SECONDS.toNanos(60);
        while(System.nanoTime() < endTime) {
            int msgId = 0;
            long val0 = 0;
            long val1 = 0;
            boolean ok = receiver.receiveNext();
            if (ok) {
                msgId = receiver.typeId();
                int offset = receiver.offset();
                val0 = receiver.buffer().getLong(offset);
                val1 = receiver.buffer().getLong(offset + 8);
            }
            if (ok && receiver.validate()) {
                assert msgId != lastMsgId;
                assert val0 > lastVal;
                assert val1 == 1 + val0;
                count++;
            }
        }
        System.out.println("receive completed [" + count + "] message received" + "lapped-count[" + receiver.lappedCount() + "]");
    }
}
