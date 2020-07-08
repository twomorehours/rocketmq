package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.*;
import java.util.concurrent.ConcurrentMap;

public class TimeWheelTask implements Runnable {


    private long timeInNumber;
    private int offset;
    private int size;
    private DefaultMessageStore store;
    private ConcurrentMap<Long, MappedFile> logMap;


    public TimeWheelTask(long timeInNumber, int offset, int size, DefaultMessageStore store, ConcurrentMap<Long, MappedFile> logMap) {
        this.timeInNumber = timeInNumber;
        this.offset = offset;
        this.size = size;
        this.store = store;
        this.logMap = logMap;
    }


    @Override
    public void run() {
        MappedFile mappedFile = logMap.get(timeInNumber);
        if (mappedFile == null) {
            return;
        }
        SelectMappedBufferResult mappedBuffer = mappedFile.selectMappedBuffer(offset, size);
        MessageExt messageExt = MessageDecoder.decode(mappedBuffer.getByteBuffer(), true, false);
        store.putMessage(ScheduleMessageService2.toInner(messageExt));
    }


}
