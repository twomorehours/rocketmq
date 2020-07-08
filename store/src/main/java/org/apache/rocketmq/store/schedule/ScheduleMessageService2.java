package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.util.TimeWheel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 你从没有见过的船新版本
 *
 * @author yuhao
 * @date 2020/7/7 4:26 下午
 */
public class ScheduleMessageService2 {

    /**
     * 内存保存30分钟的数据
     */
    static Integer MINUTES_IN_MEMORY = 30;

    /**
     * 提前10s load到内存
     */
    static Integer LOAD_IN_ADVANCE = 10;

    /**
     * 文件名对应文件
     * 20201030
     */
    private ConcurrentMap<Long, MappedFile> logMap = new ConcurrentHashMap<>();

    private DefaultMessageStore defaultMessageStore;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private TimeWheel timeWheel;

    /**
     * 重放线程
     */
    private ExecutorService reputMessageThreadPool;

    /**
     * checkpoint
     * pull log
     */
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * append message
     */
    private AppendMessageCallback appendMessageCallback;

    private FileChannel checkPoint;


    public ScheduleMessageService2(
            DefaultMessageStore defaultMessageStore) throws FileNotFoundException {
        this.defaultMessageStore = defaultMessageStore;
        this.appendMessageCallback = new ScheduleAppendMessageCallback(
                defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        String storePathRootDir = defaultMessageStore.getMessageStoreConfig().getStorePathRootDir();
        String checkPointPath = storePathRootDir + File.separator + "scheduleCheckpoint";
        this.checkPoint = new RandomAccessFile(checkPointPath, "rw").getChannel();
    }


    public synchronized void scheduleRequest(DispatchRequest request) {
        long now = System.currentTimeMillis();
        MessageExt messageExt = defaultMessageStore.lookMessageByOffset(
                request.getCommitLogOffset(), request.getMsgSize());
        if (messageExt == null) {
            return;
        }
        String property = messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_IN_SECONDS);
        long executeTime = Long.parseLong(property) * 1000 + now;
        messageExt.putUserProperty(MessageConst.PROPERTY_EXECUTE_TIME_IN_SECONDS,
                String.valueOf(executeTime / 1000));
        MessageAccessor.clearProperty(messageExt, MessageConst.PROPERTY_DELAY_TIME_IN_SECONDS);
        long executeTimeInNumber = timeInNumber(executeTime);
        MappedFile mappedFile = logMap.get(executeTimeInNumber);
        if (mappedFile == null) {
            String storePathRootDir = defaultMessageStore.getMessageStoreConfig().getStorePathRootDir();
            String logPath = storePathRootDir + File.separator + "schedulelog" + File.separator + executeTimeInNumber;
            try {
                mappedFile = new MappedFile(logPath, 10 * 1024 * 1024);
                logMap.put(executeTimeInNumber, mappedFile);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
        AppendMessageResult result = mappedFile.appendMessage(toInner(messageExt),
                this.appendMessageCallback);
        if (!result.isOk()) {
            return;
        }
        if (now / 1000 + MINUTES_IN_MEMORY * 60 >= executeTime / 1000) {
            // 直接进入时间轮
            timeWheel.addTask(executeTime / 1000,
                    new TimeWheelTask(executeTimeInNumber, (int) result.getWroteOffset(),
                            result.getWroteBytes(), defaultMessageStore, logMap));
        }
    }

    public boolean load() {
        String storePathRootDir = defaultMessageStore.getMessageStoreConfig().getStorePathRootDir();
        String logPath = storePathRootDir + File.separator + "schedulelog";
        try {
            File dir = new File(logPath);
            if (!dir.exists()) {
                return true;
            }
            File[] files = dir.listFiles();
            if (files == null || files.length == 0) {
                return true;
            }
            long checkpoint = getCheckpoint();
            long timeInNumber = timeInNumber(checkpoint);
            long timeInNumberNow = timeInNumber(System.currentTimeMillis());
            for (File file : files) {
                long fileNameInNumber = Long.parseLong(file.getName());
                // 加载checkpoint到现在的数据
                if (fileNameInNumber >= timeInNumber && fileNameInNumber <= timeInNumberNow) {
                    logMap.put(Long.parseLong(file.getName()), new MappedFile(
                            logPath + File.separator + file.getName(),
                            10 * 1024 * 1024));
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            reputMessageThreadPool.shutdown();
            timeWheel.stop();
            try {
                checkPoint.force(true);
                checkPoint.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void start() {
        if (!this.started.compareAndSet(false, true)) {
            return;
        }
        reputMessageThreadPool = Executors.newSingleThreadExecutor();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.timeWheel = new TimeWheel(MINUTES_IN_MEMORY * 60 + LOAD_IN_ADVANCE,
                reputMessageThreadPool);
        this.timeWheel.start();
        // 将checkpoint后的数据加载到内存
        for (Map.Entry<Long, MappedFile> entry : logMap.entrySet()) {
            MappedFile file = entry.getValue();
            int pos = loadMappedFile(entry.getKey(), file);
            file.setWrotePosition(pos);
            file.setFlushedPosition(pos);
            file.setCommittedPosition(pos);
        }
        //启动checkpoint
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // checkpoint
                    long checkpoint = timeWheel.getCurrentScheduleTime();
                    setCheckPoint(checkpoint);
                    // 清理内存
                    Iterator<Map.Entry<Long, MappedFile>> iterator = logMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, MappedFile> entry = iterator.next();
                        if (entry.getKey() < checkpoint) {
                            entry.getValue().release();
                            iterator.remove();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                long now = System.currentTimeMillis();
                long nowNumber = timeInNumber(now);
                long nextNumber = timeInNumber(now + LOAD_IN_ADVANCE * 1000);
                if (nowNumber == nextNumber || !logMap.containsKey(nextNumber)) {
                    return;
                }
                loadMappedFile(nextNumber, logMap.get(nextNumber));
            }
        }, 0L, 5L, TimeUnit.SECONDS);
    }


    private int loadMappedFile(long key, MappedFile file) {
        ByteBuffer byteBuffer = file.getMappedByteBuffer().slice();
        int pos = 0;
        while (true) {
            byteBuffer.position(pos);
            int msgLength = byteBuffer.getInt();
            byteBuffer.position(pos);
            if (msgLength == 0) {
                break;
            }
            MessageExt messageExt = MessageDecoder.decode(byteBuffer, true, false);
            String property = messageExt.getProperty(
                    MessageConst.PROPERTY_EXECUTE_TIME_IN_SECONDS);
            try {
                // checkpoint 之前的不用处理
                if (Long.parseLong(property) <= getCheckpoint()) {
                    continue;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            timeWheel.addTask(Long.parseLong(property),
                    new TimeWheelTask(key, pos, msgLength, defaultMessageStore, logMap));
            pos += msgLength;
        }
        return pos;
    }

    private long getCheckpoint() throws IOException {
        ByteBuffer allocate = ByteBuffer.allocate(8);
        checkPoint.read(allocate);
        allocate.flip();
        if (!allocate.hasRemaining()) {
            return 0;
        }
        return allocate.getLong();
    }

    private void setCheckPoint(long offset) throws IOException {
        ByteBuffer allocate = ByteBuffer.allocateDirect(8);
        allocate.putLong(offset);
        allocate.flip();
        checkPoint.write(allocate);
        checkPoint.force(true);
    }

    private static long timeInNumber(long millis) {
        if (millis == 0) {
            return 0;
        }
        millis = millis - (millis % (MINUTES_IN_MEMORY * 60 * 1000));
        String currTimeInNumber = new SimpleDateFormat("yyyyMMddHHmm").format(new Date(millis));
        return Long.parseLong(currTimeInNumber);
    }


    public static MessageExtBrokerInner toInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(
                MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setQueueId(msgExt.getQueueId());

        return msgInner;
    }


}
