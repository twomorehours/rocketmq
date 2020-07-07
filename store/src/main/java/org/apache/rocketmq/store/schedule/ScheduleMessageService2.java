package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 你没有见过的船新版本
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
     * 提前5分钟load到内存
     */
    static Integer LOAD_IN_ADVANCE = 5;

    /**
     * 文件名对应文件
     * 20201030
     */
    private ConcurrentMap<Long, MappedFile> logMap = new ConcurrentHashMap<>();

    private DefaultMessageStore defaultMessageStore;

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 重放线程
     */
    private ExecutorService reputMessageThreadPool;

    /**
     * fsync
     * checkpoint
     * pull log
     */
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * append message
     */
    private AppendMessageCallback appendMessageCallback;


    public ScheduleMessageService2(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.appendMessageCallback = new ScheduleAppendMessageCallback(
                defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

    }


    public synchronized void scheduleRequest(DispatchRequest request) {
        long now = System.currentTimeMillis();
        MessageExt messageExt = defaultMessageStore.lookMessageByOffset(
                request.getCommitLogOffset(), request.getMsgSize());
        String property = messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_IN_SECONDS);
        long executeTime = Long.parseLong(property) + now;
        long executeTimeInNumber = timeInNumber(executeTime);
        MappedFile mappedFile = logMap.get(executeTime);
        if (mappedFile == null) {
            String storePathRootDir = defaultMessageStore.getMessageStoreConfig().getStorePathRootDir();
            String logPath = storePathRootDir + File.separator + "schedulelog" + File.separator + executeTimeInNumber;
            try {
                mappedFile = new MappedFile(logPath, Integer.MAX_VALUE);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
        int beforePosition = mappedFile.getWrotePosition();
        mappedFile.appendMessagesInner(messageExt, this.appendMessageCallback);
        if (now + MINUTES_IN_MEMORY >= executeTime) {
            // 直接进入时间轮

        }
    }

    public boolean load() {
        //TODO load
        // 加载logMap
        return true;
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            //TODO shutdown checkpoint
            reputMessageThreadPool.shutdown();
        }
    }

    public void start() {
        if (!this.started.compareAndSet(false, true)) {
            return;
        }
        reputMessageThreadPool = Executors.newSingleThreadExecutor();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        //TODO start
        // 启动定时checkpoint 启动时间轮
    }

    private static long timeInNumber(long millis) {
        millis = millis - (millis % (MINUTES_IN_MEMORY * 60 * 1000));
        String currTimeInNumber = new SimpleDateFormat("yyyyMMddHHmm").format(new Date(millis));
        return Long.parseLong(currTimeInNumber);
    }


    public static void main(String[] args) {
        System.out.println(timeInNumber(System.currentTimeMillis()));
    }

}
