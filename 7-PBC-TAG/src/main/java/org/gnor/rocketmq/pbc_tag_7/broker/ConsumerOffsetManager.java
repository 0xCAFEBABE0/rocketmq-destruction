package org.gnor.rocketmq.pbc_tag_7.broker;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumerOffsetManager {
    public static final String TOPIC_GROUP_SEPARATOR = "@";
    protected ConcurrentMap<String/* topic@group */, Long> offsetTable = new ConcurrentHashMap<>(512);

    public ConcurrentMap<String/* topic@group */, Long> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentMap<String/* topic@group */, Long> offsetTable) {
        this.offsetTable = offsetTable;
    }

    String fileName = "/Users/qudian/data/store/config/consumerOffset.json";

    public void load() {
        try {
            String jsonString = null;
            File file = new File(fileName);
            if (file.exists()) {
                byte[] data = new byte[(int) file.length()];
                boolean result;

                try (FileInputStream inputStream = new FileInputStream(file)) {
                    int len = inputStream.read(data);
                    result = len == data.length;
                }
                if (result) {
                    jsonString = new String(data, StandardCharsets.UTF_8);
                }
            }

            if (jsonString != null) {
                ConsumerOffsetManager obj = JSON.parseObject(jsonString, ConsumerOffsetManager.class);
                if (obj != null) {
                    this.setOffsetTable(obj.getOffsetTable());
                }
                System.out.println("load " + fileName + " OK");
            }
        } catch (Exception e) {
            System.out.println("load " + fileName + " failed, and try to load backup file" + e);
        }
    }

    public synchronized void persist() {
        String jsonString = JSON.toJSONString(this, JSONWriter.Feature.PrettyFormat);
        if (jsonString != null) {
            try {
                File file = new File(fileName);
                File fileParent = file.getParentFile();
                if (fileParent != null) {
                    fileParent.mkdirs();
                }

                try (OutputStream os = Files.newOutputStream(file.toPath())) {
                    os.write(jsonString.getBytes(StandardCharsets.UTF_8));
                }
            } catch (IOException e) {
                System.out.println("persist file " + fileName + " exception" + e);
            }
        }
    }

    //public long queryOffset(final String group, final String topic) {
    public long queryOffset(final String topic) {
        // topic@group
        //String key = topic + TOPIC_GROUP_SEPARATOR + group;
        String key = topic;
        Long offset = this.offsetTable.get(key);
        if (null != offset) {
            return offset;
        }
        return 0L;
    }

    //public void commitOffset(final String group, final String topic, final long offset) {
    public void commitOffset(final String topic, final long offset) {
        //String key = topic + TOPIC_GROUP_SEPARATOR + group;
        String key = topic;
        this.offsetTable.put(key, offset);
    }
}
