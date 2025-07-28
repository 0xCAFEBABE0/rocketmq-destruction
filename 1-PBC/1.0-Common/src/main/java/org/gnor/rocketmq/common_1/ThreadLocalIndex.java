package org.gnor.rocketmq.common_1;

import java.util.Random;

public class ThreadLocalIndex {
    private final ThreadLocal<Integer> index = new ThreadLocal<>();
    private final Random random = new Random();
    private final static int POSITIVE_MASK = 0x7FFFFFFF;

    public int incrementAndGet() {
        Integer index = this.index.get();
        if (null == index) {
            index = random.nextInt();
        }
        this.index.set(++index);
        return index & POSITIVE_MASK;
    }
}
