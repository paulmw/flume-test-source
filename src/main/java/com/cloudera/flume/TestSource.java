// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestSource extends AbstractSource implements EventDrivenSource, Configurable {

    private Random random;
    private int delay;

    @Override
    public void configure(Context context) {
        random = new Random(context.getInteger("seed", 0));
        delay = context.getInteger("delay", 200);
    }

    private ExecutorService service;

    @Override
    public synchronized void start() {
        service = Executors.newSingleThreadExecutor();
        EventHandler handler = new EventHandler(this.getChannelProcessor(), random, delay);
        service.execute(handler);
    }

    @Override
    public synchronized void stop() {
        // NOP
    }

    public static class EventHandler implements Runnable {

        private ChannelProcessor channelProcessor;
        private Random random;
        private int delay;

        public EventHandler(ChannelProcessor channelProcessor, Random random, int delay) {
            this.channelProcessor = channelProcessor;
            this.random = random;
            this.delay = delay;
        }

        @Override
        public void run() {
            boolean running = true;
            while (running) {
                try {
                    process();
                    Thread.sleep(delay);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        public void process() throws EventDeliveryException {
            channelProcessor.processEvent(generateEvent());
        }

        private Event generateEvent() {
            Map<String, String> header = new HashMap<String, String>();

            int numHeaders = random.nextInt(3);
            for (int i = 0; i < numHeaders; i++) {
                header.put("k" + random.nextInt(100), "v" + random.nextInt(100));
            }

            byte[] bytes = new byte[random.nextInt(10000)];
            random.nextBytes(bytes);

            return EventBuilder.withBody(bytes, header);
        }

    }
}
