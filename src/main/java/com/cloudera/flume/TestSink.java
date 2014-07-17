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

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.AbstractSink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class TestSink extends AbstractSink implements Configurable {

    private Random random;

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            event = channel.take();
            if (event != null) {
                Event reference = generateEvent();
                if(!Arrays.equals(event.getBody(), reference.getBody())) {
                    throw new IllegalStateException("Event body is different than expected.");
                }
                if(!event.getHeaders().equals(reference.getHeaders())) {
                    throw new IllegalStateException("Event headers are different than expected.");
                }
            } else {
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Throwable t) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to log event: " + event, t);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
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

    @Override
    public void configure(Context context) {
        random = new Random(context.getInteger("seed", 0));
    }
}
