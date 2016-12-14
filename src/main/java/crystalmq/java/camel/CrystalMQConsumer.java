/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package crystalmq.java.camel;

import crystalmq.java.camel.Exception.ConsumerException;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * The CrystalMQ consumer.
 */
public class CrystalMQConsumer extends DefaultConsumer {

    private static final transient Logger log = LoggerFactory.getLogger(CrystalMQConsumer.class);

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String TOPIC = "topic";
    public static final String CHANNEL = "channel";

    private final CrystalMQEndpoint endpoint;
    private Socket socket = null;

    public CrystalMQConsumer(CrystalMQEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    @Override
    protected void doStart() throws Exception {

        String host = this.getEndpoint().getEndpointConfiguration().getParameter(HOST);
        String port = this.getEndpoint().getEndpointConfiguration().getParameter(PORT);
        String topic = this.getEndpoint().getEndpointConfiguration().getParameter(TOPIC);
        String channel = this.getEndpoint().getEndpointConfiguration().getParameter(CHANNEL);

        try {
            initialSocket(host, port, topic, channel);

            Thread t = new Thread(new ConsumerThread(socket, this.endpoint, this.getProcessor()));
            t.start();
        } catch (ConsumerException ex) {
            log.error("Error on consumer router : {} ", ex.getMessage());
            Thread.sleep(3000);
            doStart();
        }

    }

    private void initialSocket(String host, String port, String topic, String channel) throws IOException {

        log.info("Create CrystalMQ consumer, host = {},port = {}", host, port);
        log.info("Register topic = {},channel = {}", topic, channel);

        if (socket == null || !socket.isConnected()) {

            socket = new Socket(host, Integer.parseInt(port));
            registerTopicAndChannel(topic, channel);
            log.info("Socket created.");
        }


    }

    private void registerTopicAndChannel(String topic, String channel) throws IOException {

        Map<String, Object> data = new HashMap();
        data.put(TOPIC, topic);
        data.put(CHANNEL, channel);

        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.write(new MessagePack().write(data));
        dataOutputStream.flush();
    }



    @Override
    protected void doStop() throws Exception {
        super.doStop();
        log.info("Stopping CrystalMQ consumer");
        if (socket.isConnected()) {
            socket.close();
        }
    }


}
