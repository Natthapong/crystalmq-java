/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package crystalmq.java.camel;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

/**
 * The CrystalMQ producer.
 */
public class CrystalMQProducer extends DefaultProducer {

    private static final transient Logger log = LoggerFactory.getLogger(CrystalMQProducer.class);
    private CrystalMQEndpoint endpoint;
    private Socket socket = null;
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String TOPIC = "topic";
    public static final String MESSAGE = "message";

    public CrystalMQProducer(CrystalMQEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    public void process(Exchange exchange) throws Exception {

        String host = this.getEndpoint().getEndpointConfiguration().getParameter(HOST);
        String port = this.getEndpoint().getEndpointConfiguration().getParameter(PORT);
        String topic = this.getEndpoint().getEndpointConfiguration().getParameter(TOPIC);

        String message = exchange.getIn().getBody(String.class);

        if (message != null) {

            sendMessageToTopic(topic, message, host, port);
        } else {

            log.error("Message body is null.");
        }

    }


    private void sendMessageToTopic(String topic, String message, String host, String port) throws IOException {
        initialSocket(host, port);
        try {
            Map<String, Object> data = new HashMap();
            data.put(TOPIC, topic);
            data.put(MESSAGE, message);

            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.write(new MessagePack().write(data));
            dataOutputStream.flush();
        } catch (SocketException ex) {
            socket = null;
        }

    }

    private void initialSocket(String host, String port) throws IOException {

        if (socket == null || !socket.isConnected()) {

            log.info("Create CrystalMQ producer, host = {},port = {}", host, port);
            socket = new Socket(host, Integer.parseInt(port));
            log.info("Socket created.");
        }
    }


    @Override
    protected void doStop() throws Exception {
        super.doStop();
        log.info("Stopping CrystalMQ producer");
        if (socket.isConnected()) {
            socket.close();
        }
    }

}
