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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;

public class CrystalMQComponentTest extends CamelTestSupport {

    @Test
    @Ignore
    public void testCrystalMQ() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedMinimumMessageCount(1);
        
        assertMockEndpointsSatisfied();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                from("crystalmq://consumer?host=172.16.1.250&port=1235&topic=sunseries&channel=ms-chiwa-test")
                        .process(new Processor() {
                            @Override
                            public void process(Exchange exchange) throws Exception {
                                System.out.println(exchange.getIn().getBody(String.class).trim());
                            }
                        })
                        .to("mock:result");

                from("timer://test?period=5000")
                        .process(new Processor() {
                            @Override
                            public void process(Exchange exchange) throws Exception {
                                exchange.getIn().setBody("Hello : " + new Date());
                                System.out.println(".");
                            }
                        })
                        .to("crystalmq://producer?host=172.16.1.250&port=1234&topic=sunseries");
            }
        };
    }
}
