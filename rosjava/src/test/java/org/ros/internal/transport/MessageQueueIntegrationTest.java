/*
 * Copyright (C) 2011 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.ros.internal.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.ros.concurrent.CancellableLoop;
import org.ros.internal.message.DefaultMessageDeserializer;
import org.ros.internal.message.DefaultMessageSerializer;
import org.ros.internal.message.Message;
import org.ros.internal.message.definition.MessageDefinitionReflectionProvider;
import org.ros.internal.message.topic.TopicMessageFactory;
import org.ros.internal.node.service.ServiceManager;
import org.ros.internal.node.topic.TopicParticipantManager;
import org.ros.internal.transport.queue.IncomingMessageQueue;
import org.ros.internal.transport.queue.OutgoingMessageQueue;
import org.ros.internal.transport.tcp.TcpClient;
import org.ros.internal.transport.tcp.TcpClientManager;
import org.ros.internal.transport.tcp.TcpServerInitializer;
import org.ros.message.MessageDefinitionProvider;
import org.ros.message.MessageIdentifier;
import org.ros.message.MessageListener;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author damonkohler@google.com (Damon Kohler)
 */
public class MessageQueueIntegrationTest {

  private static final boolean DEBUG = false;
  private static final Log log = LogFactory.getLog(MessageQueueIntegrationTest.class);

  private static final int QUEUE_CAPACITY = 128;

  private ExecutorService executorService;
  private TcpClientManager firstTcpClientManager;
  private TcpClientManager secondTcpClientManager;
  private OutgoingMessageQueue<Message> outgoingMessageQueue;
  private IncomingMessageQueue<std_msgs.String> firstIncomingMessageQueue;
  private IncomingMessageQueue<std_msgs.String> secondIncomingMessageQueue;
  private std_msgs.String expectedMessage;

  private class ServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      if (DEBUG) {
        log.info("Channel connected: " + ctx.channel().toString());
      }
      Channel channel = ctx.channel();
      outgoingMessageQueue.addChannel(channel);
      super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      if (DEBUG) {
        log.info("Channel disconnected: " + ctx.channel().toString());
      }
      super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      if (DEBUG) {
        log.info("Channel exception: " + ctx.channel().toString());
      }
      ctx.channel().close();
      throw new RuntimeException(cause);
    }
  }

  @Before
  public void setup() {
    executorService = Executors.newCachedThreadPool();
    MessageDefinitionProvider messageDefinitionProvider = new MessageDefinitionReflectionProvider();
    TopicMessageFactory topicMessageFactory = new TopicMessageFactory(messageDefinitionProvider);
    expectedMessage = topicMessageFactory.newFromType(std_msgs.String._TYPE);
    expectedMessage.setData("Would you like to play a game?");
    outgoingMessageQueue =
        new OutgoingMessageQueue<Message>(new DefaultMessageSerializer(), executorService);
    firstIncomingMessageQueue =
        new IncomingMessageQueue<std_msgs.String>(new DefaultMessageDeserializer<std_msgs.String>(
            MessageIdentifier.of(std_msgs.String._TYPE), topicMessageFactory), executorService);
    secondIncomingMessageQueue =
        new IncomingMessageQueue<std_msgs.String>(new DefaultMessageDeserializer<std_msgs.String>(
            MessageIdentifier.of(std_msgs.String._TYPE), topicMessageFactory), executorService);
    firstTcpClientManager = new TcpClientManager(executorService);
    firstTcpClientManager.addNamedChannelHandler(firstIncomingMessageQueue.getMessageReceiver());
    secondTcpClientManager = new TcpClientManager(executorService);
    secondTcpClientManager.addNamedChannelHandler(secondIncomingMessageQueue.getMessageReceiver());
  }

  @After
  public void tearDown() {
    outgoingMessageQueue.shutdown();
    executorService.shutdown();
  }

  private void startRepeatingPublisher() {
    executorService.execute(new CancellableLoop() {
      @Override
      protected void loop() throws InterruptedException {
        outgoingMessageQueue.add(expectedMessage);
        Thread.sleep(100);
      }
    });
  }

  private Channel buildServerChannel() {
    TopicParticipantManager topicParticipantManager = new TopicParticipantManager();
    ServiceManager serviceManager = new ServiceManager();
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.group(new NioEventLoopGroup());
    bootstrap.channel(NioServerSocketChannel.class);
    ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    bootstrap.childHandler(new TcpServerInitializer(channelGroup, topicParticipantManager, serviceManager) {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        super.initChannel(ch);
        // We're not interested firstIncomingMessageQueue testing the
        // handshake here. Removing it means connections are established
        // immediately.
        ch.pipeline().remove(TcpServerInitializer.HANDSHAKE_HANDLER);
        ch.pipeline().addLast("ServerHandler", new ServerHandler());
      }
    });
    return bootstrap.bind(new InetSocketAddress(0)).syncUninterruptibly().channel();
  }

  private TcpClient connect(TcpClientManager TcpClientManager, Channel serverChannel) {
    return TcpClientManager.connect("Foo", serverChannel.localAddress());
  }

  private CountDownLatch expectMessage(IncomingMessageQueue<std_msgs.String> incomingMessageQueue)
      throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    incomingMessageQueue.addListener(new MessageListener<std_msgs.String>() {
      @Override
      public void onNewMessage(std_msgs.String message) {
        assertEquals(message, expectedMessage);
        latch.countDown();
      }
    }, QUEUE_CAPACITY);
    return latch;
  }

  private void expectMessages() throws InterruptedException {
    CountDownLatch firstLatch = expectMessage(firstIncomingMessageQueue);
    CountDownLatch secondLatch = expectMessage(secondIncomingMessageQueue);
    assertTrue(firstLatch.await(3, TimeUnit.SECONDS));
    assertTrue(secondLatch.await(3, TimeUnit.SECONDS));
  }

  @Test
  public void testSendAndReceiveMessage() throws InterruptedException {
    startRepeatingPublisher();
    Channel serverChannel = buildServerChannel();
    connect(firstTcpClientManager, serverChannel);
    connect(secondTcpClientManager, serverChannel);
    expectMessages();
  }

  @Test
  public void testSendAndReceiveLatchedMessage() throws InterruptedException {
    // Setting latched mode and writing a message should cause any
    // IncomingMessageQueues that connect in the future to receive the message.
    outgoingMessageQueue.setLatchMode(true);
    outgoingMessageQueue.add(expectedMessage);
    Channel serverChannel = buildServerChannel();
    firstIncomingMessageQueue.setLatchMode(true);
    secondIncomingMessageQueue.setLatchMode(true);
    connect(firstTcpClientManager, serverChannel);
    connect(secondTcpClientManager, serverChannel);
    // The first set of incoming messages could either be from the
    // OutgoingMessageQueue latching or the Subscriber latching. This is
    // equivalent to waiting for the message to arrive and ensures that we've
    // latched it in.
    expectMessages();
    // The second set of incoming messages can only be from the
    // IncomingMessageQueue latching since we only sent one message.
    expectMessages();
  }

  @Test
  public void testSendAfterIncomingQueueShutdown() throws InterruptedException {
    startRepeatingPublisher();
    Channel serverChannel = buildServerChannel();
    connect(firstTcpClientManager, serverChannel);
    firstTcpClientManager.shutdown();
    outgoingMessageQueue.add(expectedMessage);
  }

  @Test
  public void testSendAfterServerChannelClosed() throws InterruptedException {
    startRepeatingPublisher();
    Channel serverChannel = buildServerChannel();
    connect(firstTcpClientManager, serverChannel);
    assertTrue(serverChannel.close().await(1, TimeUnit.SECONDS));
    outgoingMessageQueue.add(expectedMessage);
  }

  @Test
  public void testSendAfterOutgoingQueueShutdown() throws InterruptedException {
    startRepeatingPublisher();
    Channel serverChannel = buildServerChannel();
    connect(firstTcpClientManager, serverChannel);
    outgoingMessageQueue.shutdown();
    outgoingMessageQueue.add(expectedMessage);
  }
}
