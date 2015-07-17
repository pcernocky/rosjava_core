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

package org.ros.internal.transport.tcp;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.ros.internal.node.server.NodeIdentifier;
import org.ros.internal.node.service.DefaultServiceServer;
import org.ros.internal.node.service.ServiceManager;
import org.ros.internal.node.service.ServiceResponseEncoder;
import org.ros.internal.node.topic.DefaultPublisher;
import org.ros.internal.node.topic.SubscriberIdentifier;
import org.ros.internal.node.topic.TopicIdentifier;
import org.ros.internal.node.topic.TopicParticipantManager;
import org.ros.internal.transport.ConnectionHeader;
import org.ros.internal.transport.ConnectionHeaderFields;
import org.ros.namespace.GraphName;

/**
 * A {@link ChannelInboundHandler} which will process the TCP server handshake.
 * 
 * @author damonkohler@google.com (Damon Kohler)
 * @author kwc@willowgarage.com (Ken Conley)
 */
@Sharable
public class TcpServerHandshakeHandler extends ChannelInboundHandlerAdapter {

  private final TopicParticipantManager topicParticipantManager;
  private final ServiceManager serviceManager;

  public TcpServerHandshakeHandler(TopicParticipantManager topicParticipantManager,
      ServiceManager serviceManager) {
    this.topicParticipantManager = topicParticipantManager;
    this.serviceManager = serviceManager;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf incomingBuffer = (ByteBuf) msg;
    ConnectionHeader incomingHeader;
    try {
      incomingHeader = ConnectionHeader.decode(incomingBuffer);
    }
    finally {
      incomingBuffer.release();
    }
    if (incomingHeader.hasField(ConnectionHeaderFields.SERVICE)) {
      handleServiceHandshake(ctx, incomingHeader);
    } else {
      handleSubscriberHandshake(ctx, incomingHeader);
    }
  }

  private void handleServiceHandshake(ChannelHandlerContext ctx, ConnectionHeader incomingHeader) {
    GraphName serviceName = GraphName.of(incomingHeader.getField(ConnectionHeaderFields.SERVICE));
    Preconditions.checkState(serviceManager.hasServer(serviceName));
    DefaultServiceServer<?, ?> serviceServer = serviceManager.getServer(serviceName);
    ctx.channel().writeAndFlush(serviceServer.finishHandshake(incomingHeader));
    String probe = incomingHeader.getField(ConnectionHeaderFields.PROBE);
    if (probe != null && probe.equals("1")) {
      ctx.channel().close();
    } else {
      ctx.pipeline().replace(TcpServerInitializer.LENGTH_FIELD_PREPENDER, "ServiceResponseEncoder",
        new ServiceResponseEncoder());
      ctx.pipeline().replace(this, "ServiceRequestHandler", serviceServer.newRequestHandler());
    }
  }

  private void handleSubscriberHandshake(ChannelHandlerContext ctx, ConnectionHeader incomingConnectionHeader)
    throws InterruptedException {
    Preconditions.checkState(incomingConnectionHeader.hasField(ConnectionHeaderFields.TOPIC),
        "Handshake header missing field: " + ConnectionHeaderFields.TOPIC);
    GraphName topicName =
        GraphName.of(incomingConnectionHeader.getField(ConnectionHeaderFields.TOPIC));
    Preconditions.checkState(topicParticipantManager.hasPublisher(topicName),
        "No publisher for topic: " + topicName);
    DefaultPublisher<?> publisher = topicParticipantManager.getPublisher(topicName);
    ByteBuf outgoingBuffer = publisher.finishHandshake(incomingConnectionHeader);
    Channel channel = ctx.channel();
    channel.writeAndFlush(outgoingBuffer).sync();
    String nodeName = incomingConnectionHeader.getField(ConnectionHeaderFields.CALLER_ID);
    publisher.addSubscriber(new SubscriberIdentifier(NodeIdentifier.forName(nodeName),
        new TopicIdentifier(topicName)), channel);

    // Once the handshake is complete, there will be nothing incoming on the
    // channel. So, we replace the handshake handler with a handler which will
    // drop everything.
    ctx.pipeline().replace(this, "DiscardHandler", new ChannelInboundHandlerAdapter());
  }
}
