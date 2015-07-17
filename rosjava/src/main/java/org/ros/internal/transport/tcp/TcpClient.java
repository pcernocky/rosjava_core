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
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ros.exception.RosRuntimeException;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author damonkohler@google.com (Damon Kohler)
 */
public class TcpClient {

  private static final boolean DEBUG = false;
  private static final Log log = LogFactory.getLog(TcpClient.class);

  private static final int DEFAULT_CONNECTION_TIMEOUT_DURATION = 5;
  private static final TimeUnit DEFAULT_CONNECTION_TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static final boolean DEFAULT_KEEP_ALIVE = true;

  private final ChannelGroup channelGroup;
  private final Bootstrap bootstrap;
  private final List<NamedChannelHandler> namedChannelHandlers;

  private Channel channel;

  public TcpClient(final ChannelGroup channelGroup, final Executor executor) {
    this.channelGroup = channelGroup;

    bootstrap = new Bootstrap();
    bootstrap.group(new NioEventLoopGroup());
    bootstrap.channel(NioSocketChannel.class);

    setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT_DURATION, DEFAULT_CONNECTION_TIMEOUT_UNIT);
    setKeepAlive(DEFAULT_KEEP_ALIVE);
    namedChannelHandlers = Lists.newArrayList();
  }

  public void setConnectionTimeout(final long duration, final TimeUnit unit) {
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TimeUnit.MILLISECONDS.convert(duration, unit));
  }

  public void setKeepAlive(final boolean value) {
    bootstrap.option(ChannelOption.SO_KEEPALIVE, value);
  }

  public void addNamedChannelHandler(final NamedChannelHandler namedChannelHandler) {
    namedChannelHandlers.add(namedChannelHandler);
  }

  public void addAllNamedChannelHandlers(final List<NamedChannelHandler> namedChannelHandlers) {
    this.namedChannelHandlers.addAll(namedChannelHandlers);
  }

  public void connect(final String connectionName, final SocketAddress socketAddress) {
    final TcpClientInitializer tcpClientInitializer = new TcpClientInitializer(channelGroup) {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        super.initChannel(ch);
        for (final NamedChannelHandler namedChannelHandler : namedChannelHandlers) {
          ch.pipeline().addLast(namedChannelHandler.getName(), namedChannelHandler);
        }
      }
    };
    bootstrap.handler(tcpClientInitializer);
    final ChannelFuture future = bootstrap.connect(socketAddress).awaitUninterruptibly();
    if (future.isSuccess()) {
      channel = future.channel();
      if (DEBUG) {
        log.info("Connected to: " + socketAddress);
      }
    } else {
      // We expect the first connection to succeed. If not, fail fast.
      throw new RosRuntimeException("Connection exception: " + socketAddress, future.cause());
    }
  }

  public Channel getChannel() {
    return channel;
  }

  public ChannelFuture write(final ByteBuf buffer) {
    Preconditions.checkNotNull(channel);
    Preconditions.checkNotNull(buffer);
    return channel.writeAndFlush(buffer);
  }
}
