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

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ros.exception.RosRuntimeException;

import java.io.IOException;
import java.nio.channels.Channels;

/**
 * Adds new {@link Channels} to the provided {@link ChannelGroup}.
 * 
 * @author damonkohler@google.com (Damon Kohler)
 */
@Sharable
public class ConnectionTrackingHandler extends ChannelInboundHandlerAdapter {

  private static final boolean DEBUG = false;
  private static final Log log = LogFactory.getLog(ConnectionTrackingHandler.class);

  /**
   * The channel group the connection is to be part of.
   */
  private final ChannelGroup channelGroup;

  public ConnectionTrackingHandler(ChannelGroup channelGroup) {
    this.channelGroup = channelGroup;
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    if (DEBUG) {
      log.info("Channel registered: " + ctx.channel());
    }
    channelGroup.add(ctx.channel());
    super.channelRegistered(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    if (DEBUG) {
      log.info("Channel closed: " + ctx.channel());
    }
    super.channelUnregistered(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    ctx.channel().close();
    if (cause instanceof IOException) {
      // NOTE(damonkohler): We ignore exceptions here because they are common
      // (e.g. network failure, connection reset by peer, shutting down, etc.)
      // and should not be fatal. However, in all cases the channel should be
      // closed.
      if (DEBUG) {
        log.error("Channel exception: " + ctx.channel(), cause);
      } else {
        log.error("Channel exception: " + cause);
      }
    } else {
      throw new RosRuntimeException(cause);
    }
  }
}
