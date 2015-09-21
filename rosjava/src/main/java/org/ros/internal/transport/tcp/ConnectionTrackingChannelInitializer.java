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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.ros.internal.transport.ConnectionTrackingHandler;

import java.nio.ByteOrder;

/**
 * @author damonkohler@google.com (Damon Kohler)
 */
public class ConnectionTrackingChannelInitializer extends ChannelInitializer {

  public static final String CONNECTION_TRACKING_HANDLER = "ConnectionTrackingHandler";
  public static final String LENGTH_FIELD_BASED_FRAME_DECODER = "LengthFieldBasedFrameDecoder";
  public static final String LENGTH_FIELD_PREPENDER = "LengthFieldPrepender";

  private final ConnectionTrackingHandler connectionTrackingHandler;
  
  public ConnectionTrackingChannelInitializer(ChannelGroup channelGroup){
    this.connectionTrackingHandler = new ConnectionTrackingHandler(channelGroup);
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().addLast(CONNECTION_TRACKING_HANDLER, connectionTrackingHandler);
    ch.pipeline().addLast(LENGTH_FIELD_PREPENDER, new LengthFieldPrepender(ByteOrder.LITTLE_ENDIAN, 4, 0, false));
    ch.pipeline().addLast(LENGTH_FIELD_BASED_FRAME_DECODER, new LengthFieldBasedFrameDecoder(
      ByteOrder.LITTLE_ENDIAN, Integer.MAX_VALUE, 0, 4, 0, 4, true));
  }
}
