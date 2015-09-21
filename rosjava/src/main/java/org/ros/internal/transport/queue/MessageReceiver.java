/*
 * Copyright (C) 2012 Google Inc.
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

package org.ros.internal.transport.queue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ros.concurrent.CircularBlockingDeque;
import org.ros.internal.transport.tcp.NamedChannelHandler;
import org.ros.message.MessageDeserializer;

/**
 * @author damonkohler@google.com (Damon Kohler)
 * 
 * @param <T>
 *          the message type
 */
@Sharable
public class MessageReceiver<T> extends ChannelInboundHandlerAdapter implements NamedChannelHandler {

  private static final boolean DEBUG = false;
  private static final Log log = LogFactory.getLog(MessageReceiver.class);

  private final CircularBlockingDeque<LazyMessage<T>> lazyMessages;
  private final MessageDeserializer<T> deserializer;

  public MessageReceiver(CircularBlockingDeque<LazyMessage<T>> lazyMessages,
      MessageDeserializer<T> deserializer) {
    this.lazyMessages = lazyMessages;
    this.deserializer = deserializer;
  }

  @Override
  public String getName() {
    return "IncomingMessageQueueChannelHandler";
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf buffer = (ByteBuf) msg;
    if (DEBUG) {
      log.info(String.format("Received %d byte message.", buffer.readableBytes()));
    }
    // todo we have to release the buffer somewhere
    lazyMessages.addLast(new LazyMessage<T>(buffer, deserializer));
  }

  @Override
  public String toString() {
    return String.format("%s: %s", getClass().getSimpleName(), super.toString());
  }

}
