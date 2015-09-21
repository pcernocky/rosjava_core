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

package org.ros.internal.node.service;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteOrder;
import java.util.List;

/**
 * @author damonkohler@google.com (Damon Kohler)
 */
@Sharable
public final class ServiceResponseEncoder extends MessageToMessageEncoder<ServiceServerResponse> {

  @Override
  protected void encode(ChannelHandlerContext ctx, ServiceServerResponse msg, List<Object> out) throws Exception {
    ByteBuf buffer = ctx.alloc().buffer().order(ByteOrder.LITTLE_ENDIAN);
    buffer.writeByte(msg.getErrorCode());
    buffer.writeInt(msg.getMessageLength());
    buffer.writeBytes(msg.getMessage());
    out.add(buffer);
  }

}
