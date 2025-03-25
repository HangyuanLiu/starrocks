// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.nio.charset.StandardCharsets;

public class HttpClient extends SimpleChannelInboundHandler<HttpObject> {
    private static final Logger LOG = LogManager.getLogger(HttpClient.class);
    private static final EventLoopGroup GROUP = new NioEventLoopGroup();

    private final HttpRequest request;
    private final BaseResponse response;

    public HttpClient(HttpRequest request, BaseResponse response) {
        this.request = request;
        this.response = response;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpResponse httpResponse) {
            LOG.info("Received HTTP Response: Status = {}", httpResponse.status());
        }

        if (msg instanceof HttpContent httpContent) {
            String content = httpContent.content().toString(StandardCharsets.UTF_8);
            response.appendContent(content);

            if (msg instanceof LastHttpContent) {
                ctx.close();
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        HttpRequest httpRequest = request;
        if (httpRequest == null) {
            LOG.error("Request is null, closing connection.");
            ctx.close();
            return;
        }

        ctx.writeAndFlush(httpRequest).addListener(future -> {
            if (!future.isSuccess()) {
                LOG.error("Failed to send HTTP request", future.cause());
                ctx.close();
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error(cause.getMessage(), cause);
        ctx.close();
    }

    public static void send(HttpRequest request, BaseResponse response) {
        try {
            URI uri = new URI(request.uri());
            String host = uri.getHost();
            int port = (uri.getPort() != -1) ? uri.getPort() : (uri.getScheme().equalsIgnoreCase("https") ? 443 : 80);
            redirect(host, port, request, response);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void redirect(String host, int port, HttpRequest request, BaseResponse response) {
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(GROUP)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new HttpClientCodec())
                                    .addLast(new HttpObjectAggregator(8192))
                                    .addLast(new HttpClient(request, response));
                        }
                    });

            ChannelFuture future = bootstrap.connect(host, port).sync();
            future.channel().closeFuture().sync();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}