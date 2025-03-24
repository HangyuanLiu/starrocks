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

package com.starrocks.http.rest;

import com.nimbusds.jose.jwk.JWKSet;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.OAuth2Context;
import com.starrocks.authentication.OpenIdConnectVerifier;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.system.Frontend;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Map;

public class OAuth2Action extends RestBaseAction {
    public OAuth2Action(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/callback", new OAuth2Action(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String authorizationCode = getSingleParameter(request, "code", r -> r);
        String connectionIdStr = getSingleParameter(request, "connectionId", r -> r);
        long connectionId = Long.parseLong(connectionIdStr);

        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();

        int fid = (int) ((connectionId >> 24) & 0xFF);
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontend(fid);
        if (!nodeMgr.getMySelf().getNodeName().equals(frontend.getNodeName())) {
            forwardHttpRequest(frontend.getHost(), frontend.getRpcPort(), request, response);
            return;
        }

        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        ConnectContext context = connectScheduler.getContext(connectionId);
        if (context == null) {
            response.appendContent(
                    "<h1><div style=\"text-align: center;\">" + "Not found connect context " + connectionId + "</div></h1>");
            //response.appendContent("Not found connect context " + connectionId);
            sendResult(request, response);
            return;
        }

        OAuth2Context oAuth2Context = context.getOAuth2Context();
        String oidcToken = getToken(authorizationCode, oAuth2Context, connectionId);

        try {
            JWKSet jwkSet;
            try {
                jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(oAuth2Context.jwksUrl());
            } catch (IOException | ParseException e) {
                throw new AuthenticationException(e.getMessage());
            }

            JSONObject authResponse = new JSONObject(oidcToken);
            String accessToken = authResponse.getString("access_token");
            String idToken = authResponse.getString("id_token");

            OpenIdConnectVerifier.verify(idToken, context.getQualifiedUser(), jwkSet,
                    oAuth2Context.principalFiled(),
                    oAuth2Context.requiredIssuer(),
                    oAuth2Context.requiredAudience());

            context.setToken(idToken);
        } catch (Exception e) {
            //throw new AuthenticationException(e.getMessage());
            response.appendContent(e.getMessage());
            sendResult(request, response);
            return;
        }

        /*
        OAuth2TokenMgr oAuth2TokenMgr = GlobalStateMgr.getCurrentState().getoAuth2TokenMgr();
        OAuth2TokenMgr.Resource resource = new OAuth2TokenMgr.Resource();
        resource.authorizationCode = authorizationCode;
        oAuth2TokenMgr.oAuth2WaitCallbackList.put(connection    Id, resource);

         */

        response.appendContent("Login Success");
        //response.appendContent(openIdConnect);
        sendResult(request, response);
    }

    public void forwardHttpRequest(String forwardHost, int forwardPort, BaseRequest request, BaseResponse response) {
        // 创建一个事件循环组
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            // 1. 创建一个BootStrap来连接目标主机
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new HttpClientCodec())
                                    .addLast(new HttpObjectAggregator(8192))
                                    .addLast(new HttpResponseDecoder())
                                    .addLast(new ForwardHandler(request, response)
                                    );  // 转发请求的Handler
                        }
                    });

            // 2. 连接到目标地址
            ChannelFuture future = bootstrap.connect(forwardHost, forwardPort).sync();

            // 等待连接关闭
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // 优雅地关闭EventLoopGroup
            group.shutdownGracefully();
        }
    }

    public class ForwardHandler extends SimpleChannelInboundHandler<HttpObject> {
        private BaseRequest request;
        private BaseResponse response;

        public ForwardHandler(BaseRequest request, BaseResponse response) {
            this.request = request;
            this.response = response;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) msg;
                DefaultFullHttpResponse defaultFullHttpResponse =
                        (DefaultFullHttpResponse) msg;
                String result = defaultFullHttpResponse.content()
                        .toString(Charset.forName("UTF-8"));
                response.appendContent(result);
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(request.getRequest());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    private String getToken(String authorizationCode, OAuth2Context oAuth2Context,
                            Long connectionId) {
        // Step 3: 通过授权码获取令牌
        Map<Object, Object> body = Map.of(
                "grant_type", "authorization_code",
                "code", authorizationCode,
                "redirect_uri", oAuth2Context.redirectUrl() + "?connectionId=" + connectionId,
                "client_id", oAuth2Context.clientId(),
                "client_secret", oAuth2Context.clientSecret()
        );

        String requestBody = body.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8) + "=" +
                        URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8))
                .reduce((a, b) -> a + "&" + b)
                .orElse("");

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(oAuth2Context.tokenServerUrl()))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        java.net.http.HttpResponse<String> response;
        try {
            response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        // Step 4: 处理响应
        if (response.statusCode() == 200) {
            return response.body();
        } else {
            return null;
        }
    }
}
