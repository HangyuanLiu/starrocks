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

package com.starrocks.authentication;

import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class OAuth2AuthenticationProvider implements AuthenticationProvider {
    private final OAuth2Context oAuth2Context;

    public OAuth2AuthenticationProvider(String authServerUrl,
                                        String tokenServerUrl,
                                        String oauthRedirectUrl,
                                        String clientId,
                                        String clientSecret,
                                        String jwksUrl,
                                        String principalFiled,
                                        String requiredIssuer,
                                        String requiredAudience,
                                        Long connectWaitTimeout) {
        oAuth2Context = new OAuth2Context(
                authServerUrl,
                tokenServerUrl,
                oauthRedirectUrl,
                clientId,
                clientSecret,
                jwksUrl,
                principalFiled,
                requiredIssuer,
                requiredAudience,
                connectWaitTimeout);
    }

    @Override
    public UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption)
            throws AuthenticationException {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setAuthPlugin(AuthPlugin.Server.AUTHENTICATION_OAUTH2.name());
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
        info.setTextForAuthPlugin(userAuthOption == null ? null : userAuthOption.getAuthString());
        return info;
    }

    @Override
    public void authenticate(String user, String host, byte[] password, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
    }

    @Override
    public byte[] authMoreDataPacket(String user, String host) throws AuthenticationException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] bytes = oAuth2Context.authServerUrl().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        bytes = oAuth2Context.clientId().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        bytes = oAuth2Context.oauthRedirectUrl().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        return outputStream.toByteArray();
    }

    public OAuth2Context getoAuth2Context() {
        return oAuth2Context;
    }
}