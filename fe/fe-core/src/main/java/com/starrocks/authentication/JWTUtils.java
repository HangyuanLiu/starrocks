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

import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.common.Config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;

public class JWTUtils {
    public static JWKSet getPublicKeyFromJWKS() throws IOException, ParseException {
        String url = Config.jwks_url;
        InputStream inputStream;
        if (url.startsWith("file://")) {
            // 如果是本地文件路径，去掉 "file://" 前缀
            String filePath = url.substring(7);
            inputStream = new FileInputStream(filePath);  // 返回 FileInputStream 来读取本地文件
        } else {
            inputStream = new URL(url).openStream();
        }

        return JWKSet.load(inputStream);
    }

    /**
     * 使用公钥验证 JWT
     */
    public static void verifyJWT(String jwt, JWKSet jwkSet) throws Exception {
        SignedJWT signedJWT = SignedJWT.parse(jwt);
        String kid = signedJWT.getHeader().getKeyID();

        // 查找适当的公钥
        RSAPublicKey publicKey = jwkSet.getKeyByKeyId(kid).toRSAKey().toRSAPublicKey();
        if (publicKey == null) {
            throw new Exception("Cannot find public key for kid: " + kid);
        }

        // 使用公钥验证 JWT
        RSASSAVerifier verifier = new RSASSAVerifier(publicKey);
        if (!signedJWT.verify(verifier)) {
            throw new Exception("JWT with kid " + kid + " is invalid!");
        }
    }
}
