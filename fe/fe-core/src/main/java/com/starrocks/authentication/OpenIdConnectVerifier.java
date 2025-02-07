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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;

public class OpenIdConnectVerifier {

    public static void verify(String oidcToken,
                              String userName,
                              String jwksUri,
                              String principalFiled,
                              String requiredIssuer,
                              String requiredAudience) throws AuthenticationException {
        try {
            JWKSet jwkSet = OpenIdConnectVerifier.getPublicKeyFromJWKS(jwksUri);
            JSONObject openIdConnectJson = new JSONObject(oidcToken);

            String accessToken = openIdConnectJson.getString("access_token");
            OpenIdConnectVerifier.verifyJWT(accessToken, jwkSet);

            String idToken = openIdConnectJson.getString("id_token");
            OpenIdConnectVerifier.verifyJWT(idToken, jwkSet);
            SignedJWT signedJWT = SignedJWT.parse(idToken);

            JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
            String jwtUserName = claims.getStringClaim(principalFiled);
            if (!jwtUserName.equalsIgnoreCase(userName)) {
                throw new AuthenticationException("Login name " + userName + " is not matched to user " + jwtUserName);
            }

            if (requiredIssuer != null && !requiredIssuer.isEmpty() && !requiredIssuer.equals(claims.getIssuer())) {
                throw new AuthenticationException("Issuer (iss) field " + claims.getIssuer() + " is invalid");
            }

            if (requiredAudience != null && !requiredAudience.isEmpty() && !claims.getAudience().contains(requiredAudience)) {
                throw new AuthenticationException("Audience (aud) field " + claims.getIssuer() + " is invalid");
            }
        } catch (ParseException | JOSEException | IOException e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    private static void verifyJWT(String jwt, JWKSet jwkSet) throws AuthenticationException, ParseException, JOSEException {
        SignedJWT signedJWT = SignedJWT.parse(jwt);
        String kid = signedJWT.getHeader().getKeyID();

        // Get public key by kid
        RSAPublicKey publicKey = jwkSet.getKeyByKeyId(kid).toRSAKey().toRSAPublicKey();
        if (publicKey == null) {
            throw new AuthenticationException("Cannot find public key for kid: " + kid);
        }

        // 使用公钥验证 JWT
        RSASSAVerifier verifier = new RSASSAVerifier(publicKey);
        if (!signedJWT.verify(verifier)) {
            throw new AuthenticationException("JWT with kid " + kid + " is invalid!");
        }
    }

    private static JWKSet getPublicKeyFromJWKS(String jwksUrl) throws IOException, ParseException {
        InputStream inputStream;
        if (jwksUrl.startsWith("file://")) {
            String filePath = jwksUrl.substring(7);
            inputStream = new FileInputStream(filePath);
        } else {
            inputStream = new URL(jwksUrl).openStream();
        }

        return JWKSet.load(inputStream);
    }
}
