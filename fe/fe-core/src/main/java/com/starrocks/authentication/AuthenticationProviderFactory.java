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

import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.AuthPlugin;
import org.json.JSONObject;

public class AuthenticationProviderFactory {
    private static final PlainPasswordAuthenticationProvider PLAIN_PASSWORD_AUTHENTICATION_PROVIDER =
            new PlainPasswordAuthenticationProvider();
    private static final LDAPAuthProviderForNative LDAP_AUTH_PROVIDER = new LDAPAuthProviderForNative();

    private AuthenticationProviderFactory() {
    }

    public static AuthenticationProvider create(String plugin, String authString) {
        AuthPlugin.Server authPlugin = AuthPlugin.Server.valueOf(plugin);
        switch (authPlugin) {
            case MYSQL_NATIVE_PASSWORD -> {
                return PLAIN_PASSWORD_AUTHENTICATION_PROVIDER;
            }

            case AUTHENTICATION_LDAP_SIMPLE -> {
                return LDAP_AUTH_PROVIDER;
            }

            case AUTHENTICATION_OPENID_CONNECT -> {
                JSONObject authStringJSON = new JSONObject(authString);

                String jwksUrl = authStringJSON.optString("jwks_url", Config.oidc_jwks_url);
                String principalFiled = authStringJSON.optString("principal_field", Config.oidc_principal_field);
                String requiredIssuer = authStringJSON.optString("required_issuer", Config.oidc_required_issuer);
                String requiredAudience = authStringJSON.optString("required_audience", Config.oidc_required_audience);

                return new OpenIdConnectAuthenticationProvider(jwksUrl, principalFiled, requiredIssuer, requiredAudience);
            }

            case AUTHENTICATION_OAUTH2 -> {
                JSONObject authStringJSON = new JSONObject(authString == null ? "{}" : authString);

                String oauth2AuthServerUrl = authStringJSON.optString("oauth2_auth_server_url", Config.oauth2_auth_server_url);
                String oauth2TokenServerUrl = authStringJSON.optString("oauth2_token_server_url", Config.oauth2_token_server_url);
                String oauth2RedirectUrl = authStringJSON.optString("oauth2_redirect_url", Config.oauth2_redirect_url);
                String oauth2ClientId = authStringJSON.optString("oauth2_client_id", Config.oauth2_client_id);
                String oauth2ClientSecret = authStringJSON.optString("oauth2_client_secret", Config.oauth2_client_secret);
                String oidcJwksUrl = authStringJSON.optString("oidc_jwks_url", Config.oidc_jwks_url);
                String oidcPrincipalField = authStringJSON.optString("oidc_principal_field", Config.oidc_principal_field);
                String oidcRequiredIssuer = authStringJSON.optString("oidc_required_issuer", Config.oidc_required_issuer);
                String oidcRequiredAudience = authStringJSON.optString("oidc_required_audience", Config.oidc_required_audience);
                Long oauthConnectWaitTimeout =
                        authStringJSON.optLong("oauth_connect_wait_timeout", Config.oauth_connect_wait_timeout);

                return new OAuth2AuthenticationProvider(new OAuth2Context(
                        oauth2AuthServerUrl,
                        oauth2TokenServerUrl,
                        oauth2RedirectUrl,
                        oauth2ClientId,
                        oauth2ClientSecret,
                        oidcJwksUrl,
                        oidcPrincipalField,
                        oidcRequiredIssuer,
                        oidcRequiredAudience,
                        oauthConnectWaitTimeout));
            }
        }

        return null;
    }
}
