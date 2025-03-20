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

package com.starrocks.mysql.privilege;

public class AuthPlugin {

    public enum Server {
        MYSQL_NATIVE_PASSWORD,
        AUTHENTICATION_LDAP_SIMPLE,
        AUTHENTICATION_OPENID_CONNECT,
        AUTHENTICATION_OAUTH2
    }

    public enum Client {
        MYSQL_NATIVE_PASSWORD,
        MYSQL_CLEAR_PASSWORD,
        AUTHENTICATION_OPENID_CONNECT_CLIENT,
        AUTHENTICATION_OAUTH2_CLIENT;

        @Override
        public String toString() {
            //In the MySQL protocol, the authPlugin passed by the client is in lowercase.
            return name().toLowerCase();
        }
    }

    public static String covertFromServerToClient(String serverPluginName) {
        if (serverPluginName.equalsIgnoreCase(Server.MYSQL_NATIVE_PASSWORD.toString())) {
            return Client.MYSQL_NATIVE_PASSWORD.toString();
        } else if (serverPluginName.equalsIgnoreCase(Server.AUTHENTICATION_LDAP_SIMPLE.toString())) {
            return Client.MYSQL_CLEAR_PASSWORD.toString();
        } else if (serverPluginName.equalsIgnoreCase(Server.AUTHENTICATION_OPENID_CONNECT.toString())) {
            return Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString();
        } else if (serverPluginName.equalsIgnoreCase(Server.AUTHENTICATION_OAUTH2.toString())) {
            return Client.AUTHENTICATION_OAUTH2_CLIENT.toString();
        }
        return null;
    }
}
