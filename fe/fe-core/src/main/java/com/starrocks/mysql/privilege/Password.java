// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.mysql.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.directory.api.util.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Password implements Writable {
    private static final Logger LOG = LogManager.getLogger(Password.class);

    //password is encrypted
    @SerializedName(value = "password")
    private byte[] password;
    @SerializedName(value = "authPlugin")
    private AuthPlugin authPlugin;
    @SerializedName(value = "userForAuthPlugin")
    private String userForAuthPlugin;

    public Password(byte[] password, AuthPlugin authPlugin, String userForAuthPlugin) {
        this.password = password;
        this.authPlugin = authPlugin;
        this.userForAuthPlugin = userForAuthPlugin;
    }

    public Password(byte[] password) {
        this(password, null, null);
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public AuthPlugin getAuthPlugin() {
        return authPlugin;
    }

    public void setAuthPlugin(AuthPlugin authPlugin) {
        this.authPlugin = authPlugin;
    }

    public String getUserForAuthPlugin() {
        return userForAuthPlugin;
    }

    public void setUserForAuthPlugin(String userForAuthPlugin) {
        this.userForAuthPlugin = userForAuthPlugin;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String jsonStr = GsonUtils.GSON.toJson(this);
        Text.writeString(out, jsonStr);
    }

    public static Password read(DataInput in) throws IOException {
        String jsonStr = Text.readString(in);
        return GsonUtils.GSON.fromJson(jsonStr, Password.class);
    }

    public boolean check(String remoteUser, byte[] remotePassword, byte[] randomString) {
        if (remoteUser.contains(":")) {
            String[] list = remoteUser.split(":");
            remoteUser = list[list.length - 1];
        }

        if (authPlugin == null || authPlugin == AuthPlugin.MYSQL_NATIVE_PASSWORD) {
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(password);
            if (saltPassword.length != remotePassword.length) {
                return false;
            }

            if (remotePassword.length == 0) {
                return true;
            }

            return MysqlPassword.checkScramble(remotePassword, randomString, saltPassword);
        } else if (authPlugin == AuthPlugin.AUTHENTICATION_LDAP_SIMPLE) {
            //clear password is a null terminate string
            byte[] clearPassword = remotePassword;
            if (remotePassword[remotePassword.length - 1] == 0) {
                clearPassword = Arrays.copyOf(remotePassword, remotePassword.length - 1);
            }
            if (Strings.isNotEmpty(userForAuthPlugin)) {
                return LdapSecurity.checkPassword(userForAuthPlugin, new String(clearPassword, StandardCharsets.UTF_8));
            } else {
                return LdapSecurity.checkPasswordByRoot(remoteUser, new String(clearPassword, StandardCharsets.UTF_8));
            }
        } else {
            LOG.warn("unknown auth plugin {} to check password", authPlugin);
            return false;
        }
    }

    public boolean checkPlain(String remoteUser, String remotePassword) {
        if (remoteUser.contains(":")) {
            String[] list = remoteUser.split(":");
            remoteUser = list[list.length - 1];
        }

        if (authPlugin == null || authPlugin == AuthPlugin.MYSQL_NATIVE_PASSWORD) {
            return MysqlPassword.checkPlainPass(password, remotePassword);
        } else if (authPlugin == AuthPlugin.AUTHENTICATION_LDAP_SIMPLE) {
            if (Strings.isNotEmpty(userForAuthPlugin)) {
                return LdapSecurity.checkPassword(userForAuthPlugin, remotePassword);
            } else {
                return LdapSecurity.checkPasswordByRoot(remoteUser, remotePassword);
            }
        } else {
            LOG.warn("unknown auth plugin {} to check password", authPlugin);
            return false;
        }
    }
}
