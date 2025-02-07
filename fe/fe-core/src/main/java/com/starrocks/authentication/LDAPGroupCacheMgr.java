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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.starrocks.common.util.FrontendDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

public class LDAPGroupCacheMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(LDAPGroupCacheMgr.class);
    private static final long DEFAULT_RUN_INTERVAL_MS = 10000;
    private static final String SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES = "groupOfNames";
    private static final String SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES = "groupOfUniqueNames";
    private static final String SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP = "posixGroup";

    /**
     * For microsoft active directory service.
     */
    private static final String SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP = "group";
    private static final Set<String> SUPPORTED_LDAP_GROUP_TYPES = new HashSet<>(Arrays.asList(
            SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES,
            SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES,
            SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP,
            SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP));

    private final String name;
    private final Map<String, String> properties;
    private final Map<String, List<String>> memberToGroups;

    public LDAPGroupCacheMgr(String name, Map<String, String> properties) {
        super("LDAPGroupCacheMgr", DEFAULT_RUN_INTERVAL_MS);
        this.name = name;
        this.properties = properties;
        this.memberToGroups = new HashMap<>();
    }

    public List<String> getGroups(String username) {
        return memberToGroups.get(username);
    }

    @Override
    public void runAfterCatalogReady() {
        refreshGroupCache(false);
    }

    public synchronized void refreshGroupCache(boolean force) {
        String groupDNList = properties.get("group_dn_list");
        String ldapUserGroupMatchAttr = properties.get("ldap_user_group_match_attr");
        boolean useMemberUid = Boolean.parseBoolean(properties.get("ldap_group_match_use_member_uid"));

        try {
            DirContext ctx = getContext();
            Set<String> groupSet = Arrays.stream(groupDNList.split(";")).map(String::trim).collect(Collectors.toSet());
            for (String groupDN : groupSet) {
                String groupType = getGroupType(ctx, groupDN);
                if (groupType == null) {
                    continue;
                }

                Set<String> memberNames = new HashSet<>();
                switch (groupType) {
                    case SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES:
                    case SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES: {
                        memberNames = getMemberNamesFromGroupOfNames(ctx, groupDN, ldapUserGroupMatchAttr);
                        break;
                    }
                    case SUPPORTED_LDAP_GROUP_TYPE_POSIX_GROUP: {
                        memberNames = getMemberNamesFromPosixGroup(ctx, groupDN);
                        break;
                    }
                    case SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP:
                        memberNames = getMemberNamesFromADGroup(ctx, groupDN, ldapUserGroupMatchAttr, useMemberUid);
                        break;
                    default:
                        LOG.warn("unsupported group objectClass for group '" +
                                groupDN + "' with class '" + groupType +
                                "', currently supported: " + SUPPORTED_LDAP_GROUP_TYPES);
                }

                memberNames.forEach(memberName ->
                        memberToGroups.computeIfAbsent(memberName, k -> new ArrayList<>()).add(groupDN));
            }
        } catch (NamingException e) {

        }
    }

    private Set<String> getMemberNamesFromGroupOfNames(DirContext ctx, String groupDN, String ldapGroupMatchAttr)
            throws NamingException {
        Set<String> memberNames = new HashSet<>();
        Attributes attrs = ctx.getAttributes(groupDN, new String[] {"member"});
        Attribute member = attrs.get("member");
        if (member == null) {
            LOG.warn("cannot find 'member' attribute from general group '{}'", groupDN);
            return memberNames;
        }
        NamingEnumeration<?> e = member.getAll();
        while (e.hasMore()) {
            String memberDN = (String) e.next();
            // recursively get all the members of subgroup
            // `memberDN` may or may not be a supported group
            String groupType = getGroupType(ctx, memberDN);
            if (Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_NAMES) ||
                    Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_GROUP_OF_UNIQUE_NAMES)) {
                memberNames.addAll(getMemberNamesFromGroupOfNames(ctx, memberDN, ldapGroupMatchAttr));
            } else {
                String name = retrieveMemberNameFromDn(memberDN, ldapGroupMatchAttr);
                if (!Strings.isNullOrEmpty(name)) {
                    memberNames.add(name);
                }
            }
        }
        return memberNames;
    }

    private Set<String> getMemberNamesFromPosixGroup(DirContext ctx, String groupDN) throws NamingException {
        Set<String> memberNames = new HashSet<>();
        Attributes attrs = ctx.getAttributes(groupDN, new String[] {"memberUid"});
        Attribute memberUid = attrs.get("memberUid");
        if (memberUid == null) {
            LOG.warn("cannot find 'memberUid' attribute from posix group '{}'", groupDN);
            return memberNames;
        }
        NamingEnumeration<?> e = memberUid.getAll();
        while (e.hasMore()) {
            String memberUidValue = (String) e.next();
            memberNames.add(memberUidValue);
        }
        return memberNames;
    }

    private Set<String> getMemberNamesFromADGroup(DirContext ctx,
                                                  String groupDN,
                                                  String ldapGroupMatchAttr,
                                                  boolean ldapGroupMatchUseMemberUid) throws NamingException {
        LOG.info("getting member names from AD group '{}'", groupDN);
        Set<String> memberNames = new HashSet<>();
        // check whether `memberUid` attribute is present or not.
        Attributes attrs = ctx.getAttributes(groupDN, new String[] {"memberUid", "member"});
        Attribute memberUid = attrs.get("memberUid");
        boolean memberRetrievedFromUid = false;
        if (ldapGroupMatchUseMemberUid && memberUid != null && memberUid.size() > 0) {
            // If present, we will use `memberUid` attribute to get the members of the group directly,
            // otherwise we will retrieve the members of the group using `member` attribute.
            memberRetrievedFromUid = true;
            NamingEnumeration<?> e = memberUid.getAll();
            while (e.hasMore()) {
                String memberUidValue = (String) e.next();
                LOG.info("get memberUid: '{}' from AD group '{}'", memberUidValue, groupDN);
                memberNames.add(memberUidValue);
            }
        }

        Attribute member = attrs.get("member");
        if (member == null) {
            LOG.warn("cannot find 'member' attribute from ad group '{}'", groupDN);
            return memberNames;
        }
        NamingEnumeration<?> e = member.getAll();
        while (e.hasMore()) {
            String memberDN = (String) e.next();
            // recursively get all the members of subgroup
            // `memberDN` may or may not be a supported group
            String groupType = getGroupType(ctx, memberDN);
            if (Objects.equals(groupType, SUPPORTED_LDAP_GROUP_TYPE_AD_GROUP)) {
                LOG.info("found sub AD group '{}' from '{}'", memberDN, groupDN);
                memberNames.addAll(getMemberNamesFromADGroup(ctx, memberDN,
                        ldapGroupMatchAttr, ldapGroupMatchUseMemberUid));
            } else if (!memberRetrievedFromUid) {
                String name = retrieveMemberNameFromDn(memberDN, ldapGroupMatchAttr);
                if (!Strings.isNullOrEmpty(name)) {
                    memberNames.add(name);
                }
            }
        }

        return memberNames;
    }

    private String getGroupType(DirContext ctx, String groupDN) throws NamingException {
        Attributes attrs;
        try {
            attrs = ctx.getAttributes(groupDN);
        } catch (NameNotFoundException e) {
            // For non-existed group, ignore it
            return null;
        }
        if (attrs == null) {
            return null;
        }
        Attribute objectClass = attrs.get("objectClass");
        if (objectClass == null) {
            LOG.warn("cannot find 'objectClass' attribute from group '{}'", groupDN);
            return null;
        }
        NamingEnumeration<?> e = objectClass.getAll();
        String objectClassName = null;
        while (e.hasMore()) {
            objectClassName = (String) e.next();
            if (SUPPORTED_LDAP_GROUP_TYPES.contains(objectClassName)) {
                return objectClassName;
            }
        }
        return objectClassName;
    }

    @VisibleForTesting
    public String retrieveMemberNameFromDn(String memberDn, String ldapGroupMatchAttr) {
        boolean usingRegex = ldapGroupMatchAttr.startsWith("regex:");
        String[] splits = memberDn.split(",\\s*");
        if (usingRegex) {
            String regex = ldapGroupMatchAttr.substring(ldapGroupMatchAttr.indexOf(":") + 1);
            Pattern p = Pattern.compile(regex);
            for (String split : splits) {
                Matcher m = p.matcher(split);
                if (m.find()) {
                    if (m.groupCount() != 1) {
                        LOG.warn("invalid regex pattern: '{}', no matched group found", regex);
                        continue;
                    }
                    String matchedName = m.group(1);
                    LOG.info("found regex matched member name '{}' from member '{}'", matchedName, memberDn);
                    return matchedName;
                }
            }
        } else {
            for (String split : splits) {
                if (split.startsWith(ldapGroupMatchAttr + "=")) {
                    String matchedName;
                    try {
                        matchedName = split.substring(split.indexOf("=") + 1);
                    } catch (IndexOutOfBoundsException e) {
                        LOG.warn("invalid member name format: '{}', msg: {}", memberDn, e.getMessage());
                        continue;
                    }
                    LOG.info("found matched member name '{}' from member '{}'", matchedName, memberDn);
                    return matchedName;
                }
            }
        }

        return null;
    }

    private DirContext getContext() throws NamingException {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(Context.SECURITY_CREDENTIALS, "ldap_bind_root_pwd");
        environment.put(Context.SECURITY_PRINCIPAL, "ldap_bind_root_dn");
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, "ldap://localhost:10389");
        environment.put("com.sun.jndi.ldap.connect.timeout", "30000");
        environment.put("com.sun.jndi.ldap.read.timeout", "30000");
        DirContext dirContext = new InitialDirContext(environment);

        return dirContext;
    }
}
