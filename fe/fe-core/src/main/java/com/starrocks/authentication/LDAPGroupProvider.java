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

import com.starrocks.sql.ast.UserIdentity;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

public class LDAPGroupProvider extends GroupProvider {
    public static final String TYPE = "ldap";
    private String ldapBindBaseDn = "";

    public LDAPGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public List<String> getGroup(UserIdentity userIdentity) {
        try {
            Hashtable<String, String> environment = new Hashtable<>();
            environment.put(Context.SECURITY_CREDENTIALS, "ldap_bind_root_pwd");
            environment.put(Context.SECURITY_PRINCIPAL, "ldap_bind_root_dn");
            environment.put(Context.SECURITY_AUTHENTICATION, "simple");
            environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            environment.put(Context.PROVIDER_URL, "ldap://localhost:10389");
            environment.put("com.sun.jndi.ldap.connect.timeout", "30000");
            environment.put("com.sun.jndi.ldap.read.timeout", "30000");
            DirContext dirContext = new InitialDirContext(environment);

            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            NamingEnumeration<SearchResult> results =
                    dirContext.search(ldapBindBaseDn, "(uid=" + userIdentity.getUser() + ")", searchControls);

            SearchResult searchResult = results.next();
            Attributes attributes = searchResult.getAttributes();
            Attribute attribute = attributes.get("memberOf");
            NamingEnumeration<?> e = attribute.getAll();

            List<String> groups = new ArrayList<>();
            while (e.hasMore()) {
                String group = (String) e.next();
                groups.add(group);
            }

            return groups;
        } catch (Exception e) {
            return null;
        }
    }
}
