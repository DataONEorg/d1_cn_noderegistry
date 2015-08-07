/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.service.ldap.tests.v1;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.dataone.cn.ldap.LDAPService;
import org.dataone.configuration.Settings;

/**
 *
 * @author waltz
 */
public class LdapPopulationService extends LDAPService {

    public LdapPopulationService() {
        // we need to use a different base for the ids
        this.setBase(Settings.getConfiguration().getString("nodeRegistry.ldap.base"));
    }

    @Override
    public void setBase(String base) {
        this.base = base;
    }

    public void deleteTestNodesByName(String nodeReference) throws NamingException {
        DirContext ctx = getContext();
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        NamingEnumeration<SearchResult> nodeResults =
                ctx.search(base, "(&(objectClass=d1Node)(d1NodeId=" + nodeReference + "))", ctls);
        while (nodeResults != null && nodeResults.hasMore()) {
            SearchResult si = nodeResults.next();
            String nodeDn = si.getNameInNamespace();
            //Find all ServiceMethodRestrictions and remove
            NamingEnumeration<SearchResult> smrResults =
                    ctx.search(base, "(&(objectClass=d1ServiceMethodRestriction)(d1NodeId=" + nodeReference + "))", ctls);
            while (smrResults != null && smrResults.hasMore()) {
                SearchResult searchR = smrResults.next();
                String smrNodeDn = searchR.getNameInNamespace();
                removeEntry(smrNodeDn);
            }
            //Find all Services
            NamingEnumeration<SearchResult> serviceNodeResults =
                    ctx.search(base, "(&(objectClass=d1NodeService)(d1NodeId=" + nodeReference + "))", ctls);
            while (serviceNodeResults != null && serviceNodeResults.hasMore()) {
                SearchResult searchR = serviceNodeResults.next();
                String serviceNodeDn = searchR.getNameInNamespace();
                removeEntry(serviceNodeDn);
            }
           //Find all Properties
            NamingEnumeration<SearchResult> propertyNodeResults =
                    ctx.search(base, "(&(objectClass=d1NodeProperty)(d1NodeId=" + nodeReference + "))", ctls);
            while (propertyNodeResults != null && propertyNodeResults.hasMore()) {
                SearchResult searchR = propertyNodeResults.next();
                String propertyNodeDn = searchR.getNameInNamespace();
                removeEntry(propertyNodeDn);
            }
            
            removeEntry(nodeDn);

        }
    }
}
