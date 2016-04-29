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
import org.dataone.cn.ldap.DirContextProvider;

import org.dataone.cn.ldap.LDAPService;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.ServiceFailure;

/**
 *
 * @author waltz
 */
public class LdapPopulationService extends LDAPService {

    public LdapPopulationService() {
        this.setBase(Settings.getConfiguration().getString("nodeRegistry.ldap.base"));
    }
private static DirContextProvider dirContextProvider = DirContextProvider.getInstance();
    public void deleteTestNodesByName(String nodeReference) throws NamingException, Exception {

        Boolean aggregateLogs = null;
  		DirContext dirContext = null;
		try {
			dirContext = dirContextProvider.borrowDirContext();
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
			throw new ServiceFailure("-5000", ex.getMessage());
		}
		if (dirContext == null) {
			throw new ServiceFailure("-5000", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again. Active Contexts: "+ dirContextProvider.getNumDirContextActive() + " Idle Contexts " + dirContextProvider.getNumDirContextIdle());
		}
 
        try {
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        NamingEnumeration<SearchResult> nodeResults =
                dirContext.search(getBase(), "(&(objectClass=d1Node)(d1NodeId=" + nodeReference + "))", ctls);
        while (nodeResults != null && nodeResults.hasMore()) {
            SearchResult si = nodeResults.next();
            String nodeDn = si.getNameInNamespace();
            //Find all ServiceMethodRestrictions and remove
            NamingEnumeration<SearchResult> smrResults =
                    dirContext.search(getBase(), "(&(objectClass=d1ServiceMethodRestriction)(d1NodeId=" + nodeReference + "))", ctls);
            while (smrResults != null && smrResults.hasMore()) {
                SearchResult searchR = smrResults.next();
                String smrNodeDn = searchR.getNameInNamespace();
                removeEntry(dirContext, smrNodeDn);
            }
            //Find all Services
            NamingEnumeration<SearchResult> serviceNodeResults =
                    dirContext.search(getBase(), "(&(objectClass=d1NodeService)(d1NodeId=" + nodeReference + "))", ctls);
            while (serviceNodeResults != null && serviceNodeResults.hasMore()) {
                SearchResult searchR = serviceNodeResults.next();
                String serviceNodeDn = searchR.getNameInNamespace();
                removeEntry(dirContext, serviceNodeDn);
            }
           //Find all Properties
            NamingEnumeration<SearchResult> propertyNodeResults =
                    dirContext.search(getBase(), "(&(objectClass=d1NodeProperty)(d1NodeId=" + nodeReference + "))", ctls);
            while (propertyNodeResults != null && propertyNodeResults.hasMore()) {
                SearchResult searchR = propertyNodeResults.next();
                String propertyNodeDn = searchR.getNameInNamespace();
                removeEntry(dirContext, propertyNodeDn);
            }
            
            removeEntry(dirContext, nodeDn);

        }
        }  finally {
            dirContextProvider.returnDirContext(dirContext);
        }
    }
}
