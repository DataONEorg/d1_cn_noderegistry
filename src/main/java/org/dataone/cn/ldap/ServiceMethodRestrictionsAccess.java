/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.ldap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.naming.CommunicationException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.security.auth.x500.X500Principal;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Subject;

/**
 *
 * Provides Access to retrieve, remove and modify the LDAP Node ServiceMethodRestrictions Data Structure
 *
 * @author waltz
 */
public class ServiceMethodRestrictionsAccess extends LDAPService {

    public static Log log = LogFactory.getLog(ServiceMethodRestrictionsAccess.class);
    private static NodeServicesAccess nodeServicesAccess = new NodeServicesAccess();

    public ServiceMethodRestrictionsAccess() {
        // we need to use a different base for the ids
        this.setBase(Settings.getConfiguration().getString("nodeRegistry.ldap.base"));
    }

    @Override
    public void setBase(String base) {
        this.base = base;
    }

    /**
     * provide a nodeReference, a Service and ServiceMethodRestriction
     * to return a string that should conform to distinguished name rules for
     * a node service method restriction entry in ldap
     * XXX As an after thought, this should be returning a DN structure
     * not a string!
     *
     * @param nodeReference
     * @param service
     * @param restrict
     * @return String of DN
     * 
     */
    public String buildServiceMethodRestrictionDN(NodeReference nodeReference, Service service, ServiceMethodRestriction restrict) {
        String d1NodeServiceId = nodeServicesAccess.buildNodeServiceId(service);
        String serviceMethodRestrictionDN = "d1ServiceMethodName=" + restrict.getMethodName() + ",d1NodeServiceId=" + d1NodeServiceId + ",cn=" + nodeReference.getValue() + ",dc=dataone,dc=org";
        return serviceMethodRestrictionDN;
    }

    /**
     * remove the Node Service Method Restriction from LDAP
     *
     * @param nodeReference
     * @param service
     * @param restrict
     * @return Boolean
     * 
     */
    public Boolean deleteServiceMethodRestriction(NodeReference nodeReference, Service service, ServiceMethodRestriction restrict) {

        return super.removeEntry(buildServiceMethodRestrictionDN(nodeReference, service, restrict));
    }

    /**
     * retrieve list of Node Service Method Restrictions from LDAP
     *
     * @param nodeIdentifier
     * @param serviceIdentifier
     * @return List<ServiceMethodRestriction>
     * @throws ServiceFailure
     * 
     */
    public List<ServiceMethodRestriction> getServiceMethodRestrictionList(String nodeIdentifier, String serviceIdentifier) throws ServiceFailure {
        List<ServiceMethodRestriction> serviceMethodRestrictionList = new ArrayList<ServiceMethodRestriction>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(this.base, "(&(&(objectClass=d1ServiceMethodRestriction)(d1NodeServiceId=" + serviceIdentifier + "))(d1NodeId=" + nodeIdentifier + "))", ctls);

            while (results != null && results.hasMore()) {
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();
                log.trace("Search result found for: " + nodeDn);

                //return dn;
                // or we could double check
                HashMap<String, NamingEnumeration> attributesMap = new HashMap<String, NamingEnumeration>();
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID().toLowerCase();
                    NamingEnumeration<?> attributeValue = attribute.getAll();
                    attributesMap.put(attributeName, attributeValue);
                }

                serviceMethodRestrictionList.add(this.mapServiceMethodRestriction(attributesMap));
            }
        } catch (CommunicationException ex) {
            log.error("LDAP Service is unreponsive " + nodeIdentifier, ex);
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            log.error("Problem search Nodes for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return serviceMethodRestrictionList;
    }

    /**
     * from the provided attributeMap returned from an LDAP query,
     * fill out a Node Service Method Restriction object
     *
     * @param attributesMap
     * @return ServiceMethodRestriction
     * @throws NamingException
     * 
     */
    public ServiceMethodRestriction mapServiceMethodRestriction(HashMap<String, NamingEnumeration> attributesMap) throws NamingException {
        ServiceMethodRestriction serviceMethodRestriction = new ServiceMethodRestriction();

        serviceMethodRestriction.setMethodName(getEnumerationValueString(attributesMap.get("d1servicemethodname")));

        if (attributesMap.containsKey("d1allowedsubject")) {
            List<Subject> subjectList = serviceMethodRestriction.getSubjectList();

            NamingEnumeration allowSubjects = attributesMap.get("d1allowedsubject");
            while (allowSubjects.hasMore()) {
                Subject allowSubject = new Subject();
                X500Principal principal = new X500Principal((String) allowSubjects.next());
                String standardizedName = principal.getName(X500Principal.RFC2253);
                allowSubject.setValue(standardizedName);
                subjectList.add(allowSubject);
            }
        }

        return serviceMethodRestriction;
    }

    /**
     * from the provided Node, Service and ServiceMethodRestriction,
     * fill in the Attributes that will be used
     * to create the Node Service Method Restriction entry in LDAP
     *
     * @param node
     * @param service
     * @param restrict 
     * @return Attributes
     * 
     */
    public Attributes mapServiceMethodRestrictionAttributes(Node node, Service service, ServiceMethodRestriction restrict) {
        Attributes serviceAttributes = new BasicAttributes();
        String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);
        serviceAttributes.put(new BasicAttribute("objectclass", "d1ServiceMethodRestriction"));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceId", nodeServiceId));
        serviceAttributes.put(new BasicAttribute("d1NodeId", node.getIdentifier().getValue()));

        serviceAttributes.put(new BasicAttribute("d1ServiceMethodName", restrict.getMethodName()));
        if (restrict.getSubjectList() != null && !(restrict.getSubjectList().isEmpty())) {
            for (Subject subject : restrict.getSubjectList()) {
                serviceAttributes.put(new BasicAttribute("d1AllowedSubject", subject.getValue()));
            }
        }
        return serviceAttributes;
    }
}
