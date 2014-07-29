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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.naming.CommunicationException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.util.DateTimeMarshaller;

/**
 *
 * Provides Access to retrieve, remove and modify the LDAP NodeService Data Structure
 *
 * @author waltz
 */
public class NodeServicesAccess extends LDAPService {

    public static Log log = LogFactory.getLog(NodeServicesAccess.class);

    public NodeServicesAccess() {
        // we need to use a different base for the ids
        this.setBase(Settings.getConfiguration().getString("nodeRegistry.ldap.base"));
    }

    @Override
    public void setBase(String base) {
        this.base = base;
    }

    /**
     * provide a nodeReference and Service to return a string that
     * should conform to distinguished name rules for
     * a node service entry in ldap
     * XXX As an after thought, this should be returning a DN structure
     * not a string!
     *
     * @param nodeReference
     * @param service
     * @return String of DN
     * 
     */
    public String buildNodeServiceDN(NodeReference nodeReference, Service service) {
        String d1NodeServiceId = buildNodeServiceId(service);
        String serviceDN = "d1NodeServiceId=" + d1NodeServiceId + ",cn=" + nodeReference.getValue() + ",dc=dataone,dc=org";
        return serviceDN;
    }

    /**
     *  the string to identify a service is a combination of the service name and
     *  its version in order to differentiate it from other versions of the same name
     *
     *  The function will return a string such that name-version is the result
     *
     * @param service
     * @return String
     *
     */
    public String buildNodeServiceId(Service service) {
        return service.getName() + "-" + service.getVersion();
    }

    /**
     * remove the Node Service from LDAP, note all other dependent structures
     * have to be removed before this is called or it will fail.
     *
     * @param nodeReference
     * @param service
     * @return Boolean
     * 
     */
    public Boolean deleteNodeService(NodeReference nodeReference, Service service) {

        return super.removeEntry(buildNodeServiceDN(nodeReference, service));
    }

    /**
     * retrieve list of Node Services from LDAP
     *
     * @param nodeIdentifier
     * @return List<Service>
     * @throws ServiceFailure
     * 
     */
    public List<Service> getServiceList(String nodeIdentifier) throws ServiceFailure {
        List<Service> allServices = new ArrayList<Service>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            
            NamingEnumeration<SearchResult> results =
                    ctx.search(this.base, "(&(objectClass=d1NodeService)(d1NodeId=" + nodeIdentifier + "))", ctls);

            while (results != null && results.hasMore()) {
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();

                //return dn;
                // or we could double check
                HashMap<String, String> attributesMap = new HashMap<String, String>();
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID().toLowerCase();
                    String attributeValue = (String) attribute.get();
                    attributesMap.put(attributeName, attributeValue);
                }

                allServices.add(this.mapService(attributesMap));
            }
        } catch (CommunicationException ex) {
            log.error("LDAP Service is unreponsive " + nodeIdentifier, ex);
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            System.err.print("[" + DateTimeMarshaller.serializeDateToUTC(new Date()) + "]  NodeId: " + nodeIdentifier + " ");
            e.printStackTrace();
            log.error("Problem search NodesServices for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allServices;
    }

    /**
     * from the provided Node and Service, fill in the Attributes that will be used
     * to create the Node Service entry in LDAP
     * ServiceMethodRestrictions are not included in this mapping
     *
     * @param node
     * @param service
     * @return Attributes
     * 
     */
    public Attributes mapNodeServiceAttributes(Node node, Service service) {
        Attributes serviceAttributes = new BasicAttributes();
        String nodeServiceId = buildNodeServiceId(service);
        serviceAttributes.put(new BasicAttribute("objectclass", "d1NodeService"));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceId", nodeServiceId));
        serviceAttributes.put(new BasicAttribute("d1NodeId", node.getIdentifier().getValue()));

        serviceAttributes.put(new BasicAttribute("d1NodeServiceName", service.getName()));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceVersion", service.getVersion()));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceAvailable", Boolean.toString(service.getAvailable()).toUpperCase()));
        return serviceAttributes;
    }

    /**
     * from the provided attributeMap returned from an LDAP query,
     * fill out a Node Service object
     * ServiceMethodRestrictions are not included
     *
     * @param attributesMap
     * @return Service
     * 
     */
    public Service mapService(HashMap<String, String> attributesMap) {
        Service service = new Service();
        service.setName(attributesMap.get("d1nodeservicename"));
        service.setVersion(attributesMap.get("d1nodeserviceversion"));
        service.setAvailable(Boolean.valueOf(attributesMap.get("d1nodeserviceavailable")));
        return service;
    }
}
