/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.ldap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;

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
     * @author waltz
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
     * @author waltz
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
     * @author waltz
     */
    public List<Service> getServiceList(String nodeIdentifier) throws ServiceFailure {
        List<Service> allServices = new ArrayList<Service>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(base, "(&(objectClass=d1NodeService)(d1NodeId=" + nodeIdentifier + "))", ctls);

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
        } catch (Exception e) {
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
     * @author waltz
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
     * @author waltz
     */
    public Service mapService(HashMap<String, String> attributesMap) {
        Service service = new Service();
        service.setName(attributesMap.get("d1nodeservicename"));
        service.setVersion(attributesMap.get("d1nodeserviceversion"));
        service.setAvailable(Boolean.valueOf(attributesMap.get("d1nodeserviceavailable")));
        return service;
    }
}