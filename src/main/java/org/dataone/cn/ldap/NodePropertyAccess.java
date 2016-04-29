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
import org.dataone.service.types.v2.Property;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.util.DateTimeMarshaller;

/**
 *
 * Provides Access to retrieve, remove and modify the LDAP Node Property Data Structure
 *
 * @author waltz
 */
public class NodePropertyAccess extends LDAPService {

    public static Log log = LogFactory.getLog(NodePropertyAccess.class);
    
    public static final String OBJECT_CLASS_ID = "d1NodeProperty";
    public static final String NODE_PROPERTY_ID = "d1NodePropertyId";
    public static final String NODE_PROPERTY_KEY = "d1NodePropertyKey";
    public static final String NODE_PROPERTY_VALUE = "d1NodePropertyValue";
    public static final String NODE_PROPERTY_TYPE = "d1NodePropertyType";

    public NodePropertyAccess() {
        // we need to use a different base for the ids
        this.setBase(Settings.getConfiguration().getString("nodeRegistry.ldap.base"));
    }


    /**
     * provide a nodeReference and Property to return a string that
     * should conform to distinguished name rules for
     * a node property entry in ldap
     * XXX As an after thought, this should be returning a DN structure
     * not a string!
     *
     * @param nodeReference
     * @param property
     * @return String of DN
     * 
     */
    protected String buildNodePropertyDN(NodeReference nodeReference, Property property) {
        String d1NodePropertyId = buildNodePropertyId(property);
        String propertyDN =  NODE_PROPERTY_ID + "=" + d1NodePropertyId + ",cn=" + nodeReference.getValue() + ",dc=dataone,dc=org";
        return propertyDN;
    }

    /**
     *  the string to identify a property is a combination of the property name and
     *  its version in order to differentiate it from other versions of the same name
     *
     *  The function will return a string such that name-version is the result
     *
     * @param property
     * @return String
     *
     */
    protected String buildNodePropertyId(Property property) {
        return property.getKey();
        //return property.getKey() + "-" + property.getValue();
    }

    /**
     * remove the Node Property from LDAP, note all other dependent structures
     * have to be removed before this is called or it will fail.
     *
     * @param nodeReference
     * @param property
     * @return Boolean
     * 
     */
    protected Boolean deleteNodeProperty(DirContext ctx, NodeReference nodeReference, Property property) {

        return super.removeEntry(ctx, buildNodePropertyDN(nodeReference, property));
    }

    /**
     * retrieve list of Node Properties from LDAP
     *
     * @param nodeIdentifier
     * @return List<Property>
     * @throws ServiceFailure
     * 
     */
    protected List<Property> getPropertyList(DirContext ctx, String nodeIdentifier) throws ServiceFailure {
        List<Property> allProperties = new ArrayList<Property>();
        try {
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            
            NamingEnumeration<SearchResult> results =
                    ctx.search(getBase(), String.format("(&(objectClass=%s)(%s=%s))",
                    OBJECT_CLASS_ID, NodeAccess.NODE_ID, nodeIdentifier), ctls);
                    
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

                allProperties.add(this.mapProperty(attributesMap));
            }
        } catch (CommunicationException ex) {
            log.error("LDAP Service is unresponsive " + nodeIdentifier, ex);
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
        } catch (Exception e) {
            System.err.print("[" + DateTimeMarshaller.serializeDateToUTC(new Date()) + "]  NodeId: " + nodeIdentifier + " ");
            e.printStackTrace();
            log.error("Problem search NodesProperties for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allProperties;
    }

    /**
     * from the provided Node and Service, fill in the Attributes that will be used
     * to create the Node Property entry in LDAP
     * ServiceMethodRestrictions are not included in this mapping
     *
     * @param node
     * @param property
     * @return Attributes
     * 
     */
    protected Attributes mapNodePropertyAttributes(Node node, Property property) {
        Attributes propertyAttributes = new BasicAttributes(true /* ignore attributeID case */);
        String NodePropertyId = buildNodePropertyId(property);
        propertyAttributes.put(new BasicAttribute("objectclass", OBJECT_CLASS_ID));
        propertyAttributes.put(new BasicAttribute(NODE_PROPERTY_ID, NodePropertyId));
        propertyAttributes.put(new BasicAttribute(NodeAccess.NODE_ID, node.getIdentifier().getValue()));

        propertyAttributes.put(new BasicAttribute(NODE_PROPERTY_KEY, property.getKey()));
        propertyAttributes.put(new BasicAttribute(NODE_PROPERTY_VALUE, property.getValue()));
        if (property.getType() != null) {
        	propertyAttributes.put(new BasicAttribute(NODE_PROPERTY_TYPE, property.getType()));
        }
        return propertyAttributes;
    }

    /**
     * from the provided attributeMap returned from an LDAP query,
     * fill out a Node Property object
     * ServiceMethodRestrictions are not included
     *
     * @param attributesMap
     * @return Property
     * 
     */
    private Property mapProperty(HashMap<String, String> attributesMap) {
        Property property = new Property();
        property.setKey(attributesMap.get(NODE_PROPERTY_KEY.toLowerCase()));
        property.setValue(attributesMap.get(NODE_PROPERTY_VALUE.toLowerCase()));
        property.setType(attributesMap.get(NODE_PROPERTY_TYPE.toLowerCase()));
        return property;
    }
 
    
}
