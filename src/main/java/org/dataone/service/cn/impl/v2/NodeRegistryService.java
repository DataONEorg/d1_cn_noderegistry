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

package org.dataone.service.cn.impl.v2;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.cn.ldap.NodePropertyAccess;
import org.dataone.cn.ldap.NodeServicesAccess;
import org.dataone.cn.ldap.ServiceMethodRestrictionsAccess;
import org.dataone.cn.quartz.CronExpression;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v2.Property;

/**
 * Though this is an implementation based on the CNRegister interface.
 * The methods do not conform to the CNRegister interface definitions (yet)
 * The CNRegister interface should be implemented and all business logic
 * maintained in this class. However, the current business logic requires
 * a dependency on the d1_identity_manager component. Since d1_identity_manager
 * already depends on this class and therefore this component, the business
 * logic is separated into the controller for the time being so as not to
 * create a circular dependency between components (XXX should merge d1_identity_manager
 * and d1_cn_noderegistry classes into the d1_cn_common component).
 *
 * The package also extends beyond the definition of CNRegister
 * to include other public service methods, such as getNode and listNodes,
 * that relate to the NodeRegistry but are a part of other layers of the CN
 *
 * Lastly, deleteNode and acceptNode are not a part of any public definitions
 * as of yet, but are provided as an indication of future needs.
 *
 * This class is composed of Data Access Objects that serve to interact
 * directly with LDAP store
 * 
 * @author waltz
 */
public class NodeRegistryService {

    public static Log log = LogFactory.getLog(NodeRegistryService.class);
    private static NodeAccess nodeAccess = new NodeAccess();
    private static NodeServicesAccess nodeServicesAccess = new NodeServicesAccess();
    private static NodePropertyAccess nodePropertyAccess = new NodePropertyAccess();
    private static ServiceMethodRestrictionsAccess serviceMethodRestrictionsAccess = new ServiceMethodRestrictionsAccess();
    static Pattern excludeNodeBaseURLPattern = Pattern
            .compile("^https?\\:\\/\\/(?:(?:localhost(?:\\:8080)?\\/)|(?:127\\.0)).+");
    static Pattern validNodeIdPattern = Pattern.compile(Settings.getConfiguration().getString(
            "cn.nodeId.validation"));

    /*
     * Retreive a list of nodes that have been registered and approved
     * with the DataONE infrastructure.
     *
     * @author waltz
     * @return a DataONE NodeList
     * @throws ServiceFailure
     * @throws NotImplemented
     * 
     */
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        NodeList nodeList = new NodeList();

        List<Node> allNodes = nodeAccess.getApprovedNodeList();
        log.debug("found " + allNodes.size() + " nodes");
        for (Node node : allNodes) {

            String nodeIdentifier = node.getIdentifier().getValue();
            log.trace(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL());
            List<Service> serviceList = nodeServicesAccess.getServiceList(nodeIdentifier);
            if (!serviceList.isEmpty()) {
                for (Service service : serviceList) {
                    String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);
                    ;
                    log.trace("\t has service " + nodeServiceId);
                    List<ServiceMethodRestriction> restrictionList = serviceMethodRestrictionsAccess
                            .getServiceMethodRestrictionList(nodeIdentifier, nodeServiceId);
                    for (ServiceMethodRestriction restrict : restrictionList) {
                        log.trace("\t\t has restriction" + restrict.getMethodName());
                    }
                    service.setRestrictionList(restrictionList);
                }
                Services services = new Services();
                services.setServiceList(serviceList);
                node.setServices(services);
            }

            // set the property list
            List<Property> propertyList = nodePropertyAccess.getPropertyList(nodeIdentifier);
            node.setPropertyList(propertyList);

        }
        nodeList.setNodeList(allNodes);
        return nodeList;
    }

    public Set<NodeReference> getNodeReferences() {
        Set<NodeReference> nodeRefs = new HashSet<NodeReference>();
        try {
            for (Node node : listNodes().getNodeList()) {
                NodeReference nodeRef = new NodeReference();
                nodeRef.setValue(node.getIdentifier().getValue());
                nodeRefs.add(nodeRef);
            }
        } catch (NotImplemented ni) {
            log.error("Unable to get node list from node registry service", ni);
            ni.printStackTrace();
        } catch (ServiceFailure sf) {
            log.error("Unable to get node list from node registry service", sf);
            sf.printStackTrace();
        }
        return nodeRefs;
    }

    /*
     * Retreive a node that have been registered
     * within the DataONE infrastructure.
     *
     * @author waltz
     * @param NodeReference The Node Identifier to be retreived
     * @return a DataONE Node
     * @throws ServiceFailure
     * @throws NotFound
     * 
     */

    public Node getNode(NodeReference nodeReference) throws ServiceFailure, NotFound {

        Node node = null;
        try {
            node = nodeAccess.getNode(nodeReference);
        } catch (NameNotFoundException ex) {
            log.warn("Node not found: " + nodeReference.getValue());
            throw new NotFound("4842", ex.getMessage());
        } catch (NamingException ex) {
            throw new ServiceFailure("4842", ex.getMessage());
        }

        log.debug(nodeReference.getValue() + " " + node.getName() + " " + node.getBaseURL() + " "
                + node.getBaseURL());
        List<Service> serviceList = nodeServicesAccess.getServiceList(nodeReference.getValue());
        if (!serviceList.isEmpty()) {
            for (Service service : serviceList) {
                String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);

                service.setRestrictionList(serviceMethodRestrictionsAccess
                        .getServiceMethodRestrictionList(node.getIdentifier().getValue(),
                                nodeServiceId));
            }
            Services services = new Services();
            services.setServiceList(serviceList);
            node.setServices(services);

        }
        // set the property list
        List<Property> propertyList = nodePropertyAccess.getPropertyList(nodeReference.getValue());
        node.setPropertyList(propertyList);

        return node;
    }

    /*
     * Retreive a node that have been registered and approved
     * within the DataONE infrastructure.
     *
     * @author waltz
     * @param NodeReference The Node Identifier to be retreived
     * @return a DataONE Node
     * @throws ServiceFailure
     * @throws NotFound
     * 
     */

    public Node getApprovedNode(NodeReference nodeIdentifier) throws ServiceFailure, NotFound {

        Node node = nodeAccess.getApprovedNode(nodeIdentifier);

        log.debug(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " "
                + node.getBaseURL());
        List<Service> serviceList = nodeServicesAccess.getServiceList(nodeIdentifier.getValue());
        if (!serviceList.isEmpty()) {
            for (Service service : serviceList) {
                String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);

                service.setRestrictionList(serviceMethodRestrictionsAccess
                        .getServiceMethodRestrictionList(node.getIdentifier().getValue(),
                                nodeServiceId));
            }
            Services services = new Services();
            services.setServiceList(serviceList);
            node.setServices(services);
        }

        // set the property list
        List<Property> propertyList = nodePropertyAccess.getPropertyList(nodeIdentifier.getValue());
        node.setPropertyList(propertyList);

        return node;
    }

    /*
     * Create a new node in the system. The node will be marked as unapproved until
     * an Administrator confirms the registration is authentic. The returned NodeReference
     * will be the Identifier generated by the system for the Node.
     * 
     * If the node already exists, then an IdentifierNotUnique exception MUST be returned.
     *
     * @author waltz
     * @param Node The Node to be registered
     * @return The generated Node Identifier for the newly registered Node
     * @throws ServiceFailure
     * @throws IdentifierNotUnique
     * 
     */

    public NodeReference register(Node node) throws ServiceFailure, InvalidRequest,
            IdentifierNotUnique, NotImplemented {
        // do not allow localhost to be baseURL of a node
        Matcher httpPatternMatcher = excludeNodeBaseURLPattern.matcher(node.getBaseURL());
        if (httpPatternMatcher.find()) {
            throw new InvalidRequest("4823", "BaseURL may not point to localhost! "
                    + node.getBaseURL());
        }
        Matcher validNodeIdMatcher = validNodeIdPattern.matcher(node.getIdentifier().getValue());
        if (!validNodeIdMatcher.matches()) {
            throw new InvalidRequest("4823", "Problem registring "
                    + node.getIdentifier().getValue() + "-"
                    + Settings.getConfiguration().getString("cn.nodeId.validation.errorText"));
        }
        // validate that the node Id in the node structure is unique and conforms
        // to the naming convention rules and max length rules
        String newNodeId = node.getIdentifier().getValue();
        String newBaseUrl = node.getBaseURL();
        Map<String, String> nodeIds = nodeAccess.getNodeIdList();
        if (nodeIds.containsKey(newNodeId)) {
            IdentifierNotUnique ex = new IdentifierNotUnique("4844", node.getIdentifier()
                    .getValue() + " is not available for registration");
            throw ex;
        }
        if (nodeIds.containsValue(newBaseUrl)) {
            String offendingNodeId = "";
            for (String nodeId : nodeIds.keySet()) {
                if (nodeIds.get(nodeId).equals(newBaseUrl)) {
                    offendingNodeId = nodeId;
                    break;
                }
            }
            throw new InvalidRequest("4823", "BaseURL " + newBaseUrl + " used by another node "
                    + offendingNodeId);
        }

        try {
            if (node.isSynchronize()) {
                validateSynchronizationSchedule(node);
            }
            nodeAccess.createNode(node);
            return node.getIdentifier();
        } catch (ServiceFailure ex) {
            ex.setDetail_code("4842");
            throw ex;
        } catch (InvalidRequest ex) {
            ex.setDetail_code("4843");
            throw ex;
        } catch (NotImplemented ex) {
            ex.setDetail_code("4820");
            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("Problem registering node " + node.getName(), ex);
            throw new ServiceFailure("4842", ex.getMessage());
        }

    }

    /*
     * Create a new node in the system. The node will be marked as unapproved until
     * an Administrator confirms the registration is authentic. The returned NodeReference
     * will be the Identifier generated by the system for the Node.
     *
     * If the node already exists, then an IdentifierNotUnique exception MUST be returned.
     *
     * @author waltz
     * @param NodeReference The Node Identifier to be updated
     * @param Node The Node to be modified
     * @return true upon success
     * @throws NotImplemented
     * @throws ServiceFailure
     * @throws InvalidRequest
     * @throws NotFound
     * 
     */

    public boolean updateNodeCapabilities(NodeReference nodeid, Node node) throws NotImplemented,
            ServiceFailure, InvalidRequest, NotFound {
        try {
            if (node.isSynchronize()) {
                validateSynchronizationSchedule(node);
            }
            nodeAccess.updateNode(node);
            return true;
        } catch (ServiceFailure ex) {
            ex.setDetail_code("4822");
            throw ex;
        } catch (InvalidRequest ex) {
            ex.setDetail_code("4823");
            throw ex;
        } catch (NotFound ex) {
            ex.setDetail_code("4824");
            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("Problem registering node " + node.getName(), ex);
            throw new ServiceFailure("4842", ex.getMessage());
        }
    }

    /*
     * TODO: completed details of this function as a service representation
     *
     * Remove a registered node from the system. Only nodes marked as unapproved
     * should be removed. Only administrators should be able to execute.
     *
     * @author waltz
     * @param NodeReference The Node Identifier to be removed
     * @return true upon success (?)
     * @throws ServiceFailure
     * 
     */

    public void deleteNode(NodeReference nodeReference) throws ServiceFailure {
        if (!nodeAccess.getNodeApproved(nodeReference)) {
            List<Service> services = nodeServicesAccess.getServiceList(nodeReference.getValue());
            if ((services != null) && (services.size() > 0)) {
                for (Service service : services) {
                    log.debug("deleteNode Service: " + service.getName());
                    List<ServiceMethodRestriction> serviceRestrictionList = serviceMethodRestrictionsAccess
                            .getServiceMethodRestrictionList(nodeReference.getValue(),
                                    nodeServicesAccess.buildNodeServiceId(service));
                    if (serviceRestrictionList != null) {
                        for (ServiceMethodRestriction restriction : serviceRestrictionList) {
                            log.debug("deleteNode deleting "
                                    + serviceMethodRestrictionsAccess
                                            .buildServiceMethodRestrictionDN(nodeReference,
                                                    service, restriction));
                            if (!serviceMethodRestrictionsAccess.deleteServiceMethodRestriction(
                                    nodeReference, service, restriction)) {

                                throw new ServiceFailure(
                                        "0",
                                        "Unable to delete restriction "
                                                + serviceMethodRestrictionsAccess
                                                        .buildServiceMethodRestrictionDN(
                                                                nodeReference, service, restriction));
                            }
                        }
                    }
                    if (!nodeServicesAccess.deleteNodeService(nodeReference, service)) {
                        throw new ServiceFailure("0", "Unable to delete service "
                                + nodeServicesAccess.buildNodeServiceDN(nodeReference, service));
                    }
                }
            }
            // delete the properties
            List<Property> propertyList = nodePropertyAccess.getPropertyList(nodeReference
                    .getValue());
            if (propertyList != null && propertyList.size() > 0) {
                for (Property property : propertyList) {
                    nodePropertyAccess.deleteNodeProperty(nodeReference, property);
                }
            }
            nodeAccess.deleteNode(nodeReference);
        }
    }

    /*
     * TODO: completed details of this function as a service representation
     *
     * Approve a registered node of the system. Only administrators should be able to execute.
     * 
     * @author waltz
     * @param NodeReference The Node Identifier to be approved
     * @return true upon success (?)
     * @throws ServiceFailure
     * 
     */

    public void approveNode(NodeReference nodeReference) throws ServiceFailure {
        nodeAccess.setNodeApproved(nodeReference, Boolean.TRUE);

    }

    /*
     * There are certain rules that make for a valid schedule.
     * 
     * Currently, the only d1 restriction apart from the quartz 
     * specification is that seconds may only be a digit between 0 and 60. 
     * 
     * 
     * @author waltz
     * @see http://quartz-scheduler.org/api/2.1.0/org/quartz/CronExpression.html
     */
    private void validateSynchronizationSchedule(Node node) throws InvalidRequest {
        if (node.getSynchronization() == null) {
            throw new InvalidRequest(
                    "-1",
                    "If the node has synchronization attribute set to true, then the node should include the Synchronization element");
        }

        String seconds = node.getSynchronization().getSchedule().getSec().replace(" ", "");
        String minutes = node.getSynchronization().getSchedule().getMin().replace(" ", "");
        String hours = node.getSynchronization().getSchedule().getHour().replace(" ", "");
        String dayOfMonth = node.getSynchronization().getSchedule().getMday().replace(" ", "");
        String month = node.getSynchronization().getSchedule().getMon().replace(" ", "");
        String dayOfWeek = node.getSynchronization().getSchedule().getWday().replace(" ", "");
        String year = node.getSynchronization().getSchedule().getYear().replace(" ", "");
        try {
            Integer secondsInteger = Integer.parseInt(seconds);
            if (secondsInteger == null || secondsInteger < 0 || secondsInteger >= 60) {
                throw new InvalidRequest("-1", "seconds:" + seconds + " must be between 0 and 59");
            }
        } catch (NumberFormatException ex) {
            throw new InvalidRequest("-1", "seconds:" + seconds
                    + " must be an integer between 0 and 59");
        }
        String crontabExpression = seconds + " " + minutes + " " + hours + " " + dayOfMonth + " "
                + month + " " + dayOfWeek + " " + year;
        if (!CronExpression.isValidExpression(crontabExpression)) {
            throw new InvalidRequest("-1", "Not a valid synchronization schedule");
        }

    }

    public static NodeAccess getNodeAccess() {
        return nodeAccess;
    }

}
