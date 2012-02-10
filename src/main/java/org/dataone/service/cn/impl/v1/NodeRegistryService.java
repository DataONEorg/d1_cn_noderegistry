/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.service.cn.impl.v1;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.cn.ldap.NodeServicesAccess;
import org.dataone.cn.ldap.ServiceMethodRestrictionsAccess;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Services;

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
    private static ServiceMethodRestrictionsAccess serviceMethodRestrictionsAccess = new ServiceMethodRestrictionsAccess();
    static final Pattern excludeNodeBaseURLPattern = Pattern.compile("^https?\\:\\/\\/(?:(?:localhost(?:\\:8080)?\\/)|(?:127\\.0)).+");
    static final Pattern validNodeIdPattern = Pattern.compile(Settings.getConfiguration().getString("cn.nodeId.validation"));

    /*
     * Retreive a list of nodes that have been registered and approved
     * with the DataONE infrastructure.
     *
     *
     * @return a DataONE NodeList
     * @throws ServiceFailure
     * @throws NotImplemented
     * @author waltz
     */
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        NodeList nodeList = new NodeList();

        List<Node> allNodes = nodeAccess.getApprovedNodeList();
        log.debug("found " + allNodes.size() + " nodes");
        for (Node node : allNodes) {

            String nodeIdentifier = node.getIdentifier().getValue();
            log.trace(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
            List<Service> serviceList = nodeServicesAccess.getServiceList(nodeIdentifier);
            if (!serviceList.isEmpty()) {
                for (Service service : serviceList) {
                    String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);
                    ;
                    log.trace("\t has service " + nodeServiceId);
                    List<ServiceMethodRestriction> restrictionList = serviceMethodRestrictionsAccess.getServiceMethodRestrictionList(nodeIdentifier, nodeServiceId);
                    for (ServiceMethodRestriction restrict : restrictionList) {
                        log.trace("\t\t has restriction" + restrict.getMethodName());
                    }
                    service.setRestrictionList(restrictionList);
                }
                Services services = new Services();
                services.setServiceList(serviceList);
                node.setServices(services);
            }
        }
        nodeList.setNodeList(allNodes);
        return nodeList;
    }
    /*
     * Retreive a node that have been registered
     * with the DataONE infrastructure.
     *
     * @param NodeReference The Node Identifier to be retreived
     * @return a DataONE Node
     * @throws ServiceFailure
     * @throws NotFound
     * @author waltz
     */

    public Node getNode(NodeReference nodeIdentifier) throws ServiceFailure, NotFound {

        String dnNodeIdentifier = nodeAccess.buildNodeDN(nodeIdentifier);
        Node node = null;
        try {
            node = nodeAccess.getNode(dnNodeIdentifier);
        } catch (NameNotFoundException ex) {
            log.warn("Node not found: " + nodeIdentifier.getValue());
            throw new NotFound("4842", ex.getMessage());
        } catch (NamingException ex) {
            throw new ServiceFailure("4842", ex.getMessage());
        }


        log.debug(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
        List<Service> serviceList = nodeServicesAccess.getServiceList(nodeIdentifier.getValue());
        if (!serviceList.isEmpty()) {
            for (Service service : serviceList) {
                String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);

                service.setRestrictionList(serviceMethodRestrictionsAccess.getServiceMethodRestrictionList(node.getIdentifier().getValue(), nodeServiceId));
            }
            Services services = new Services();
            services.setServiceList(serviceList);
            node.setServices(services);

        }

        return node;
    }
    /*
     * Create a new node in the system. The node will be marked as unapproved until
     * an Administrator confirms the registration is authentic. The returned NodeReference
     * will be the Identifier generated by the system for the Node.
     * 
     * If the node already exists, then an IdentifierNotUnique exception MUST be returned.
     *
     * @param Node The Node to be registered
     * @return The generated Node Identifier for the newly registered Node
     * @throws ServiceFailure
     * @throws IdentifierNotUnique
     * @author waltz
     */

    public NodeReference register(Node node) throws ServiceFailure, InvalidRequest, IdentifierNotUnique {
        // do not allow localhost to be baseURL of a node
        Matcher httpPatternMatcher = excludeNodeBaseURLPattern.matcher(node.getBaseURL());
        if (httpPatternMatcher.find()) {
            throw new InvalidRequest("4823", "BaseURL may not point to localhost! " + node.getBaseURL());
        }
        Matcher validNodeIdMatcher = validNodeIdPattern.matcher(node.getIdentifier().getValue());
        if (!validNodeIdMatcher.matches()) {
            throw new InvalidRequest("4823", Settings.getConfiguration().getString("cn.nodeId.validation.errorText"));
        }
        // validate that the node Id in the node structure is unique and conforms
        // to the naming convention rules and max length rules
        String newNodeId = node.getIdentifier().getValue();
        String newBaseUrl = node.getBaseURL();
        Map<String, String> nodeIds = nodeAccess.getNodeIdList();
        if (nodeIds.containsKey(newNodeId)) {
            IdentifierNotUnique ex = new IdentifierNotUnique("4844", node.getIdentifier().getValue() + " is not available for registration");
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
            throw new InvalidRequest("4823", "BaseURL " + newBaseUrl + " used by another node " + offendingNodeId);
        }

        try {

            nodeAccess.setNode(node);

            return node.getIdentifier();
        } catch (ServiceFailure ex) {
            throw ex;
        } catch (InvalidRequest ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("Problem registering node " + node.getName(), ex);
            throw new ServiceFailure("4801", ex.getMessage());
        }

    }
    /*
     * Create a new node in the system. The node will be marked as unapproved until
     * an Administrator confirms the registration is authentic. The returned NodeReference
     * will be the Identifier generated by the system for the Node.
     *
     * If the node already exists, then an IdentifierNotUnique exception MUST be returned.
     *
     * @param NodeReference The Node Identifier to be updated
     * @param Node The Node to be modified
     * @return true upon success
     * @throws NotImplemented
     * @throws ServiceFailure
     * @throws InvalidRequest
     * @throws NotFound
     * @author waltz
     */

    public boolean updateNodeCapabilities(NodeReference nodeid, Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
        nodeAccess.setNode(node);
        return true;
    }
    /*
     * TODO: completed details of this function as a service representation
     *
     * Remove a registered node from the system. Only nodes marked as unapproved
     * should be removed. Only administrators should be able to execute.
     *
     *
     * @param NodeReference The Node Identifier to be removed
     * @return true upon success (?)
     * @throws ServiceFailure
     * @author waltz
     */

    public void deleteNode(NodeReference nodeReference) throws ServiceFailure {
        if (!nodeAccess.getNodeApproved(nodeReference)) {
            List<Service> services = nodeServicesAccess.getServiceList(nodeReference.getValue());
            if ((services != null) && (services.size() > 0)) {
                for (Service service : services) {
                    log.debug("deleteNode Service: " + service.getName());
                    List<ServiceMethodRestriction> serviceRestrictionList = serviceMethodRestrictionsAccess.getServiceMethodRestrictionList(nodeReference.getValue(), nodeServicesAccess.buildNodeServiceId(service));
                    if (serviceRestrictionList != null) {
                        for (ServiceMethodRestriction restriction : serviceRestrictionList) {
                            log.debug("deleteNode deleting " + serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(nodeReference, service, restriction));
                            if (!serviceMethodRestrictionsAccess.deleteServiceMethodRestriction(nodeReference, service, restriction)) {

                                throw new ServiceFailure("0", "Unable to delete restriction " + serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(nodeReference, service, restriction));
                            }
                        }
                    }
                    if (!nodeServicesAccess.deleteNodeService(nodeReference, service)) {
                        throw new ServiceFailure("0", "Unable to delete service " + nodeServicesAccess.buildNodeServiceDN(nodeReference, service));
                    }
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
     * @param NodeReference The Node Identifier to be approved
     * @return true upon success (?)
     * @throws ServiceFailure
     * @author waltz
     */

    public void approveNode(NodeReference nodeReference) throws ServiceFailure {
        nodeAccess.setNodeApproved(nodeReference, Boolean.TRUE);

    }
}
