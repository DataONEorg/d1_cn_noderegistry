/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.ldap;

import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * Expose public access to protected methods in NodeAccess, NodePropertyAccess, NodeServicesAccess and
 * ServiceMethodRestrictionsAccess
 *
 * The public methods will also control the borrowing and returning of LDAPContexts to the LDAP Pool
 *
 * @author waltz
 */
public class NodeFacade {

    public static Log log = LogFactory.getLog(NodeFacade.class);
    private static NodeAccess nodeAccess = new NodeAccess();
    private static NodeServicesAccess nodeServicesAccess = new NodeServicesAccess();
    private static NodePropertyAccess nodePropertyAccess = new NodePropertyAccess();
    private static ServiceMethodRestrictionsAccess serviceMethodRestrictionsAccess = new ServiceMethodRestrictionsAccess();
    private static DirContextProvider dirContextProvider = DirContextProvider.getInstance();
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

    public NodeList getApprovedNodeList() throws NotImplemented, ServiceFailure {
        DirContext dirContext = null;
        NodeList nodeList = null;
        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("4801", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("4801", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            nodeList = new NodeList();
            List<Node> allNodes = nodeAccess.getApprovedNodeList(dirContext);
            if (log.isDebugEnabled())
                    log.debug("found " + allNodes.size() + " nodes");
            for (Node node : allNodes) {

                String nodeIdentifier = node.getIdentifier().getValue();
                if (log.isTraceEnabled())
                    log.trace(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL());
                List<Service> serviceList = nodeServicesAccess.getServiceList(dirContext, nodeIdentifier);
                if (!serviceList.isEmpty()) {
                    for (Service service : serviceList) {
                        String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);

                        if (log.isTraceEnabled()) 
                            log.trace("\t has service " + nodeServiceId);
                        List<ServiceMethodRestriction> restrictionList = serviceMethodRestrictionsAccess
                                .getServiceMethodRestrictionList(dirContext, nodeIdentifier, nodeServiceId);
                        if (log.isTraceEnabled())
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
                List<Property> propertyList = nodePropertyAccess.getPropertyList(dirContext, nodeIdentifier);
                node.setPropertyList(propertyList);

            }
            nodeList.setNodeList(allNodes);

        } finally {
            dirContextProvider.returnDirContext(dirContext);
        }
        return nodeList;

    }

    public List<NodeReference> getPendingNodeReferenceList() throws ServiceFailure {
        DirContext dirContext = null;
        List<NodeReference> pendingNodeReferenceList = null;
        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("14800", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("14800", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            pendingNodeReferenceList = nodeAccess.getPendingNodeReferenceList(dirContext);
        } finally {
            dirContextProvider.returnDirContext(dirContext);
        }
        return pendingNodeReferenceList;
    }
    /*
     * Retreive a node that has been registered, but not necessarily approved,
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
        DirContext dirContext = null;

        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("4822", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("4822", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        Node node = null;
        try {
            try {
                node = nodeAccess.getNode(dirContext, nodeReference);
            } catch (NameNotFoundException ex) {
                log.warn("Node not found: " + nodeReference.getValue());
                throw new NotFound("4824", ex.getMessage());
            } catch (NamingException ex) {
                throw new ServiceFailure("4824", ex.getMessage());
            }

            if (log.isDebugEnabled())
                log.debug(nodeReference.getValue() + " " + node.getName() + " " + node.getBaseURL() + " "
                    + node.getBaseURL());
            List<Service> serviceList = nodeServicesAccess.getServiceList(dirContext, nodeReference.getValue());
            if (!serviceList.isEmpty()) {
                for (Service service : serviceList) {
                    String nodeServiceId = nodeServicesAccess.buildNodeServiceId(service);

                    service.setRestrictionList(serviceMethodRestrictionsAccess
                            .getServiceMethodRestrictionList(dirContext, node.getIdentifier().getValue(),
                                    nodeServiceId));
                }
                Services services = new Services();
                services.setServiceList(serviceList);
                node.setServices(services);
            }
            // set the property list
            List<Property> propertyList = nodePropertyAccess.getPropertyList(dirContext, nodeReference.getValue());
            node.setPropertyList(propertyList);
        } finally {
            dirContextProvider.returnDirContext(dirContext);
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
     * @author waltz
     * @param Node The Node to be registered
     * @return The generated Node Identifier for the newly registered Node
     * @throws ServiceFailure
     * @throws IdentifierNotUnique
     * 
     */
    public void createNode(Node node) throws ServiceFailure, InvalidRequest,
            IdentifierNotUnique, NotImplemented, NotFound {
        DirContext dirContext = null;

        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("4842", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("4842", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            nodeAccess.createNode(dirContext, node);
        } finally {
            dirContextProvider.returnDirContext(dirContext);
        }
    }

    /*
     * Update any attribute of a node with the exception of the Node Identifier
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
    public void updateNode(Node node) throws NotImplemented,
            ServiceFailure, InvalidRequest, NotFound {
        DirContext dirContext = null;

        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("4822", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("4822", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            nodeAccess.updateNode(dirContext, node);
        } finally {
            dirContextProvider.returnDirContext(dirContext);
        }

    }

    /*
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
        DirContext dirContext = null;

        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("14801", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("14801", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            if (!nodeAccess.getNodeApproved(dirContext, nodeReference)) {
                List<Service> services = nodeServicesAccess.getServiceList(dirContext, nodeReference.getValue());
                if ((services != null) && (services.size() > 0)) {
                    for (Service service : services) {
                        if (log.isDebugEnabled())
                            log.debug("deleteNode Service: " + service.getName());
                        List<ServiceMethodRestriction> serviceRestrictionList = serviceMethodRestrictionsAccess
                                .getServiceMethodRestrictionList(dirContext, nodeReference.getValue(),
                                        nodeServicesAccess.buildNodeServiceId(service));
                        if (serviceRestrictionList != null) {
                            for (ServiceMethodRestriction restriction : serviceRestrictionList) {
                                
                                if (log.isDebugEnabled())
                                    log.debug("deleteNode deleting " + serviceMethodRestrictionsAccess
                                        .buildServiceMethodRestrictionDN(nodeReference, service, restriction));
                                
                                if (!serviceMethodRestrictionsAccess.deleteServiceMethodRestriction(dirContext,
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
                        if (!nodeServicesAccess.deleteNodeService(dirContext, nodeReference, service)) {
                            throw new ServiceFailure("0", "Unable to delete service "
                                    + nodeServicesAccess.buildNodeServiceDN(nodeReference, service));
                        }
                    }
                }
                // delete the properties
                List<Property> propertyList = nodePropertyAccess.getPropertyList(dirContext, nodeReference
                        .getValue());
                if (propertyList != null && propertyList.size() > 0) {
                    for (Property property : propertyList) {
                        nodePropertyAccess.deleteNodeProperty(dirContext, nodeReference, property);
                    }
                }
                nodeAccess.deleteNode(dirContext, nodeReference);
            }
        } finally {
            dirContextProvider.returnDirContext(dirContext);
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
    public void setNodeApproved(NodeReference nodeReference, Boolean approved) throws ServiceFailure {
        DirContext dirContext = null;

        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("14802", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("14802", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            nodeAccess.setNodeApproved(dirContext, nodeReference, approved);
        } finally {
            dirContextProvider.returnDirContext(dirContext);
        }

    }

    public Map<String, String> getNodeIdList() throws ServiceFailure {
        DirContext dirContext = null;
        Map<String, String> nodeIdList = null;

        try {
            dirContext = dirContextProvider.borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("14803", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("14803", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }

        try {
            nodeIdList = nodeAccess.getNodeIdList(dirContext);
        } finally {
            dirContextProvider.returnDirContext(dirContext);
        }
        return nodeIdList;
    }

    protected static NodeAccess getNodeAccess() {
        return nodeAccess;
    }

    protected static DirContextProvider getDirContextProvider() {
        return dirContextProvider;
    }
    
    
}
