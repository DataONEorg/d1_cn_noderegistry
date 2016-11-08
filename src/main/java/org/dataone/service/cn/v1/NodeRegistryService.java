/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.service.cn.v1;

import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;

/**
 *
 * @author waltz
 */
public interface NodeRegistryService {

    void approveNode(NodeReference nodeReference) throws ServiceFailure;

    void deleteNode(NodeReference nodeReference) throws ServiceFailure;
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

    Node getNodeCapabilities(NodeReference nodeIdentifier) throws ServiceFailure, NotFound;

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
    NodeList listNodes() throws NotImplemented, ServiceFailure;

    /*
     * Retreive a list of nodes that have been registered but not approved
     * with the DataONE infrastructure.
     *
     * @author waltz
     * @return a DataONE NodeList
     * @throws ServiceFailure
     * @throws NotImplemented
     *
     */
    NodeList listPendingNodes() throws NotImplemented, ServiceFailure, NotFound;
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
    NodeReference register(Node node) throws ServiceFailure, InvalidRequest, IdentifierNotUnique, NotImplemented;
    /*
     * Update any attribute of a node with the exception of the Node Identifier
     *
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

    boolean updateNodeCapabilities(NodeReference nodeid, Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound;
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
    
}