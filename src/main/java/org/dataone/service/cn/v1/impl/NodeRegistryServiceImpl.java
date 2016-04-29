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

package org.dataone.service.cn.v1.impl;

import org.dataone.service.cn.v1.NodeRegistryService;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.TypeFactory;

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
public class NodeRegistryServiceImpl implements NodeRegistryService {

    private org.dataone.service.cn.v2.impl.NodeRegistryServiceImpl impl = new org.dataone.service.cn.v2.impl.NodeRegistryServiceImpl();

    
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
    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        
        
    	NodeList nodeList = null;
        org.dataone.service.types.v2.NodeList nodes = impl.listNodes();
        
        // TODO: which is the better approach for serializing the structures?
        // can we just convert the NodeList without iterating?
        try {
        	nodeList = TypeFactory.convertTypeFromType(nodes, NodeList.class);
        } catch (Exception e) {
        	
        	// or do we need to iterate over the nodes and convert one by one?
        	nodeList = new NodeList();
        	for (org.dataone.service.types.v2.Node n: nodes.getNodeList()) {
        		Node currentNode = null;
    			try {
    				currentNode = TypeFactory.convertTypeFromType(n, Node.class);
    			} catch (Exception ee) {
    				throw new ServiceFailure("0000", ee.getMessage());
    			}
        		nodeList.addNode(currentNode);
        	}
		}

        return nodeList;
    }
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
    @Override
    public NodeList listPendingNodes() throws NotImplemented, ServiceFailure, NotFound {
        
        
    	NodeList nodeList = null;
        org.dataone.service.types.v2.NodeList nodes = impl.listPendingNodes();
        
        // TODO: which is the better approach for serializing the structures?
        // can we just convert the NodeList without iterating?
        try {
        	nodeList = TypeFactory.convertTypeFromType(nodes, NodeList.class);
        } catch (Exception e) {
        	
        	// or do we need to iterate over the nodes and convert one by one?
        	nodeList = new NodeList();
        	for (org.dataone.service.types.v2.Node n: nodes.getNodeList()) {
        		Node currentNode = null;
    			try {
    				currentNode = TypeFactory.convertTypeFromType(n, Node.class);
    			} catch (Exception ee) {
    				throw new ServiceFailure("0000", ee.getMessage());
    			}
        		nodeList.addNode(currentNode);
        	}
		}

        return nodeList;
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

    @Override
    public Node getNodeCapabilities(NodeReference nodeIdentifier) throws ServiceFailure, NotFound {

    	Node node = null;
    	try {
        	node = TypeFactory.convertTypeFromType(impl.getNodeCapabilities(nodeIdentifier), Node.class);
        } catch (Exception e) {
        	e.printStackTrace();
			throw new ServiceFailure("0000", e.getClass().getSimpleName() + ":" + e.getMessage());        	
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

    @Override
    public NodeReference register(Node node) throws ServiceFailure, InvalidRequest, IdentifierNotUnique, NotImplemented  {
    	org.dataone.service.types.v2.Node currentNode = null;
    	try {
    		currentNode = TypeFactory.convertTypeFromType(node, org.dataone.service.types.v2.Node.class);
        } catch (Exception e) {
			throw new ServiceFailure("0000", e.getMessage());        	
        }
		return impl.register(currentNode);

    }
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

    @Override
    public boolean updateNodeCapabilities(NodeReference nodeid, Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
    	org.dataone.service.types.v2.Node currentNode = null;
    	try {
    		currentNode = TypeFactory.convertTypeFromType(node, org.dataone.service.types.v2.Node.class);
        } catch (Exception e) {
        	e.printStackTrace();
			throw new ServiceFailure("0000", e.getClass().getSimpleName() + ":" + e.getMessage());        	
        }
		return impl.updateNodeCapabilities(nodeid, currentNode);
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

    @Override
    public void deleteNode(NodeReference nodeReference) throws ServiceFailure {
		impl.deleteNode(nodeReference);
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

    @Override
    public void approveNode(NodeReference nodeReference) throws ServiceFailure {
        impl.approveNode(nodeReference);

    }

}
