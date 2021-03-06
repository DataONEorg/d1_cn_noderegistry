/**
 * This work was created by participants in the DataONE project, and is jointly copyrighted by participating
 * institutions in DataONE. For more information on DataONE, see our web site at http://dataone.org.
 *
 * Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * $Id$
 */
package org.dataone.service.cn.v2.impl;

import org.dataone.service.cn.v2.NodeRegistryService;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.ldap.NodeFacade;
import org.dataone.cn.quartz.CronExpression;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

/**
 * Though this is an implementation based on the CNRegister interface. The methods do not conform to the CNRegister
 * interface definitions (yet) The CNRegister interface should be implemented and all business logic maintained in this
 * class. However, the current business logic requires a dependency on the d1_identity_manager component. Since
 * d1_identity_manager already depends on this class and therefore this component, the business logic is separated into
 * the controller for the time being so as not to create a circular dependency between components (XXX should merge
 * d1_identity_manager and d1_cn_noderegistry classes into the d1_cn_common component).
 *
 * The package also extends beyond the definition of CNRegister to include other public service methods, such as getNode
 * and listNodes, that relate to the NodeRegistry but are a part of other layers of the CN
 *
 * Lastly, deleteNode and acceptNode are not a part of any public definitions as of yet, but are provided as an
 * indication of future needs.
 *
 * This class is composed of Data Access Objects that serve to interact directly with LDAP store
 *
 * @author waltz
 */
public class NodeRegistryServiceImpl implements NodeRegistryService {

    public static Log log = LogFactory.getLog(NodeRegistryServiceImpl.class);
    private NodeFacade nodeFacade = new NodeFacade();


    static Pattern excludeNodeBaseURLPattern = Pattern
            .compile("^https?\\:\\/\\/(?:(?:localhost(?:\\:8080)?\\/)|(?:127\\.0)).+");
    static Pattern validNodeIdPattern = Pattern.compile(Settings.getConfiguration().getString(
            "cn.nodeId.validation"));
    private long lastNodelistRefreshTimeMS = 0;

    static final long NODELIST_REFRESH_INTERVAL_MS = Settings.getConfiguration().getLong("noderegistry.nodeListRefreshInterval", 3L) * 1000L;
    static final long NODE_REFRESH_INTERVAL_MS = Settings.getConfiguration().getLong("noderegistry.nodeRefreshInterval", 3L) * 1000L;
    ;
    NodeList nodeList = null;

    Map<NodeReference, Long> nodeRefreshDateMap = new ConcurrentHashMap<NodeReference, Long>();
    Map<NodeReference, Node> nodeCacheMap = new ConcurrentHashMap<NodeReference, Node>();
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
    public synchronized NodeList listNodes() throws NotImplemented, ServiceFailure {

        if ((nodeList == null) || this.isTimeForNodelistRefresh()) {
            nodeList = nodeFacade.getApprovedNodeList();

        }
        return nodeList;
    }

    @Override
    public NodeList listPendingNodes() throws NotImplemented, ServiceFailure, NotFound {
        NodeList pendingNodeList = new NodeList();

        List<NodeReference> pendingReferenceNodeList = nodeFacade.getPendingNodeReferenceList();
        for (NodeReference nodeReference : pendingReferenceNodeList){
            Node node = this.getNodeCapabilities(nodeReference);
            pendingNodeList.addNode(node);
            
        }
        return pendingNodeList;
    }
    /*
     * Retreive the capabilities of the specified node 
     * if it is registered on the Coordinating Node being called.
     * 
     * @author waltz
     * @param NodeReference The Node Identifier to be retreived
     * @return a DataONE Node
     * @throws ServiceFailure
     * @throws NotFound
     * 
     */
    @Override
    public synchronized Node getNodeCapabilities(NodeReference nodeReference) throws ServiceFailure, NotFound {

        Node node = null;
        if ( (!nodeCacheMap.containsKey(nodeReference))
                || this.isTimeForNodeRefresh(nodeReference)) {
                node = nodeFacade.getNode(nodeReference);

            nodeCacheMap.put(nodeReference, node);
        }
        node = nodeCacheMap.get(nodeReference);
        if (node == null) {
            throw new ServiceFailure("4803", "could not retrieve " + nodeReference.getValue() + " from nodeCacheMap");
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
        Map<String, String> nodeIds = nodeFacade.getNodeIdList();
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
            nodeFacade.createNode(node);
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
    @Override
    public boolean updateNodeCapabilities(NodeReference nodeid, Node node) throws NotImplemented,
            ServiceFailure, InvalidRequest, NotFound {
        Matcher httpPatternMatcher = excludeNodeBaseURLPattern.matcher(node.getBaseURL());
        if (httpPatternMatcher.find()) {
            throw new InvalidRequest("4823", "BaseURL may not point to localhost! "
                    + node.getBaseURL());
        }
        try {
            if (node.isSynchronize()) {
                validateSynchronizationSchedule(node);
            }
            nodeFacade.updateNode(node);
            this.setLastNodelistRefreshTimeMS(this.getLastNodelistRefreshTimeMS()-NODELIST_REFRESH_INTERVAL_MS);
            if (nodeRefreshDateMap.containsKey(nodeid)) {
                // force update on next refresh
                nodeRefreshDateMap.put(nodeid, nodeRefreshDateMap.get(nodeid) - NODE_REFRESH_INTERVAL_MS);
            }
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
    @Override
    public void deleteNode(NodeReference nodeReference) throws ServiceFailure {
            nodeFacade.deleteNode(nodeReference);
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
        nodeFacade.setNodeApproved(nodeReference, Boolean.TRUE);

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

    /**
     * determines if it is time to refresh the nodelist information cache.
     *
     * @return boolean. true if time to refresh
     */
    private Boolean isTimeForNodelistRefresh() {
        Date now = new Date();
        long nowMS = now.getTime();
        DateFormat df = DateFormat.getDateTimeInstance();
        df.format(now);

        if (nowMS - this.getLastNodelistRefreshTimeMS() > NODELIST_REFRESH_INTERVAL_MS) {
            this.setLastNodelistRefreshTimeMS(nowMS);
            log.info("  nodelist refresh: new cached time: " + df.format(now));
            return true;
        } else {
            return false;
        }
    }

    /**
     * determines if it is time to refresh the node information cache.
     *
     * @return boolean. true if time to refresh
     */
    private Boolean isTimeForNodeRefresh(NodeReference nodeReference) {
        Date now = new Date();
        Long nowMS = new Long(now.getTime());
        DateFormat df = DateFormat.getDateTimeInstance();
        df.format(now);
        if (!nodeRefreshDateMap.containsKey(nodeReference)) {
            nodeRefreshDateMap.put(nodeReference, nowMS);

            log.info("node initial refresh: new cached time: " + df.format(now));
            return true;
        } else if (nowMS - nodeRefreshDateMap.get(nodeReference) > NODELIST_REFRESH_INTERVAL_MS) {
            nodeRefreshDateMap.put(nodeReference, nowMS);

            log.info("node refresh: new cached time: " + df.format(now));
            return true;
        } else {
            return false;
        }
    }

    private long getLastNodelistRefreshTimeMS() {
        return lastNodelistRefreshTimeMS;
    }

    private synchronized void setLastNodelistRefreshTimeMS(long lastNodelistRefreshTimeMS) {
        this.lastNodelistRefreshTimeMS = lastNodelistRefreshTimeMS;
    }
}
