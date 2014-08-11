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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.CommunicationException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.security.auth.x500.X500Principal;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeReplicationPolicy;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Ping;
import org.dataone.service.types.v1.Schedule;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.Synchronization;
import org.dataone.service.util.DateTimeMarshaller;

/**
 * Provides Access to retrieve, remove and modify the LDAP Node Data Structure
 *
 * @author waltz
 */
public class NodeAccess extends LDAPService {

    public static Log log = LogFactory.getLog(NodeAccess.class);
    public static final String ProcessingStateAttribute = "d1NodeProcessingState";
    public static final String LogLastAggregatedAttribute = "d1NodeLogLastAggregated";
    public static final String AggregateLogsAttribute = "d1NodeAggregateLogs";
    public static final String NodeApprovedAttribute = "d1NodeApproved";
    private static NodeServicesAccess nodeServicesAccess = new NodeServicesAccess();
    private static ServiceMethodRestrictionsAccess serviceMethodRestrictionsAccess = new ServiceMethodRestrictionsAccess();

    public NodeAccess() {
        // we need to use a different base for the ids
        this.setBase(Settings.getConfiguration().getString("nodeRegistry.ldap.base"));
    }

    @Override
    public void setBase(String base) {
        this.base = base;
    }

    /**
     * provide a nodeReference to return a string that
     * should conform to distinguished name rules for
     * a node entry in ldap
     * XXX As an after thought, this should be returning a DN structure
     * not a string!
     *
     * @param nodeReference
     * @return String of DN
     * 
     */
    public String buildNodeDN(NodeReference nodeReference) {
        return "cn=" + nodeReference.getValue() + ",dc=dataone,dc=org";
    }

    /**
     * provide a nodeReference and return a string that
     * should conform to distinguished name rules for
     * a node entry in ldap
     *
     * @param nodeDN Distinguished name of the DN, provided by buildNodeDN
     * @return String
     * @throws NamingException
     * 
     */
    public HashMap<String, NamingEnumeration> buildNodeAttributeMap(String nodeDN) throws NamingException {
        HashMap<String, NamingEnumeration> attributesMap = new HashMap<String, NamingEnumeration>();

        DirContext ctx = getContext();
        Attributes attributes = ctx.getAttributes(nodeDN);
        NamingEnumeration<? extends Attribute> values = attributes.getAll();
        while (values.hasMore()) {

            Attribute attribute = values.next();
            String attributeName = attribute.getID().toLowerCase();
            NamingEnumeration<?> attributeValue = attribute.getAll();
            attributesMap.put(attributeName, attributeValue);

        }

        return attributesMap;

    }

    /**
     * remove the node from LDAP, note all other dependent structures
     * have to be removed before this is called or it will fail.
     *
     * @param nodeReference
     * @return Boolean
     * @throws ServiceFailure
     * 
     */
    public Boolean deleteNode(NodeReference nodeReference) throws ServiceFailure {


        return super.removeEntry(buildNodeDN(nodeReference));

    }

    /**
     * retrieve list of approved NodeReference values from LDAP
     * (is this method used anywhere?)
     *
     * @return List<String>
     * @throws ServiceFailure
     * 
     */
    public List<String> getApprovedNodeIdList() throws ServiceFailure {
        List<String> allNodeIds = new ArrayList<String>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(base, "(&(objectClass=d1Node)(d1NodeApproved=TRUE))", ctls);

            while (results != null && results.hasMore()) {
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();
                log.trace("Search result found for: " + nodeDn);

                //return dn;
                // or we could double check
                HashMap<String, String> attributesMap = new HashMap<String, String>();
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID();
                    if (attributeName.equalsIgnoreCase("d1NodeId")) {
                        allNodeIds.add((String) attribute.get());
                    }
                }
            }
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem searching Approved Node Ids ", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allNodeIds;
    }

    /**
     * return a list of Nodes that are marked approved in LDAP
     *
     * @return List<Node>
     * @throws ServiceFailure
     * 
     */
    public List<Node> getApprovedNodeList() throws ServiceFailure {
        List<Node> allNode = new ArrayList<Node>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            log.trace("BASE: " + base);
            NamingEnumeration<SearchResult> results =
                    ctx.search(this.base, "(&(objectClass=d1Node)(d1NodeApproved=TRUE))", ctls);

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

                allNode.add(this.mapNode(attributesMap));
            }
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem searching Approved Nodes for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allNode;
    }

    /*
     * getCnLoggingStatus
     * 
     * get the status of logging aggregation on CNs.
     * each cn will have a state of the dataone processing such Offline, Recovery, or Active
     * each CN will also have a date associated with logging named LogLastAggregated.
     * the date and the state equals a logging status
     *
     * There is not a dataone type that currently maps or defines these values
     * They are not part of the public API or services or types
     * They are purely intended for log aggregation maintenance
     * dataone processing state is different from the state of the CN. the CN may be up
     * even though processing is Offline (if processing state is offline, synchronization
     * and replication may also be offline, so it is be more of a processing attribute in general)
     *
     *
     * @return Map<NodeReference, Map<String, String>>
     * @throws ServiceFailure
     * 
     */
    public Map<NodeReference, Map<String, String>> getCnLoggingStatus() throws ServiceFailure {
        Map<NodeReference, Map<String, String>> nodeLogStatus = new HashMap<NodeReference, Map<String, String>>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(base, "(&(objectClass=d1Node)(d1NodeType=cn))", ctls);

            while (results != null && results.hasMore()) {
                NodeReference nodeReference = new NodeReference();
                Map<String, String> nodeProperties = new HashMap<String, String>();
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();
                log.trace("Search result found for: " + nodeDn);

                //return dn;
                // or we could double check
                HashMap<String, String> attributesMap = new HashMap<String, String>();
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID();
                    if (attributeName.equalsIgnoreCase("d1NodeId")) {
                        nodeReference.setValue((String) attribute.get());
                    }
                    if (attributeName.equalsIgnoreCase(ProcessingStateAttribute)) {
                        nodeProperties.put(ProcessingStateAttribute, (String) attribute.get());
                    }
                    if (attributeName.equalsIgnoreCase(LogLastAggregatedAttribute)) {
                        nodeProperties.put(LogLastAggregatedAttribute, (String) attribute.get());
                    }
                }
                nodeLogStatus.put(nodeReference, nodeProperties);
            }
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem search Nodes for LoggingStatus", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return nodeLogStatus;

    }

    /**
     * return the date the last time a node was harvested for log records
     * the date is not the time the harvesting ran, but
     * the latest dateLogged from the LogEvent records harvested
     *
     * @param nodeReference
     * @return Date
     * @throws ServiceFailure
     * 
     */
    public Date getLogLastAggregated(NodeReference nodeIdentifier) throws ServiceFailure {
        Date logLastAggregated = null;
        try {
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey("d1nodeloglastaggregated")) {
                logLastAggregated = DateTimeMarshaller.deserializeDateToUTC(getEnumerationValueString(attributesMap.get("d1nodeloglastaggregated")));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem retrieving d1nodeloglastaggregated of " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could retrieve d1nodeloglastaggregated from: " + nodeIdentifier + " " + e.getMessage());
        }
        return logLastAggregated;
    }

    /**
     * retrieve a complete DataONE Node from LDAP.
     *
     * @param nodeDN Distinguished name of the DN, provided by buildNodeDN
     * @return Node
     * @throws NamingException
     * @throws NotFound
     * @throws NameNotFoundException
     * 
     */
    public Node getNode(String nodeDN) throws NotFound, NamingException, NameNotFoundException {

        HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(nodeDN);
        log.debug("Retrieved Node for: " + nodeDN);

        if (attributesMap.isEmpty()) {
            throw new NotFound("4801", nodeDN + " not found on the server");
        }

        return this.mapNode(attributesMap);


    }
    /**
     * determine if an Administrator has approved the node after it was registered,
     * Then return the node if it is approved, otherwise throw a NotFound exception
     *
     * @param nodeIdentifier
     * @return Boolean
     * @throws ServiceFailure
     * 
     */
    public Node getApprovedNode(String nodeIdentifier) throws ServiceFailure, NotFound {
        Boolean nodeApproved = false;
        HashMap<String, NamingEnumeration> attributesMap;
        try {
            attributesMap = buildNodeAttributeMap(nodeIdentifier);
        } catch (NameNotFoundException e) {
            throw new NotFound("4801", nodeIdentifier + " not found on the server");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("buildNodeAttributeMap- Problem determining approved state " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "buildNodeAttributeMap- Could not determine approved state of : " + nodeIdentifier + " " + e.getMessage());
        }
        if (attributesMap.isEmpty()) {
            throw new NotFound("4801", nodeIdentifier + " not found on the server");
        }
        if (attributesMap.containsKey(NodeApprovedAttribute.toLowerCase())) {
            try {
                nodeApproved = Boolean.valueOf(getEnumerationValueString(attributesMap.get(NodeApprovedAttribute.toLowerCase())));
            } catch (Exception e) {
                e.printStackTrace();
                log.error("getEnumerationValueString- Problem determining approved state " + nodeIdentifier, e);
                throw new ServiceFailure("4801", "getEnumerationValueString- Could not determine approved state of : " + nodeIdentifier + " " + e.getMessage());
            }
        } else {
            log.error("attributesMap.containsKey- Problem determining approved state " + nodeIdentifier);
            throw new ServiceFailure("4801", "attributesMap.containsKey- Could not determine approved state of : " + nodeIdentifier);   
        }


        if (!nodeApproved) {
            throw new NotFound("4801", nodeIdentifier + " not approved on the server");
        }
        try {
            return this.mapNode(attributesMap);
        } catch (NameNotFoundException ex) {
            log.warn("Node not found: " + nodeIdentifier);
            throw new NotFound("4842", ex.getMessage());
        } catch (NamingException ex) {
            throw new ServiceFailure("4842", ex.getMessage());
        }
    }
    /**
     * determine if an Administrator has approved the node after it was registered
     *
     * @param nodeIdentifier
     * @return Boolean
     * @throws ServiceFailure
     * 
     */
    public Boolean getNodeApproved(NodeReference nodeIdentifier) throws ServiceFailure {
        Boolean nodeApproved = false;
        try {
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(NodeApprovedAttribute.toLowerCase())) {
                String nodeApprovedStr = getEnumerationValueString(attributesMap.get(NodeApprovedAttribute.toLowerCase()));
                
                nodeApproved = Boolean.valueOf(nodeApprovedStr.toLowerCase());
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem determining approved state " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not determine approved state of : " + nodeIdentifier + " " + e.getMessage());
        }
        return nodeApproved;

    }

    /**
     * retrieve mapping of all NodeReference values from LDAP
     * to their baseUrls
     * this should contain both approved and non approved ids
     *
     * @return Map<String, String>
     * @throws ServiceFailure
     * 
     */
    public Map<String, String> getNodeIdList() throws ServiceFailure {
        Map<String, String> allNodeIds = new HashMap<String, String>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(base, "(objectClass=d1Node)", ctls);

            while (results != null && results.hasMore()) {
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();
                log.trace("Search result found for: " + nodeDn);

                //return dn;
                // or we could double check
                HashMap<String, String> attributesMap = new HashMap<String, String>();
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                String nodeId = null;
                String nodeBaseUrl = null;
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID();
                    if (attributeName.equalsIgnoreCase("d1NodeId")) {
                        nodeId = (String) attribute.get();
                    }
                    if (attributeName.equalsIgnoreCase("d1NodeBaseURL")) {
                        nodeBaseUrl = (String) attribute.get();
                    }
                }
                allNodeIds.put(nodeId, nodeBaseUrl);
            }
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem search Nodes for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allNodeIds;
    }

    /**
     * get a list of NodeReferences that have yet to be approved by an
     * Administrator after the Nodes have been registered
     *
     * @return List<NodeReference>
     * @throws ServiceFailure
     * 
     */
    public List<NodeReference> getPendingNodeReferenceList() throws ServiceFailure {
        List<NodeReference> allNodeIds = new ArrayList<NodeReference>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(base, "(&(objectClass=d1Node)(d1NodeApproved=FALSE))", ctls);

            while (results != null && results.hasMore()) {
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();
                log.trace("Search result found for: " + nodeDn);

                //return dn;
                // or we could double check
                HashMap<String, String> attributesMap = new HashMap<String, String>();
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID();
                    if (attributeName.equalsIgnoreCase("d1NodeId")) {
                        NodeReference nodeId = new NodeReference();
                        nodeId.setValue((String) attribute.get());
                        allNodeIds.add(nodeId);
                    }
                }
            }
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem search Pending Nodes for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allNodeIds;
    }

    /**
     * each CN will have a state associated with DataONE processing
     * ProcessingState values are Offline, Recovery, or Active
     *
     * @param nodeIdentifier
     * @return ProcessingState
     * @throws ServiceFailure
     * 
     */
    public ProcessingState getProcessingState(NodeReference nodeIdentifier) throws ServiceFailure {
        ProcessingState processingState = null;
        try {
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(ProcessingStateAttribute.toLowerCase())) {
                processingState = ProcessingState.convert(getEnumerationValueString(attributesMap.get(ProcessingStateAttribute.toLowerCase())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem determining processing state " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not determine state of : " + nodeIdentifier + " " + e.getMessage());
        }
        return processingState;
    }

    /**
     * each MN Node will have a boolean associated with DataONE logAggregation
     * The field is hidden from the nodeList, it may only be manipulated by D1 processing
     * or a DataONE LDAP administrator
     * 
     * The LDAP attribute of the Node functionally a replicated lock. It works in
     * conjuction with a java distributed lock to ensure that only a single
     * log aggregation process at a time may be running.
     * 
     * The attribute may also serve as a backend mechanism to turn logAggregation
     * off for a particular node
     *
     * @param nodeIdentifier
     * @return AggregateLogs
     * @throws ServiceFailure
     * 
     */
    public Boolean getAggregateLogs(NodeReference nodeIdentifier) throws ServiceFailure {
        // since this attribute will not be prepopulated, return true as the default
        // it will be populated once it is set to false before the initial run
        // of course this leaves a race condition if the first run of a node
        // is performed during a split brain state of the cluster
        // however, preventing logAgg from running during split brain
        // will be dealt with in the logAgg code and not here
        Boolean aggregateLogs = Boolean.valueOf(true);
        try {
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(AggregateLogsAttribute.toLowerCase())) {

                 aggregateLogs =   Boolean.valueOf(getEnumerationValueString(attributesMap.get(AggregateLogsAttribute.toLowerCase())));
                 log.debug("aggregateLogsString is " + aggregateLogs);
            } else {
                // It not exist, so create it
 
                String dnNodeIdentifier = buildNodeDN(nodeIdentifier);
                Attribute d1NodeAggregateLogs = new BasicAttribute(AggregateLogsAttribute, Boolean.toString(aggregateLogs).toUpperCase());
                // get a handle to an Initial DirContext
                DirContext ctx = getContext();

                // construct the list of modifications to make
                ModificationItem[] mods = new ModificationItem[1];
                mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeAggregateLogs);

                // make the change
                ctx.modifyAttributes(dnNodeIdentifier, mods);
                log.debug("Initialize Aggregate Logs: " + dnNodeIdentifier);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem determining processing state " + nodeIdentifier + " of attribute" + AggregateLogsAttribute, e);
            throw new ServiceFailure("4801", "Could not determine state of : " + nodeIdentifier + " of attribute " + AggregateLogsAttribute + " " + e.getMessage());
        }
        return aggregateLogs;
    }
    /**
     * from the provided attributeMap returned from an LDAP query, fill out a Node
     * NodeServices are not including in this mapping
     *
     * @param attributesMap
     * @return Node
     * @throws NamingException
     * 
     */
    public Node mapNode(HashMap<String, NamingEnumeration> attributesMap) throws NamingException {
        Node node = new Node();

        if (attributesMap.containsKey("d1nodeid")) {
            NodeReference nodeReference = new NodeReference();

            nodeReference.setValue(getEnumerationValueString(attributesMap.get("d1nodeid")));
            node.setIdentifier(nodeReference);
        }

        if (attributesMap.containsKey("d1nodename")) {
            node.setName(getEnumerationValueString(attributesMap.get("d1nodename")));
        }

        if (attributesMap.containsKey("d1nodebaseurl")) {
            node.setBaseURL(getEnumerationValueString(attributesMap.get("d1nodebaseurl")));
        }

        if (attributesMap.containsKey("d1nodedescription")) {
            node.setDescription(getEnumerationValueString(attributesMap.get("d1nodedescription")));
        }

        if (attributesMap.containsKey("subject")) {
            NamingEnumeration subjects = attributesMap.get("subject");
            while (subjects.hasMore()) {
                Subject nodeSubject = new Subject();
                String subjectValue = (String) subjects.next();
                X500Principal principal = new X500Principal(subjectValue);
                String standardizedName = principal.getName(X500Principal.RFC2253);
                nodeSubject.setValue(standardizedName);
                node.addSubject(nodeSubject);
            }
        }
        if (attributesMap.containsKey("d1nodecontactsubject")) {
            NamingEnumeration contactSubjects = attributesMap.get("d1nodecontactsubject");
            while (contactSubjects.hasMore()) {
                Subject nodeContactSubject = new Subject();
                String contactSubjectValue = (String) contactSubjects.next();
                X500Principal principal = new X500Principal(contactSubjectValue);
                String standardizedName = principal.getName(X500Principal.RFC2253);
                nodeContactSubject.setValue(standardizedName);
                node.addContactSubject(nodeContactSubject);
            }
        }
        if (attributesMap.containsKey("d1nodereplicate")) {
            node.setReplicate(Boolean.valueOf(getEnumerationValueString(attributesMap.get("d1nodereplicate"))));
        }

        if (attributesMap.containsKey("d1nodesynchronize")) {
            node.setSynchronize(Boolean.valueOf(getEnumerationValueString(attributesMap.get("d1nodesynchronize"))));
        }

        if (attributesMap.containsKey("d1nodestate")) {
            node.setState(NodeState.convert(getEnumerationValueString(attributesMap.get("d1nodestate"))));
        }

        if (attributesMap.containsKey("d1nodetype")) {
            node.setType(NodeType.convert(getEnumerationValueString(attributesMap.get("d1nodetype"))));

            // Here begins the optional params

            // synchronization schedules and status reports are only for MNs
            if (node.getType().compareTo(NodeType.MN) == 0) {
                // My assumption is if d1NodeSynSchdSec does not exist, then
                // the node does not have a schedule
                log.trace("found a Membernode");
                if (attributesMap.containsKey("d1nodesynschdsec")) {
                    Synchronization synchronization = new Synchronization();
                    Schedule schedule = new Schedule();
                    schedule.setSec(getEnumerationValueString(attributesMap.get("d1nodesynschdsec")));
                    schedule.setMin(getEnumerationValueString(attributesMap.get("d1nodesynschdmin")));
                    schedule.setHour(getEnumerationValueString(attributesMap.get("d1nodesynschdhour")));
                    schedule.setMday(getEnumerationValueString(attributesMap.get("d1nodesynschdmday")));
                    schedule.setMon(getEnumerationValueString(attributesMap.get("d1nodesynschdmon")));
                    schedule.setWday(getEnumerationValueString(attributesMap.get("d1nodesynschdwday")));
                    schedule.setYear(getEnumerationValueString(attributesMap.get("d1nodesynschdyear")));
                    synchronization.setSchedule(schedule);

                    synchronization.setLastHarvested(DateTimeMarshaller.deserializeDateToUTC(getEnumerationValueString(attributesMap.get("d1nodelastharvested"))));
                    synchronization.setLastCompleteHarvest(DateTimeMarshaller.deserializeDateToUTC(getEnumerationValueString(attributesMap.get("d1nodelastcompleteharvest"))));
                    node.setSynchronization(synchronization);
                }
                // this is optional for a membernode as well
                if (attributesMap.containsKey("d1nodepingsuccess")) {
                    Ping ping = new Ping();
                    ping.setSuccess(Boolean.valueOf(getEnumerationValueString(attributesMap.get("d1nodepingsuccess"))));
                    ping.setLastSuccess(DateTimeMarshaller.deserializeDateToUTC(getEnumerationValueString(attributesMap.get("d1nodepingdatechecked"))));
                    node.setPing(ping);
                }
                
                NodeReplicationPolicy nrp = null;
                if (attributesMap.containsKey("d1ReplicationPolicyMaxObjectSize")) {
                	if (nrp == null) nrp = new NodeReplicationPolicy();
                	nrp.setMaxObjectSize(new BigInteger(getEnumerationValueString(attributesMap.get("d1ReplicationPolicyMaxObjectSize"))));
                }
                if (attributesMap.containsKey("d1ReplicationPolicySpaceAllocated")) {
                	if (nrp == null) nrp = new NodeReplicationPolicy();
                	nrp.setSpaceAllocated(new BigInteger(getEnumerationValueString(attributesMap.get("d1ReplicationPolicySpaceAllocated"))));
                }
                if (attributesMap.containsKey("d1ReplicationPolicyAllowedNode")) {
                	if (nrp == null) nrp = new NodeReplicationPolicy();
                	NamingEnumeration allowedNodes = attributesMap.get("d1ReplicationPolicyAllowedNode");
                    while (allowedNodes.hasMore()) {
                    	NodeReference nr = new NodeReference();
                    	nr.setValue( (String) allowedNodes.next());
                    	nrp.addAllowedNode(nr);
                    }
                }
                if (attributesMap.containsKey("d1ReplicationPolicyAllowedObjectFormat")) {
                	if (nrp == null) nrp = new NodeReplicationPolicy();
                	NamingEnumeration allowedFormats = attributesMap.get("d1ReplicationPolicyAllowedObjectFormat");
                    while (allowedFormats.hasMore()) {
                    	ObjectFormatIdentifier formatid = new ObjectFormatIdentifier();
                    	formatid.setValue( (String) allowedFormats.next());
                    	nrp.addAllowedObjectFormat(formatid);
                    }
                }
                if (nrp != null) {
                	node.setNodeReplicationPolicy(nrp);
                }
            }
        }
        return node;
    }

    /**
     * from the provided Node, fill in the Attributes that will be used
     * to create the Node in LDAP
     * NodeServices are not included in this mapping
     *
     * @param node
     * @return Attributes
     * 
     */
    public Attributes mapNodeAttributes(Node node) {
        Attributes nodeAttributes = new BasicAttributes();
        Attribute objClasses = new BasicAttribute("objectclass");
        objClasses.add("device");
        objClasses.add("d1Node");
        nodeAttributes.put(objClasses);

        nodeAttributes.put(new BasicAttribute("cn", node.getIdentifier().getValue()));

        nodeAttributes.put(new BasicAttribute("d1NodeId", node.getIdentifier().getValue()));
        nodeAttributes.put(new BasicAttribute("d1NodeName", node.getName()));
        nodeAttributes.put(new BasicAttribute("d1NodeDescription", node.getDescription()));
        nodeAttributes.put(new BasicAttribute("d1NodeBaseURL", node.getBaseURL()));
        nodeAttributes.put(new BasicAttribute("d1NodeReplicate", Boolean.toString(node.isReplicate()).toUpperCase()));
        nodeAttributes.put(new BasicAttribute("d1NodeSynchronize", Boolean.toString(node.isSynchronize()).toUpperCase()));
        nodeAttributes.put(new BasicAttribute("d1NodeType", node.getType().xmlValue()));
        nodeAttributes.put(new BasicAttribute("d1NodeState", node.getState().xmlValue()));
        nodeAttributes.put(new BasicAttribute(NodeApprovedAttribute, Boolean.toString(Boolean.FALSE).toUpperCase()));
        // Any other attributes are membernode only attributes

        if ((node.getSubjectList() != null) && (!node.getSubjectList().isEmpty())) {
            Attribute subjects = new BasicAttribute("subject");
            for (Subject subject : node.getSubjectList()) {
                //X500Principal principal = new X500Principal(subject.getValue());
                //String standardizedName = principal.getName(X500Principal.RFC2253);
                //subjects.add(standardizedName);
                // save the subject as it is sent, just standardize upon retrieval
                subjects.add(subject.getValue());
            }
            nodeAttributes.put(subjects);
        }
        if ((node.getContactSubjectList() != null) && (!node.getContactSubjectList().isEmpty())) {
            Attribute contactSubjects = new BasicAttribute("d1NodeContactSubject");
            for (Subject contactSubject : node.getContactSubjectList()) {
                //X500Principal principal = new X500Principal(contactSubject.getValue());
                //String standardizedName = principal.getName(X500Principal.RFC2253);
                //contactSubjects.add(standardizedName);
                // save the subject as it is sent, just standardize upon retrieval
                contactSubjects.add(contactSubject.getValue());
            }
            nodeAttributes.put(contactSubjects);
        } else {
            // throw an exception
            throw new NullPointerException("ContactSubjectList may not be null or empty");
        }
        // synchronization schedules and status reports are only for MNs
        if (node.getType().compareTo(NodeType.MN) == 0) {
            // If there is  synchronization
            if (node.getSynchronization() != null) {
                nodeAttributes.put(new BasicAttribute("d1NodeSynSchdSec", node.getSynchronization().getSchedule().getSec()));
                nodeAttributes.put(new BasicAttribute("d1NodeSynSchdMin", node.getSynchronization().getSchedule().getMin()));
                nodeAttributes.put(new BasicAttribute("d1NodeSynSchdHour", node.getSynchronization().getSchedule().getHour()));
                nodeAttributes.put(new BasicAttribute("d1NodeSynSchdMday", node.getSynchronization().getSchedule().getMday()));
                nodeAttributes.put(new BasicAttribute("d1NodeSynSchdMon", node.getSynchronization().getSchedule().getMon()));
                nodeAttributes.put(new BasicAttribute("d1NodeSynSchdWday", node.getSynchronization().getSchedule().getWday()));
                nodeAttributes.put(new BasicAttribute("d1NodeSynSchdYear", node.getSynchronization().getSchedule().getYear()));
                nodeAttributes.put(new BasicAttribute("d1NodeLastHarvested", "1900-01-01T00:00:00Z"));
                nodeAttributes.put(new BasicAttribute("d1NodeLastCompleteHarvest", "1900-01-01T00:00:00Z"));
            }
        }
        
        /* Node Replication Policy items */
        NodeReplicationPolicy nrp = node.getNodeReplicationPolicy();
        if (nrp != null)  {
        	nodeAttributes.put(new BasicAttribute("d1ReplicationPolicyMaxObjectSize", nrp.getMaxObjectSize()));
        	nodeAttributes.put(new BasicAttribute("d1ReplicationPolicySpaceAllocated", nrp.getSpaceAllocated()));
        	if (nrp.getAllowedNodeList() != null && !nrp.getAllowedNodeList().isEmpty()) {
        		Attribute allowedNodes = new BasicAttribute("d1ReplicationPolicyAllowedNode");
        		for (NodeReference nr : nrp.getAllowedNodeList()) {
        			allowedNodes.add(nr);
        		}
        		nodeAttributes.put(allowedNodes);
        	}
        	if (nrp.getAllowedObjectFormatList() != null && !nrp.getAllowedObjectFormatList().isEmpty()) {
        		Attribute allowedFormats = new BasicAttribute("d1ReplicationPolicyAllowedObjectFormat");
        		for (ObjectFormatIdentifier nr : nrp.getAllowedObjectFormatList()) {
        			allowedFormats.add(nr);
        		}
        		nodeAttributes.put(allowedFormats);
        	}
        }
 
        return nodeAttributes;
    }

    /**
     * from the provided Node and AttributesMap returned from a query,
     * fill in the modifiable Attributes that will be used
     * to update the Node in LDAP
     * NodeServices are not including in this mapping
     *
     * @param attributesMap
     * @param node
     * @return List<ModificationItem>
     * @throws NamingException
     * 
     */
    public List<ModificationItem> mapNodeModificationItemList(HashMap<String, NamingEnumeration> attributesMap, Node node) 
    throws NamingException {
        List<ModificationItem> modificationItemList = new ArrayList<ModificationItem>();

        if (attributesMap.containsKey("d1nodename")) {
            String currentNodeName = getEnumerationValueString(attributesMap.get("d1nodename"));
            if (!node.getName().contentEquals(currentNodeName)) {
                Attribute d1NodeName = new BasicAttribute("d1NodeName", node.getName());
                modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeName));
            }
        } else {
            Attribute d1NodeName = new BasicAttribute("d1NodeName", node.getName());
            modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeName));
        }

        if (attributesMap.containsKey("d1nodebaseurl")) {
            String currentBaseUrl = getEnumerationValueString(attributesMap.get("d1nodebaseurl"));
            if (!node.getBaseURL().contentEquals(currentBaseUrl)) {
                Attribute d1BaseUrl = new BasicAttribute("d1NodeBaseURL", node.getBaseURL());

                modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1BaseUrl));
            }
        } else {
            Attribute d1BaseUrl = new BasicAttribute("d1NodeBaseURL", node.getBaseURL());
            modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1BaseUrl));
        }

        if (attributesMap.containsKey("d1nodedescription")) {
            String currentNodeDescription = getEnumerationValueString(attributesMap.get("d1nodedescription"));
            if (!node.getDescription().contentEquals(currentNodeDescription)) {
                Attribute d1NodeDescription = new BasicAttribute("d1NodeDescription", node.getDescription());
                modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeDescription));
            }

        } else {
            Attribute d1NodeDescription = new BasicAttribute("d1NodeDescription", node.getDescription());
            modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeDescription));

        }

        if (attributesMap.containsKey("subject") || !(node.getSubjectList().isEmpty())) {
            List<Subject> existingSubjectList = new ArrayList<Subject>();
            if (attributesMap.containsKey("subject")) {
                NamingEnumeration subjects = attributesMap.get("subject");
                while (subjects.hasMore()) {
                    Subject nodeSubject = new Subject();
                    nodeSubject.setValue((String) subjects.next());
                    existingSubjectList.add(nodeSubject);
                }
            }
            // Determine which attributes to add
            // added attributes should be the items in the new subject list
            // minus the same items in the existing list
            List<Subject> addSubjectList = new ArrayList<Subject>();
            addSubjectList.addAll(node.getSubjectList());

            if (!(existingSubjectList.isEmpty()) && !(addSubjectList.isEmpty())) {
                addSubjectList.removeAll(existingSubjectList);
            }

            if (!addSubjectList.isEmpty()) {
                Attribute addSubjects = new BasicAttribute("subject");

                for (Subject addSubject : addSubjectList) {
                    addSubjects.add(addSubject.getValue());
                }
                modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, addSubjects));
            }

            // Determine which attributes to remove
            // removed attributes should be the items in the existing subject list
            // minus the same items in the new subject list
            List<Subject> removeSubjectList = new ArrayList<Subject>();
            removeSubjectList.addAll(existingSubjectList);
            if (!(node.getSubjectList().isEmpty()) && !(removeSubjectList.isEmpty())) {
                removeSubjectList.removeAll(node.getSubjectList());
            }

            if (!removeSubjectList.isEmpty()) {
                Attribute removeSubjects = new BasicAttribute("subject");

                for (Subject removeSubject : removeSubjectList) {
                    removeSubjects.add(removeSubject.getValue());
                }
                modificationItemList.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, removeSubjects));
            }
        }

        if (attributesMap.containsKey("d1nodecontactsubject") || !(node.getContactSubjectList().isEmpty())) {

            List<Subject> existingContactSubjectList = new ArrayList<Subject>();
            if (attributesMap.containsKey("d1nodecontactsubject")) {
                NamingEnumeration contactSubjects = attributesMap.get("d1nodecontactsubject");
                while (contactSubjects.hasMore()) {
                    Subject contactSubject = new Subject();
                    contactSubject.setValue((String) contactSubjects.next());
                    existingContactSubjectList.add(contactSubject);

                }
            }
            // Determine which attributes to add
            // added attributes should be the items in the new subject list
            // minus the same items in the existing list
            List<Subject> addContactSubjectList = new ArrayList<Subject>();
            addContactSubjectList.addAll(node.getContactSubjectList());

            if (!(existingContactSubjectList.isEmpty()) && !(addContactSubjectList.isEmpty())) {
                addContactSubjectList.removeAll(existingContactSubjectList);
            }

            if (!addContactSubjectList.isEmpty()) {
                Attribute addContactSubjects = new BasicAttribute("d1NodeContactSubject");

                for (Subject addContactSubject : addContactSubjectList) {
                    addContactSubjects.add(addContactSubject.getValue());
                }
                modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, addContactSubjects));
            }

            // Determine which attributes to remove
            // removed attributes should be the items in the existing subject list
            // minus the same items in the new subject list
            List<Subject> removeContactSubjectList = new ArrayList<Subject>();
            removeContactSubjectList.addAll(existingContactSubjectList);
            if (!(node.getContactSubjectList().isEmpty()) && !(removeContactSubjectList.isEmpty())) {
                removeContactSubjectList.removeAll(node.getContactSubjectList());
            }

            if (!removeContactSubjectList.isEmpty()) {
                Attribute removeContactSubjects = new BasicAttribute("d1NodeContactSubject");

                for (Subject removeContactSubject : removeContactSubjectList) {
                    removeContactSubjects.add(removeContactSubject.getValue());
                }
                modificationItemList.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, removeContactSubjects));
            }
        }

        if (attributesMap.containsKey("d1nodereplicate")) {
            Boolean currentNodeReplicate = Boolean.valueOf(getEnumerationValueString(attributesMap.get("d1nodereplicate")));
            if (node.isReplicate() != currentNodeReplicate) {
                Attribute d1NodeReplicate = new BasicAttribute("d1NodeReplicate", Boolean.toString(node.isReplicate()).toUpperCase());
                modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeReplicate));
            }
        } else {
            Attribute d1NodeReplicate = new BasicAttribute("d1NodeReplicate", Boolean.toString(node.isReplicate()).toUpperCase());
            modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeReplicate));

        }

        if (attributesMap.containsKey("d1nodesynchronize")) {
            Boolean currentNodeSynchronize = Boolean.valueOf(getEnumerationValueString(attributesMap.get("d1nodesynchronize")));
            if (node.isSynchronize() != currentNodeSynchronize) {
                Attribute d1NodeSynchronize = new BasicAttribute("d1NodeSynchronize", Boolean.toString(node.isSynchronize()).toUpperCase());
                modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeSynchronize));
            }

        } else {
            Attribute d1NodeSynchronize = new BasicAttribute("d1NodeSynchronize", Boolean.toString(node.isSynchronize()).toUpperCase());
            modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynchronize));

        }

        if (attributesMap.containsKey("d1nodestate")) {
            NodeState currentNodeState = NodeState.convert(getEnumerationValueString(attributesMap.get("d1nodestate")));
            if (node.getState() == null) {
                log.error("node state is null");
            }
            if (currentNodeState == null) {
                log.error("currentNodeStateis null:" + getEnumerationValueString(attributesMap.get("d1nodestate")));
            }
            if (node.getState().compareTo(currentNodeState) != 0) {
                Attribute d1NodeState = new BasicAttribute("d1NodeState", node.getState().xmlValue());
                modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeState));
            }

        } else {
            Attribute d1NodeState = new BasicAttribute("d1NodeState", node.getState().xmlValue());
            modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeState));

        }

        
        if (attributesMap.containsKey("d1nodetype")) {
            NodeType nodeType = NodeType.convert(getEnumerationValueString(attributesMap.get("d1nodetype")));

            // Here begins the optional params, can not change node types!

            // synchronization schedules and status reports are only for MNs
            if (nodeType.compareTo(NodeType.MN) == 0) {
                // My assumption is if d1NodeSynSchdSec does not exist, then
                // the node does not have a schedule
                log.trace("found a Membernode");
                if (attributesMap.containsKey("d1nodesynschdsec")) {
                    if ((node.getSynchronization() != null)) {

                        String currentNodeSynSchdSec = getEnumerationValueString(attributesMap.get("d1nodesynschdsec"));
                        if (!node.getSynchronization().getSchedule().getSec().contentEquals(currentNodeSynSchdSec)) {
                            Attribute d1NodeSynSchdSec = new BasicAttribute("d1NodeSynSchdSec", node.getSynchronization().getSchedule().getSec());
                            modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeSynSchdSec));
                        }

                        String currentNodeSynSchdMin = getEnumerationValueString(attributesMap.get("d1nodesynschdmin"));
                        if (!node.getSynchronization().getSchedule().getMin().contentEquals(currentNodeSynSchdMin)) {
                            Attribute d1odeSynSchdMin = new BasicAttribute("d1NodeSynSchdMin", node.getSynchronization().getSchedule().getMin());
                            modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1odeSynSchdMin));
                        }

                        String currentNodeSynSchdHour = getEnumerationValueString(attributesMap.get("d1nodesynschdhour"));
                        if (!node.getSynchronization().getSchedule().getHour().contentEquals(currentNodeSynSchdHour)) {
                            Attribute d1NodeSynSchdHour = new BasicAttribute("d1NodeSynSchdHour", node.getSynchronization().getSchedule().getHour());
                            modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeSynSchdHour));
                        }

                        String currentNodeSynSchdMday = getEnumerationValueString(attributesMap.get("d1nodesynschdmday"));
                        if (!node.getSynchronization().getSchedule().getMday().contentEquals(currentNodeSynSchdMday)) {
                            Attribute d1NodeSynSchdMday = new BasicAttribute("d1NodeSynSchdMday", node.getSynchronization().getSchedule().getMday());
                            modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeSynSchdMday));
                        }

                        String currentNodeSynSchdMon = getEnumerationValueString(attributesMap.get("d1nodesynschdmon"));
                        if (!node.getSynchronization().getSchedule().getMon().contentEquals(currentNodeSynSchdMon)) {
                            Attribute d1NodeSynSchdMon = new BasicAttribute("d1NodeSynSchdMon", node.getSynchronization().getSchedule().getMon());
                            modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeSynSchdMon));
                        }

                        String currentNodeSynSchdWday = getEnumerationValueString(attributesMap.get("d1nodesynschdwday"));
                        if (!node.getSynchronization().getSchedule().getWday().contentEquals(currentNodeSynSchdWday)) {
                            Attribute d1NodeSynSchdWday = new BasicAttribute("d1NodeSynSchdWday", node.getSynchronization().getSchedule().getWday());
                            modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeSynSchdWday));
                        }

                        String currentNodeSynSchdYear = getEnumerationValueString(attributesMap.get("d1nodesynschdyear"));
                        if (!node.getSynchronization().getSchedule().getWday().contentEquals(currentNodeSynSchdYear)) {
                            Attribute d1NodeSynSchdYear = new BasicAttribute("d1NodeSynSchdYear", node.getSynchronization().getSchedule().getYear());
                            modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeSynSchdYear));
                        }
                    } else {
                        // well you can't remove a schedule, but you can turn off synchronization if you want
                        log.error("Unable to remove Synchronization for " + node.getIdentifier().getValue());
                    }
                } else {
                    if ((node.getSynchronization() != null)) {

                        Attribute d1NodeSynSchdSec = new BasicAttribute("d1NodeSynSchdSec", node.getSynchronization().getSchedule().getSec());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynSchdSec));

                        Attribute d1NodeSynSchdMin = new BasicAttribute("d1NodeSynSchdMin", node.getSynchronization().getSchedule().getMin());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynSchdMin));

                        Attribute d1NodeSynSchdHour = new BasicAttribute("d1NodeSynSchdHour", node.getSynchronization().getSchedule().getHour());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynSchdHour));

                        Attribute d1NodeSynSchdMday = new BasicAttribute("d1NodeSynSchdMday", node.getSynchronization().getSchedule().getMday());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynSchdMday));

                        Attribute d1NodeSynSchdMon = new BasicAttribute("d1NodeSynSchdMon", node.getSynchronization().getSchedule().getMon());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynSchdMon));

                        Attribute d1NodeSynSchdWday = new BasicAttribute("d1NodeSynSchdWday", node.getSynchronization().getSchedule().getWday());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynSchdWday));

                        Attribute d1NodeSynSchdYear = new BasicAttribute("d1NodeSynSchdYear", node.getSynchronization().getSchedule().getYear());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeSynSchdYear));
                        
                        Attribute d1NodeLastHarvested = new BasicAttribute("d1NodeLastHarvested", "1900-01-01T00:00:00Z");
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeLastHarvested));
                        
                        Attribute d1NodeLastCompleteHarvest = new BasicAttribute("d1NodeLastCompleteHarvest", "1900-01-01T00:00:00Z");
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeLastCompleteHarvest));
                    }
                }

            }
            
            /* NodeReplicationPolicy elements and attributes */
            if (node.getNodeReplicationPolicy() != null) {
            
	            String attrLDAPname = "d1ReplicationPolicyMaxObjectSize";
	            BigInteger value = node.getNodeReplicationPolicy().getMaxObjectSize();
	            if (attributesMap.containsKey(attrLDAPname)) {
	                String currentVal = getEnumerationValueString(attributesMap.get(attrLDAPname));
	                if (!value.toString().contentEquals(currentVal)) {
	                    Attribute attr = new BasicAttribute(attrLDAPname, value);
	                    modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attr));
	                }
	
	            } else {
	                Attribute attr = new BasicAttribute(attrLDAPname, value);
	                modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attr));
	
	            }
	            
	            attrLDAPname = "d1ReplicationPolicySpaceAllocated";
	            /* BigInteger */ value = node.getNodeReplicationPolicy().getSpaceAllocated();
	            if (attributesMap.containsKey(attrLDAPname)) {
	                String currentVal = getEnumerationValueString(attributesMap.get(attrLDAPname));
	                if (!value.toString().contentEquals(currentVal)) {
	                    Attribute attr = new BasicAttribute(attrLDAPname, value);
	                    modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attr));
	                }
	
	            } else {
	                Attribute attr = new BasicAttribute(attrLDAPname, value);
	                modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attr));
	
	            }
	            
	            attrLDAPname = "d1ReplicationPolicyAllowedNode";
	            
	            List<NodeReference> existingNodeList = new ArrayList<NodeReference>();
	            if (attributesMap.containsKey(attrLDAPname)) {
	                NamingEnumeration currentList = attributesMap.get(attrLDAPname);
	                while (currentList.hasMore()) {
	                    NodeReference nodeValue = new NodeReference();
	                    nodeValue.setValue((String) currentList.next());
	                    existingNodeList.add(nodeValue);
	
	                }
	            }
	            // Determine which attributes to add
	            // added attributes should be the items in the new subject list
	            // minus the same items in the existing list
	            List<NodeReference> additionList = new ArrayList<NodeReference>();
	            additionList.addAll(node.getNodeReplicationPolicy().getAllowedNodeList());
	
	            if (!(existingNodeList.isEmpty()) && !(additionList.isEmpty())) {
	                additionList.removeAll(existingNodeList);
	            }
	
	            if (!additionList.isEmpty()) {
	                Attribute addNodes = new BasicAttribute(attrLDAPname);
	
	                for (NodeReference addition : additionList) {
	                    addNodes.add(addition.getValue());
	                }
	                modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, addNodes));
	            }
	
	            // Determine which attributes to remove
	            // removed attributes should be the items in the existing subject list
	            // minus the same items in the new subject list
	            List<NodeReference> removalList = new ArrayList<NodeReference>();
	            removalList.addAll(existingNodeList);
	            if (!(node.getNodeReplicationPolicy().getAllowedObjectFormatList().isEmpty()) && !(removalList.isEmpty())) {
	                removalList.removeAll(node.getContactSubjectList());
	            }
	
	            if (!removalList.isEmpty()) {
	                Attribute removeNodes = new BasicAttribute(attrLDAPname);
	
	                for (NodeReference removal : removalList) {
	                    removeNodes.add(removal.getValue());
	                }
	                modificationItemList.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, removeNodes));
	            }
	            
	            attrLDAPname = "d1ReplicationPolicyAllowedObjectFormat";
	            
	            List<ObjectFormatIdentifier> existingFormatList = new ArrayList<ObjectFormatIdentifier>();
	            if (attributesMap.containsKey(attrLDAPname)) {
	                NamingEnumeration currentList = attributesMap.get(attrLDAPname);
	                while (currentList.hasMore()) {
	                	ObjectFormatIdentifier formatValue = new ObjectFormatIdentifier();
	                    formatValue.setValue((String) currentList.next());
	                    existingFormatList.add(formatValue);
	
	                }
	            }
	            // Determine which attributes to add
	            // added attributes should be the items in the new subject list
	            // minus the same items in the existing list
	            List<ObjectFormatIdentifier> formatAdditionList = new ArrayList<ObjectFormatIdentifier>();
	            formatAdditionList.addAll(node.getNodeReplicationPolicy().getAllowedObjectFormatList());
	
	            if (!(existingFormatList.isEmpty()) && !(formatAdditionList.isEmpty())) {
	                formatAdditionList.removeAll(existingFormatList);
	            }
	
	            if (!formatAdditionList.isEmpty()) {
	                Attribute addFormats = new BasicAttribute(attrLDAPname);
	
	                for (ObjectFormatIdentifier addition : formatAdditionList) {
	                    addFormats.add(addition.getValue());
	                }
	                modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, addFormats));
	            }
	
	            // Determine which attributes to remove
	            // removed attributes should be the items in the existing subject list
	            // minus the same items in the new subject list
	            List<ObjectFormatIdentifier> formatRemovalList = new ArrayList<ObjectFormatIdentifier>();
	            formatRemovalList.addAll(existingFormatList);
	            if (!(node.getNodeReplicationPolicy().getAllowedObjectFormatList().isEmpty()) && !(formatRemovalList.isEmpty())) {
	                formatRemovalList.removeAll(node.getContactSubjectList());
	            }
	
	            if (!formatRemovalList.isEmpty()) {
	                Attribute removeFormats = new BasicAttribute(attrLDAPname);
	
	                for (ObjectFormatIdentifier removal : formatRemovalList) {
	                    removeFormats.add(removal.getValue());
	                }
	                modificationItemList.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, removeFormats));
	            }
            }
        }
        
        return modificationItemList;
    }
    
    

    /**
     * update the date a Member node was last Synchronized
     *
     * Synchronization uses last harvested date to poll membernodes for new content
     * the date is the latest dateSysMetadataModified from the last batch of records
     * harvested
     *
     * @param nodeIdentifier
     * @param lastDateNodeHarvested
     * @throws ServiceFailure
     * 
     */
    public void setDateLastHarvested(NodeReference nodeIdentifier, Date lastDateNodeHarvested) throws ServiceFailure {
        try {
            String dnNodeIdentifier = "cn=" + nodeIdentifier.getValue() + ",dc=dataone,dc=org";
            Attribute d1NodeLastHarvested = new BasicAttribute("d1NodeLastHarvested", DateTimeMarshaller.serializeDateToUTC(lastDateNodeHarvested));
            // get a handle to an Initial DirContext
            DirContext ctx = getContext();

            // construct the list of modifications to make
            ModificationItem[] mods = new ModificationItem[1];
            mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeLastHarvested);

            // make the change
            ctx.modifyAttributes(dnNodeIdentifier, mods);
            log.debug("Updated entry: " + dnNodeIdentifier);
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem updating lastHarvested for node " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not update account: " + e.getMessage());
        }
    }

    /**
     * update the date a DataONE Node was last Aggregated for Log records
     *
     * LogAggregation uses last aggregated date to poll membernodes for new content
     * the date is the latest dateLogged from the LogEvent records harvested
     *
     * @param nodeIdentifier
     * @param logAggregationDate
     * @throws ServiceFailure
     * 
     */
    public void setLogLastAggregated(NodeReference nodeIdentifier, Date logAggregationDate) throws ServiceFailure {
        try {
            String dnNodeIdentifier = buildNodeDN(nodeIdentifier);
            String strLogLastAggregated = DateTimeMarshaller.serializeDateToUTC(logAggregationDate);
            Attribute d1NodeLogLastAggregated = new BasicAttribute(LogLastAggregatedAttribute, strLogLastAggregated);
            // get a handle to an Initial DirContext
            DirContext ctx = getContext();

            // construct the list of modifications to make
            ModificationItem[] mods = new ModificationItem[1];
            if (getLogLastAggregated(nodeIdentifier) == null) {
                mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeLogLastAggregated);
            } else {
                mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeLogLastAggregated);
            }
            // make the change
            ctx.modifyAttributes(dnNodeIdentifier, mods);
            log.debug("set LogLastAggregated: " + dnNodeIdentifier + " to " + strLogLastAggregated);
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem setting LogLastAggregated " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
        }
    }

    /**
     * update a registered DataONE Node to be approved (or unapproved if need be)
     *
     *
     * @param nodeIdentifier
     * @param approved
     * @throws ServiceFailure
     * 
     */
    public void setNodeApproved(NodeReference nodeIdentifier, Boolean approved) throws ServiceFailure {
        try {
            String dnNodeIdentifier = buildNodeDN(nodeIdentifier);
            Attribute d1NodeApproval = new BasicAttribute(NodeApprovedAttribute, Boolean.toString(approved).toUpperCase());
            // get a handle to an Initial DirContext
            DirContext ctx = getContext();

            // construct the list of modifications to make
            ModificationItem[] mods = new ModificationItem[1];
            mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeApproval);

            // make the change
            ctx.modifyAttributes(dnNodeIdentifier, mods);
            log.debug("Approved Node: " + dnNodeIdentifier);
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem approving node " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
        }
    }
    /**
     * create a DataONE Node
     *
     *
     * @param node
     * @throws ServiceFailure
     * @throws NotImplemented
     * @throws InvalidRequest
     * @throws NotFound
     * 
     */
    public void createNode(Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
        String dnNodeIdentifier = buildNodeDN(node.getIdentifier());

        try {
            DirContext ctx = getContext();
            Attributes nodeAttributes = mapNodeAttributes(node);
            ctx.createSubcontext(dnNodeIdentifier, nodeAttributes);
            log.debug("Added Node entry " + dnNodeIdentifier);
            if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
                for (Service service : node.getServices().getServiceList()) {
                    String serviceDN = nodeServicesAccess.buildNodeServiceDN(node.getIdentifier(), service);
                    Attributes serviceAttributes = nodeServicesAccess.mapNodeServiceAttributes(node, service);
                    ctx.createSubcontext(serviceDN, serviceAttributes);
                    log.debug("Added Node Service entry " + serviceDN);
                    if ((service.getRestrictionList() != null) && (service.getRestrictionList().size() > 0)) {
                        for (ServiceMethodRestriction restriction : service.getRestrictionList()) {
                            String serviceMethodRestrictionDN = serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(node.getIdentifier(), service, restriction);
                            Attributes serviceMethodRestrictionAttributes = serviceMethodRestrictionsAccess.mapServiceMethodRestrictionAttributes(node, service, restriction);
                            ctx.createSubcontext(serviceMethodRestrictionDN, serviceMethodRestrictionAttributes);
                            log.debug("Added Service Method Restriction entry " + serviceMethodRestrictionDN);
                        }
                    }
                }
            }
        } catch (NamingException ex1) {
            ex1.printStackTrace();
            throw new ServiceFailure("0", "Register failed due to LDAP communication failure");
        }

    }
    /**
     * update a DataONE Node
     *
     *
     * @param node
     * @throws ServiceFailure
     * @throws NotImplemented
     * @throws InvalidRequest
     * @throws NotFound
     * 
     */
    public void updateNode(Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {

        try {
            DirContext ctx = getContext();
            NodeReference nodeid = node.getIdentifier();
            String nodeDn = buildNodeDN(nodeid);
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(nodeDn);
            List<ModificationItem> modificationItemList = mapNodeModificationItemList(attributesMap, node);
            ModificationItem[] modificationArray = new ModificationItem[modificationItemList.size()];
            modificationArray = modificationItemList.toArray(modificationArray);
            ctx.modifyAttributes(nodeDn, modificationArray);

            // easiest to remove existingServices and then adding the new ones back
            List<Service> existingNodeServices = nodeServicesAccess.getServiceList(nodeid.getValue());
            if ((existingNodeServices != null) && !(existingNodeServices.isEmpty())) {
                for (Service removeService : existingNodeServices) {
                    String d1NodeServiceId = nodeServicesAccess.buildNodeServiceId(removeService);
                    List<ServiceMethodRestriction> serviceMethodRestrictionList = serviceMethodRestrictionsAccess.getServiceMethodRestrictionList(nodeid.getValue(), d1NodeServiceId);
                    if ((serviceMethodRestrictionList != null) && !(serviceMethodRestrictionList.isEmpty())) {
                        for (ServiceMethodRestriction removeServiceMethodRestriction : serviceMethodRestrictionList) {
                            serviceMethodRestrictionsAccess.deleteServiceMethodRestriction(nodeid, removeService, removeServiceMethodRestriction);
                        }
                    }
                    nodeServicesAccess.deleteNodeService(nodeid, removeService);
                }
            }
            // add in the services
            if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
                for (Service service : node.getServices().getServiceList()) {
                    String serviceDN = nodeServicesAccess.buildNodeServiceDN(node.getIdentifier(), service);
                    Attributes serviceAttributes = nodeServicesAccess.mapNodeServiceAttributes(node, service);
                    ctx.createSubcontext(serviceDN, serviceAttributes);
                    log.trace("updateNodeCapabilities Added Node Service entry " + serviceDN);
                    if (service.getRestrictionList() != null) {
                        for (ServiceMethodRestriction restriction : service.getRestrictionList()) {
                            String serviceMethodRestrictionDN = serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(node.getIdentifier(), service, restriction);
                            Attributes serviceMethodRestrictionAttributes = serviceMethodRestrictionsAccess.mapServiceMethodRestrictionAttributes(node, service, restriction);
                            ctx.createSubcontext(serviceMethodRestrictionDN, serviceMethodRestrictionAttributes);
                            log.trace("updateNodeCapabilities Added Service Method Restriction entry " + serviceMethodRestrictionDN);
                        }
                    }
                }
            }
            log.debug("Updated NodeCapabilities Node: " + nodeDn);
        } catch (NamingException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("0", "updateNodeCapabilities failed due to LDAP communication failure");
        }

        
    }

    /**
     * update the state a DataONE Node's processing daemon
     * ProcessingState values are Offline, Recovery, or Active
     * 
     * needed by log aggregation to keep track of which nodes are available for recovery
     *
     * @param nodeIdentifier
     * @param processingState
     * @throws ServiceFailure
     * 
     */
    public void setProcessingState(NodeReference nodeIdentifier, ProcessingState processingState) throws ServiceFailure {
        try {
            String dnNodeIdentifier = buildNodeDN(nodeIdentifier);

            // get a handle to an Initial DirContext
            DirContext ctx = getContext();
            Attribute d1ProcessingStateAttribute = new BasicAttribute(ProcessingStateAttribute, processingState.getValue());
            // construct the list of modifications to make
            ModificationItem[] mods = new ModificationItem[1];
            if (getProcessingState(nodeIdentifier) == null) {
                mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE, d1ProcessingStateAttribute);
            } else {
                mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1ProcessingStateAttribute);
            }
            // make the change
            ctx.modifyAttributes(dnNodeIdentifier, mods);
            log.debug("set " + ProcessingStateAttribute + ": " + dnNodeIdentifier + " to " + processingState.getValue());
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            log.error("LDAP Service is unreponsive " + nodeIdentifier);
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("Problem setting ProcessingState " + nodeIdentifier, ex);
            throw new ServiceFailure("4801", "Could not set Processing state: " + nodeIdentifier + " " + ex.getMessage());
        }
    }
    
    /**
     * update the aggregate Logs Boolean on a DataONE Node
     *
     *
     * @param nodeIdentifier
     * @param aggregateLogs
     * @throws ServiceFailure
     * 
     */
    public void setAggregateLogs(NodeReference nodeIdentifier, Boolean aggregateLogs) throws ServiceFailure {
        try {
            String dnNodeIdentifier = buildNodeDN(nodeIdentifier);
            Attribute d1NodeAggregateLogs = new BasicAttribute(AggregateLogsAttribute, Boolean.toString(aggregateLogs).toUpperCase());
            // get a handle to an Initial DirContext
            DirContext ctx = getContext();

            // construct the list of modifications to make
            ModificationItem[] mods = new ModificationItem[1];
            mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeAggregateLogs);

            // make the change
            ctx.modifyAttributes(dnNodeIdentifier, mods);
            log.debug("Set Aggregate Logs: " + dnNodeIdentifier);
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unreponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem Setting Aggregate Logs " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not set Aggregate Logs: " + nodeIdentifier + " " + e.getMessage());
        }
    }
}
