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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
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
    
	/* default value for a node record property when the node record is first persisted 
	 * (with the logic that it couldn't be harvested if not in the NodeRegistry )        */
	// TODO: does this belong here?  it seems like business logic...
	public static final String DEFAULT_NON_HARVEST_DATE = "1900-01-01T00:00:00Z";

	/* non-datatype attributes */
	public static final String NODE_APPROVED = "d1NodeApproved";
	public static final String PROCESSING_STATE = "d1NodeProcessingState";
	public static final String LOG_LAST_AGGREGATED = "d1NodeLogLastAggregated";
	public static final String AGGREGATE_LOGS = "d1NodeAggregateLogs";
	
	/* datatype attributes - only those that can be represented as lists or scalars */
	public static final String NODE_ID = "d1NodeId";
	public static final String NODE_NAME = "d1NodeName";
	public static final String NODE_DESCRIPTION = "d1NodeDescription";
	public static final String NODE_BASEURL = "d1NodeBaseURL";
	// services come next in the xml, constants defined in NodeServicesAccess
	
	/* synchronization elements */
	public static final String SYNC_SCHEDULE_SEC =  "d1NodeSynSchdSec";
	public static final String SYNC_SCHEDULE_MIN =  "d1NodeSynSchdMin";
	public static final String SYNC_SCHEDULE_HOUR = "d1NodeSynSchdHour";
	public static final String SYNC_SCHEDULE_MDAY = "d1NodeSynSchdMday";
	public static final String SYNC_SCHEDULE_MON =  "d1NodeSynSchdMon";
	public static final String SYNC_SCHEDULE_WDAY = "d1NodeSynSchdWday";
	public static final String SYNC_SCHEDULE_YEAR = "d1NodeSynSchdYear";
	public static final String NODE_LAST_HARVESTED = "d1NodeLastHarvested";
	public static final String NODE_LAST_COMPLETE_HARVEST = "d1NodeLastCompleteHarvest";
	/* replication policy elements */
	public static final String REP_POLICY_MAXOBJECTSIZE = "d1ReplicationPolicyMaxObjectSize";
	public static final String REP_POLICY_SPACEALLOCATED = "d1ReplicationPolicySpaceAllocated";
	public static final String REP_POLICY_ALLOWEDNODE = "d1ReplicationPolicyAllowedNode";
	public static final String REP_POLICY_ALLOWEDOBJECTFORMAT = "d1ReplicationPolicyAllowedObjectFormat";
	/* ping elements */
	public static final String PING_SUCCESS = "d1NodePingSuccess";
	public static final String PING_DATE_CHECKED = "d1NodePingDateChecked";

	public static final String NODE_SUBJECT = "subject";
	public static final String NODE_CONTACT_SUBJECT = "d1NodeContactSubject";	
	public static final String NODE_REPLICATE = "d1NodeReplicate";
	public static final String NODE_SYNCHRONIZE = "d1NodeSynchronize";
	public static final String NODE_TYPE = "d1NodeType";
	public static final String NODE_STATE = "d1NodeState";
	
    
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
    public HashMap<String, NamingEnumeration<?>> buildNodeAttributeMap(String nodeDN) throws NamingException {
        HashMap<String, NamingEnumeration<?>> attributesMap = new HashMap<String, NamingEnumeration<?>>();

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
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
                HashMap<String, NamingEnumeration<?>> attributesMap = new HashMap<String, NamingEnumeration<?>>();
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID().toLowerCase();
                    NamingEnumeration<?> attributeValue = attribute.getAll();
                    attributesMap.put(attributeName, attributeValue);
                }

                allNode.add(this.mapBasicNodeProperties(attributesMap, new Node()));
            }
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
                Attributes attributes = si.getAttributes();
                NamingEnumeration<? extends Attribute> values = attributes.getAll();
                while (values.hasMore()) {
                    Attribute attribute = values.next();
                    String attributeName = attribute.getID();
                    if (attributeName.equalsIgnoreCase("d1NodeId")) {
                        nodeReference.setValue((String) attribute.get());
                    }
                    if (attributeName.equalsIgnoreCase(PROCESSING_STATE)) {
                        nodeProperties.put(PROCESSING_STATE, (String) attribute.get());
                    }
                    if (attributeName.equalsIgnoreCase(LOG_LAST_AGGREGATED)) {
                        nodeProperties.put(LOG_LAST_AGGREGATED, (String) attribute.get());
                    }
                }
                nodeLogStatus.put(nodeReference, nodeProperties);
            }
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
            HashMap<String, NamingEnumeration<?>> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
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

        HashMap<String, NamingEnumeration<?>> attributesMap = buildNodeAttributeMap(nodeDN);
        log.debug("Retrieved Node for: " + nodeDN);

        if (attributesMap.isEmpty()) {
            throw new NotFound("4801", nodeDN + " not found on the server");
        }

        return this.mapBasicNodeProperties(attributesMap, new Node());


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
        HashMap<String, NamingEnumeration<?>> attributesMap;
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
        if (attributesMap.containsKey(NODE_APPROVED.toLowerCase())) {
            try {
                nodeApproved = Boolean.valueOf(getEnumerationValueString(attributesMap.get(NODE_APPROVED.toLowerCase())));
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
            return this.mapBasicNodeProperties(attributesMap, new Node());
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
            HashMap<String, NamingEnumeration<?>> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(NODE_APPROVED.toLowerCase())) {
                String nodeApprovedStr = getEnumerationValueString(attributesMap.get(NODE_APPROVED.toLowerCase()));
                
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
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
            HashMap<String, NamingEnumeration<?>> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(PROCESSING_STATE.toLowerCase())) {
                processingState = ProcessingState.convert(getEnumerationValueString(attributesMap.get(PROCESSING_STATE.toLowerCase())));
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
            HashMap<String, NamingEnumeration<?>> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(AGGREGATE_LOGS.toLowerCase())) {

                 aggregateLogs =   Boolean.valueOf(getEnumerationValueString(attributesMap.get(AGGREGATE_LOGS.toLowerCase())));
                 log.debug("aggregateLogsString is " + aggregateLogs);
            } else {
                // It not exist, so create it
 
                String dnNodeIdentifier = buildNodeDN(nodeIdentifier);
                Attribute d1NodeAggregateLogs = new BasicAttribute(AGGREGATE_LOGS, Boolean.toString(aggregateLogs).toUpperCase());
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
            log.error("Problem determining processing state " + nodeIdentifier + " of attribute" + AGGREGATE_LOGS, e);
            throw new ServiceFailure("4801", "Could not determine state of : " + nodeIdentifier + " of attribute " + AGGREGATE_LOGS + " " + e.getMessage());
        }
        return aggregateLogs;
    }

    
	/**
	 * from the provided attributeMap returned from an LDAP query, build out
	 * a DataONE Node datatype instance with the basic properties.  Complex 
	 * sub-types (such as NodeServices) that cannot be expressed as attributes of
	 * the Node record are not included in this mapping.
	 *
	 * @param attributesMap
	 * @return Node
	 * @throws NamingException
	 * 
	 */
	private Node mapBasicNodeProperties(HashMap<String, NamingEnumeration<?>> attributesMap, Node node) throws NamingException {

		if (attributesMap.containsKey(NODE_ID.toLowerCase())) {
			NodeReference nodeReference = new NodeReference();

			nodeReference.setValue(getEnumerationValueString(attributesMap.get(NODE_ID.toLowerCase())));
			node.setIdentifier(nodeReference);
		}

		if (attributesMap.containsKey(NODE_NAME.toLowerCase())) {
			node.setName(getEnumerationValueString(attributesMap.get(NODE_NAME.toLowerCase())));
		}

		if (attributesMap.containsKey(NODE_BASEURL.toLowerCase())) {
			node.setBaseURL(getEnumerationValueString(attributesMap.get(NODE_BASEURL.toLowerCase())));
		}

		if (attributesMap.containsKey(NODE_DESCRIPTION.toLowerCase())) {
			node.setDescription(getEnumerationValueString(attributesMap.get(NODE_DESCRIPTION.toLowerCase())));
		}

		if (attributesMap.containsKey(NODE_SUBJECT.toLowerCase())) {
			NamingEnumeration<?> subjects = attributesMap.get(NODE_SUBJECT.toLowerCase());
			while (subjects.hasMore()) {
				Subject nodeSubject = new Subject();
				String subjectValue = (String) subjects.next();
				X500Principal principal = new X500Principal(subjectValue);
				String standardizedName = principal.getName(X500Principal.RFC2253);
				nodeSubject.setValue(standardizedName);
				node.addSubject(nodeSubject);
			}
		}
		if (attributesMap.containsKey(NODE_CONTACT_SUBJECT.toLowerCase())) {
			NamingEnumeration<?> contactSubjects = attributesMap.get(NODE_CONTACT_SUBJECT.toLowerCase());
			while (contactSubjects.hasMore()) {
				Subject nodeContactSubject = new Subject();
				String contactSubjectValue = (String) contactSubjects.next();
				X500Principal principal = new X500Principal(contactSubjectValue);
				String standardizedName = principal.getName(X500Principal.RFC2253);
				nodeContactSubject.setValue(standardizedName);
				node.addContactSubject(nodeContactSubject);
			}
		}
		if (attributesMap.containsKey(NODE_REPLICATE.toLowerCase())) {
			node.setReplicate(Boolean.valueOf(getEnumerationValueString(attributesMap.get(NODE_REPLICATE.toLowerCase()))));
		}

		if (attributesMap.containsKey(NODE_SYNCHRONIZE.toLowerCase())) {
			node.setSynchronize(Boolean.valueOf(getEnumerationValueString(attributesMap.get(NODE_SYNCHRONIZE.toLowerCase()))));
		}

		if (attributesMap.containsKey(NODE_STATE.toLowerCase())) {
			node.setState(NodeState.convert(getEnumerationValueString(attributesMap.get(NODE_STATE.toLowerCase()))));
		}

		if (attributesMap.containsKey(NODE_TYPE.toLowerCase())) {
			node.setType(NodeType.convert(getEnumerationValueString(attributesMap.get(NODE_TYPE.toLowerCase()))));

			// Here begins the optional params

			// synchronization schedules and status reports are only for MNs
			if (node.getType().compareTo(NodeType.MN) == 0) {
				// My assumption is if d1NodeSynSchdSec does not exist, then
				// the node does not have a schedule
				log.trace("found a Membernode");
				if (attributesMap.containsKey(SYNC_SCHEDULE_SEC.toLowerCase())) {
					Synchronization synchronization = new Synchronization();
					Schedule schedule = new Schedule();
					schedule.setSec(getEnumerationValueString(attributesMap.get(SYNC_SCHEDULE_SEC.toLowerCase())));
					schedule.setMin(getEnumerationValueString(attributesMap.get(SYNC_SCHEDULE_MIN.toLowerCase())));
					schedule.setHour(getEnumerationValueString(attributesMap.get(SYNC_SCHEDULE_HOUR.toLowerCase())));
					schedule.setMday(getEnumerationValueString(attributesMap.get(SYNC_SCHEDULE_MDAY.toLowerCase())));
					schedule.setMon(getEnumerationValueString(attributesMap.get(SYNC_SCHEDULE_MON.toLowerCase())));
					schedule.setWday(getEnumerationValueString(attributesMap.get(SYNC_SCHEDULE_WDAY.toLowerCase())));
					schedule.setYear(getEnumerationValueString(attributesMap.get(SYNC_SCHEDULE_YEAR.toLowerCase())));
					synchronization.setSchedule(schedule);

					synchronization.setLastHarvested(DateTimeMarshaller.deserializeDateToUTC(
							getEnumerationValueString(attributesMap.get(NODE_LAST_HARVESTED.toLowerCase()))));
					synchronization.setLastCompleteHarvest(DateTimeMarshaller.deserializeDateToUTC(
							getEnumerationValueString(attributesMap.get(NODE_LAST_COMPLETE_HARVEST.toLowerCase()))));
					node.setSynchronization(synchronization);
				}
				// this is optional for a membernode as well
				if (attributesMap.containsKey(PING_SUCCESS.toLowerCase())) {
					Ping ping = new Ping();
					ping.setSuccess(Boolean.valueOf(
							getEnumerationValueString(attributesMap.get(PING_SUCCESS.toLowerCase()))));
					ping.setLastSuccess(DateTimeMarshaller.deserializeDateToUTC(
							getEnumerationValueString(attributesMap.get(PING_DATE_CHECKED.toLowerCase()))));
					node.setPing(ping);
				}

				NodeReplicationPolicy nrp = null;
				if (attributesMap.containsKey(REP_POLICY_MAXOBJECTSIZE.toLowerCase())) {
					if (nrp == null) nrp = new NodeReplicationPolicy();
					nrp.setMaxObjectSize(new BigInteger(
							getEnumerationValueString(attributesMap.get(REP_POLICY_MAXOBJECTSIZE.toLowerCase()))));
				}
				if (attributesMap.containsKey(REP_POLICY_SPACEALLOCATED.toLowerCase())) {
					if (nrp == null) nrp = new NodeReplicationPolicy();
					nrp.setSpaceAllocated(new BigInteger(
							getEnumerationValueString(attributesMap.get(REP_POLICY_SPACEALLOCATED.toLowerCase()))));
				}
				if (attributesMap.containsKey(REP_POLICY_ALLOWEDNODE.toLowerCase())) {
					if (nrp == null) nrp = new NodeReplicationPolicy();
					NamingEnumeration<?> allowedNodes = attributesMap.get(REP_POLICY_ALLOWEDNODE.toLowerCase());
					while (allowedNodes.hasMore()) {
						NodeReference nr = new NodeReference();
						nr.setValue( (String) allowedNodes.next());
						nrp.addAllowedNode(nr);
					}
				}
				if (attributesMap.containsKey(REP_POLICY_ALLOWEDOBJECTFORMAT.toLowerCase())) {
					if (nrp == null) nrp = new NodeReplicationPolicy();
					NamingEnumeration<?> allowedFormats = attributesMap.get(REP_POLICY_ALLOWEDOBJECTFORMAT.toLowerCase());
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
     * from the provided Node instance, fill in the Attributes that will be used
     * to create the Node in LDAP.  This is a lower-level mapping responsible only
     * for a subset of the attribute mappings. So, note that NodeServices and 
     * ServiceMethodRestrictions are not included in this mapping.
     *
     * @param node
     * @return Attributes
     * 
     */
    protected Attributes mapNodeAttributes(Node node) {
        Attributes nodeAttributes = new BasicAttributes(true /* ignore case */);
        Attribute objClasses = new BasicAttribute("objectclass");
        objClasses.add("device");
        objClasses.add("d1Node");
        nodeAttributes.put(objClasses);

        nodeAttributes.put(new BasicAttribute("cn", node.getIdentifier().getValue()));

        nodeAttributes.put(new BasicAttribute(NODE_ID, node.getIdentifier().getValue()));
        nodeAttributes.put(new BasicAttribute(NODE_NAME, node.getName()));
        nodeAttributes.put(new BasicAttribute(NODE_DESCRIPTION, node.getDescription()));
        nodeAttributes.put(new BasicAttribute(NODE_BASEURL, node.getBaseURL()));
        nodeAttributes.put(new BasicAttribute(NODE_REPLICATE, Boolean.toString(node.isReplicate()).toUpperCase()));
        nodeAttributes.put(new BasicAttribute(NODE_SYNCHRONIZE, Boolean.toString(node.isSynchronize()).toUpperCase()));
        nodeAttributes.put(new BasicAttribute(NODE_TYPE, node.getType().xmlValue()));
        nodeAttributes.put(new BasicAttribute(NODE_STATE, node.getState().xmlValue()));
        nodeAttributes.put(new BasicAttribute(NODE_APPROVED, Boolean.toString(Boolean.FALSE).toUpperCase()));
        // Any other attributes are membernode only attributes

        if ((node.getSubjectList() != null) && (!node.getSubjectList().isEmpty())) {
            Attribute subjects = new BasicAttribute(NODE_SUBJECT);
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
            Attribute contactSubjects = new BasicAttribute(NODE_CONTACT_SUBJECT);
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
                nodeAttributes.put(new BasicAttribute(SYNC_SCHEDULE_SEC, node.getSynchronization().getSchedule().getSec()));
                nodeAttributes.put(new BasicAttribute(SYNC_SCHEDULE_MIN, node.getSynchronization().getSchedule().getMin()));
                nodeAttributes.put(new BasicAttribute(SYNC_SCHEDULE_HOUR, node.getSynchronization().getSchedule().getHour()));
                nodeAttributes.put(new BasicAttribute(SYNC_SCHEDULE_MDAY, node.getSynchronization().getSchedule().getMday()));
                nodeAttributes.put(new BasicAttribute(SYNC_SCHEDULE_MON, node.getSynchronization().getSchedule().getMon()));
                nodeAttributes.put(new BasicAttribute(SYNC_SCHEDULE_WDAY, node.getSynchronization().getSchedule().getWday()));
                nodeAttributes.put(new BasicAttribute(SYNC_SCHEDULE_YEAR, node.getSynchronization().getSchedule().getYear()));
                nodeAttributes.put(new BasicAttribute(NODE_LAST_HARVESTED, DEFAULT_NON_HARVEST_DATE));
                nodeAttributes.put(new BasicAttribute(NODE_LAST_COMPLETE_HARVEST, DEFAULT_NON_HARVEST_DATE));
            }
        }
       
        /* Node Replication Policy items */
        NodeReplicationPolicy nrp = node.getNodeReplicationPolicy();
        if (nrp != null)  {
        	nodeAttributes.put(new BasicAttribute(REP_POLICY_MAXOBJECTSIZE, nrp.getMaxObjectSize().toString()));
        	nodeAttributes.put(new BasicAttribute(REP_POLICY_SPACEALLOCATED, nrp.getSpaceAllocated().toString()));
        	if (nrp.getAllowedNodeList() != null && !nrp.getAllowedNodeList().isEmpty()) {
        		Attribute allowedNodes = new BasicAttribute(REP_POLICY_ALLOWEDNODE);
        		for (NodeReference nr : nrp.getAllowedNodeList()) {
        			allowedNodes.add(nr.getValue());
        		}
        		nodeAttributes.put(allowedNodes);
        	}
        	if (nrp.getAllowedObjectFormatList() != null && !nrp.getAllowedObjectFormatList().isEmpty()) {
        		Attribute allowedFormats = new BasicAttribute(REP_POLICY_ALLOWEDOBJECTFORMAT);
        		for (ObjectFormatIdentifier formatid : nrp.getAllowedObjectFormatList()) {
        			allowedFormats.add(formatid.getValue());
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
    public List<ModificationItem> mapNodeModificationItemList(HashMap<String, NamingEnumeration<?>> attributesMap, Node node) 
    throws NamingException {
        List<ModificationItem> modificationItemList = new ArrayList<ModificationItem>();


        modificationItemList.addAll(calcModifications(NODE_NAME, attributesMap, node.getName()));
        modificationItemList.addAll(calcModifications(NODE_BASEURL, attributesMap, node.getBaseURL()));
        modificationItemList.addAll(calcModifications(NODE_DESCRIPTION, attributesMap, node.getDescription()));
        modificationItemList.addAll(calcSubjectListModifications(NODE_SUBJECT, attributesMap, node.getSubjectList()));
        modificationItemList.addAll(calcSubjectListModifications(NODE_CONTACT_SUBJECT, attributesMap, node.getContactSubjectList()));
        modificationItemList.addAll(calcModifications(NODE_REPLICATE, attributesMap, String.valueOf(node.isReplicate())));
        modificationItemList.addAll(calcModifications(NODE_SYNCHRONIZE, attributesMap, String.valueOf(node.isSynchronize())));
        modificationItemList.addAll(calcModifications(NODE_STATE, attributesMap, String.valueOf(node.getState().xmlValue())));

        if (attributesMap.containsKey(NODE_TYPE.toLowerCase())) {
            NodeType nodeType = NodeType.convert(getEnumerationValueString(attributesMap.get(NODE_TYPE.toLowerCase())));

            // Here begins the optional params, can not change node types!

            // synchronization schedules and status reports are only for MNs
            if (nodeType.compareTo(NodeType.MN) == 0) {
                log.trace("found a Membernode");
            	
                // My assumption is if d1NodeSynSchdSec does not exist, then
                // the node does not have a schedule
                if (attributesMap.containsKey(SYNC_SCHEDULE_SEC.toLowerCase()) &&
                		node.getSynchronization() == null) {
                	 // well you can't remove a schedule, but you can turn off synchronization if you want
                	log.error("Unable to remove Synchronization for " + node.getIdentifier().getValue());
                } else {
                    Schedule newSchedule = node.getSynchronization().getSchedule();
                    log.warn(String.format("%s-%s-%s-%s-%s-%s-%s", newSchedule.getWday(), newSchedule.getYear(), newSchedule.getMon(), newSchedule.getMday(), newSchedule.getHour(), newSchedule.getMin(), newSchedule.getSec()));
                    modificationItemList.addAll(calcModifications(SYNC_SCHEDULE_SEC, attributesMap, newSchedule.getSec()));
                    modificationItemList.addAll(calcModifications(SYNC_SCHEDULE_MIN, attributesMap, newSchedule.getMin()));
                    modificationItemList.addAll(calcModifications(SYNC_SCHEDULE_HOUR, attributesMap, newSchedule.getHour()));
                    modificationItemList.addAll(calcModifications(SYNC_SCHEDULE_MDAY, attributesMap, newSchedule.getMday()));
                    modificationItemList.addAll(calcModifications(SYNC_SCHEDULE_MON, attributesMap, newSchedule.getMon()));
                    modificationItemList.addAll(calcModifications(SYNC_SCHEDULE_WDAY, attributesMap, newSchedule.getWday()));
                    modificationItemList.addAll(calcModifications(SYNC_SCHEDULE_YEAR, attributesMap, newSchedule.getYear()));
                }  
                
                // default harvest dates are not set on node creation if there wasn't a synchronization
                // element at that time.  We don't want to overwrite existing ones either in the case of
                // empty values, so will only create modifications (as additions) if there are no stored values
                // (these dates get modified in specialized methods in this class)
                if (!attributesMap.containsKey(NODE_LAST_HARVESTED.toLowerCase()) &&
                		node.getSynchronization() != null) {
                	Attribute d1NodeLastHarvested = new BasicAttribute(NODE_LAST_HARVESTED, DEFAULT_NON_HARVEST_DATE);
                	modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeLastHarvested));

                	Attribute d1NodeLastCompleteHarvest = new BasicAttribute(NODE_LAST_COMPLETE_HARVEST, DEFAULT_NON_HARVEST_DATE);
                	modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1NodeLastCompleteHarvest));

                }
            }
            
            /* NodeReplicationPolicy elements and attributes */
            if (node.getNodeReplicationPolicy() != null) {
               	NodeReplicationPolicy rp = node.getNodeReplicationPolicy();
               	
            	modificationItemList.addAll(calcModifications(REP_POLICY_MAXOBJECTSIZE, attributesMap, rp.getMaxObjectSize().toString()));
            	modificationItemList.addAll(calcModifications(REP_POLICY_SPACEALLOCATED, attributesMap, rp.getSpaceAllocated().toString()));
            	modificationItemList.addAll(calcListModifications(
            			REP_POLICY_ALLOWEDNODE,
            			attributesMap, 
            			rp.getAllowedNodeList() == null ? null : CollectionUtils.collect(rp.getAllowedNodeList(),
            					new Transformer() {
            				        public Object transform(Object o) {
            					       return ((NodeReference) o).getValue();
            				        }
            			        })
                		));           		            
               	modificationItemList.addAll(calcListModifications(
            			REP_POLICY_ALLOWEDOBJECTFORMAT,
            			attributesMap, 
            			rp.getAllowedObjectFormatList() == null ? null : CollectionUtils.collect(rp.getAllowedObjectFormatList(),
                		        new Transformer() {
                			        public Object transform(Object o) {
                				       return ((ObjectFormatIdentifier) o).getValue();
                			        }
                		        })
            			));
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
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
            Attribute d1NodeLogLastAggregated = new BasicAttribute(LOG_LAST_AGGREGATED, strLogLastAggregated);
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
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
            Attribute d1NodeApproval = new BasicAttribute(NODE_APPROVED, Boolean.toString(approved).toUpperCase());
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
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
            HashMap<String, NamingEnumeration<?>> attributesMap = buildNodeAttributeMap(nodeDn);
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
            throw new ServiceFailure("0", "updateNodeCapabilities failed due to LDAP communication failure:: " + 
            		ex.getClass().getSimpleName() + ":" + ex.getMessage() + ":" + ex.getExplanation());
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
            Attribute d1ProcessingStateAttribute = new BasicAttribute(PROCESSING_STATE, processingState.getValue());
            // construct the list of modifications to make
            ModificationItem[] mods = new ModificationItem[1];
            if (getProcessingState(nodeIdentifier) == null) {
                mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE, d1ProcessingStateAttribute);
            } else {
                mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1ProcessingStateAttribute);
            }
            // make the change
            ctx.modifyAttributes(dnNodeIdentifier, mods);
            log.debug("set " + PROCESSING_STATE + ": " + dnNodeIdentifier + " to " + processingState.getValue());
        } catch (CommunicationException ex) {
            ex.printStackTrace();
            log.error("LDAP Service is unresponsive " + nodeIdentifier);
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
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
            Attribute d1NodeAggregateLogs = new BasicAttribute(AGGREGATE_LOGS, Boolean.toString(aggregateLogs).toUpperCase());
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
            throw new ServiceFailure("-1", "LDAP Service is unresponsive");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Problem Setting Aggregate Logs " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not set Aggregate Logs: " + nodeIdentifier + " " + e.getMessage());
        }
    }
    
    
    /**
     * create ModificationItems for the named single-value attribute.  There will
     * be zero or 1 modification item, depending on whether a change is required. 
     * A null newValue parameter will be treated as empty String, and will result 
     * in removal of the existing value.
     */
    //TODO: should a Remove operation happen when the newValue is emptyString or null?
    // (currently a Replace op happens and places an empty string)
    protected List<ModificationItem> calcModifications(String attributeName, HashMap<String, NamingEnumeration<?>> attributesMap, String newValue) 
    throws NamingException 
    {
    	if (newValue == null) newValue = "";
    	List<ModificationItem> modificationItemList = new ArrayList<ModificationItem>();
    	if (attributesMap.containsKey(attributeName.toLowerCase())) {
    		String currentValue = getEnumerationValueString(attributesMap.get(attributeName.toLowerCase()));
    		if (!newValue.contentEquals(currentValue)) {
    			Attribute attr = new BasicAttribute(attributeName, newValue);
    			modificationItemList.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attr));
            }
        } else {
            Attribute attr = new BasicAttribute(attributeName, newValue);
            modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attr));
        }
    	return modificationItemList;
    }
    

    
    /**
     * create ModificationItems for the named multi-value attribute.  There will
     * be [0..2] modification items, depending on whether additions, removals, 
     * or both are needed. A null newValues parameter will be treated as empty 
     * list, and will result in removal of existing value.
     * of existing values)
     * @param <E>
     * @param attributesMap
     * @param newValues
     * @param attributeName
     * @return
     * @throws NamingException
     */
    @SuppressWarnings("rawtypes")
	protected List<ModificationItem> calcListModifications(String attributeName, 
			HashMap<String, NamingEnumeration<?>> attributesMap, Collection<String> newValues)
    throws NamingException
    {
    	if (newValues == null) 
    		newValues = new ArrayList<String>();
    	List<ModificationItem> modificationItemList = new ArrayList<ModificationItem>();
    	
    	if (attributesMap.containsKey(attributeName.toLowerCase()) || !(newValues.isEmpty())) {
    		
    		List<String> existingList = new ArrayList<String>();
    		if (attributesMap.containsKey(attributeName.toLowerCase())) {
    			NamingEnumeration enumeration = attributesMap.get(attributeName.toLowerCase());
    			while (enumeration.hasMoreElements()) {
    				existingList.add((String)enumeration.nextElement());
    			}
    		}
    		// Determine which attributes to add
    		// added attributes should be the items in the new subject list
    		// minus the same items in the existing list
    		List<String> additionList = new ArrayList<String>();
    		additionList.addAll(newValues);

    		if (!(existingList.isEmpty()) && !(additionList.isEmpty())) {
    			additionList.removeAll(existingList);
    		}

    		if (!additionList.isEmpty()) {
    			Attribute additions = new BasicAttribute(attributeName);

    			for (String addition : additionList) {
    				additions.add(addition);
    			}
    			modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, additions));
    		}

    		// Determine which attributes to remove
    		// removed attributes should be the items in the existing subject list
    		// minus the same items in the new subject list
    		List<String> removalList = new ArrayList<String>();
    		removalList.addAll(existingList);
    		if (!(newValues.isEmpty()) && !(removalList.isEmpty())) {
    			removalList.removeAll(newValues);
    		}

    		if (!removalList.isEmpty()) {
    			Attribute removals = new BasicAttribute(attributeName);

    			for (String removal : removalList) {
    				removals.add(removal);
    			}
    			modificationItemList.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, removals));
    		}
    	}
    	return modificationItemList;
    }
    
    /**
     * create ModificationItems for the named multi-value attribute.  There will
     * be [0..2] modification items, depending on whether additions, removals, 
     * or both are needed. A null newValues parameter will be treated as empty 
     * list, and will result in removal of existing value.
     * of existing values)
     * @param attributesMap
     * @param newValues
     * @param attributeName
     * @return
     * @throws NamingException
     */
	protected List<ModificationItem> calcSubjectListModifications(String attributeName, 
			HashMap<String, NamingEnumeration<?>> attributesMap, List<Subject> newValues)
    throws NamingException
    {
    	if (newValues == null) 
    		newValues = new ArrayList<Subject>();
    	List<ModificationItem> modificationItemList = new ArrayList<ModificationItem>();
    	
    	if (attributesMap.containsKey(attributeName.toLowerCase()) || !(newValues.isEmpty())) {
    		
    		List<Subject> existingList = new ArrayList<Subject>();
    		if (attributesMap.containsKey(attributeName.toLowerCase())) {
    			NamingEnumeration<?> enumeration = attributesMap.get(attributeName.toLowerCase());
    			while (enumeration.hasMoreElements()) {
    				Subject s = new Subject();
    				s.setValue((String)enumeration.nextElement());
    				existingList.add(s);
    			}
    		}
    		// Determine which attributes to add
    		// added attributes should be the items in the new subject list
    		// minus the same items in the existing list
    		List<Subject> additionList = new ArrayList<Subject>();
    		additionList.addAll(newValues);

    		if (!(existingList.isEmpty()) && !(additionList.isEmpty())) {
    			additionList.removeAll(existingList);
    		}

    		if (!additionList.isEmpty()) {
    			Attribute additions = new BasicAttribute(attributeName);

    			for (Subject addition : additionList) {
    				additions.add(addition.getValue());
    			}
    			modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, additions));
    		}

    		// Determine which attributes to remove
    		// removed attributes should be the items in the existing subject list
    		// minus the same items in the new subject list
    		List<Subject> removalList = new ArrayList<Subject>();
    		removalList.addAll(existingList);
    		if (!(newValues.isEmpty()) && !(removalList.isEmpty())) {
    			removalList.removeAll(newValues);
    		}

    		if (!removalList.isEmpty()) {
    			Attribute removals = new BasicAttribute(attributeName);

    			for (Subject removal : removalList) {
    				removals.add(removal.getValue());
    			}
    			modificationItemList.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, removals));
    		}
    	}
    	return modificationItemList;
    }
}
