/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.ldap;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
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
     * @author waltz
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
     * @author waltz
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
     * @author waltz
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
     * @author waltz
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
        } catch (Exception e) {
            log.error("Problem search Nodes for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allNodeIds;
    }

    /**
     * return a list of Nodes that are marked approved in LDAP
     *
     * @return List<Node>
     * @throws ServiceFailure
     * @author waltz
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
        } catch (Exception e) {
            log.error("Problem search Nodes for Nodelist", e);
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
     * @author waltz
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
        } catch (Exception e) {
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
     * @author waltz
     */
    public Date getLogLastAggregated(NodeReference nodeIdentifier) throws ServiceFailure {
        Date logLastAggregated = null;
        try {
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey("d1nodeloglastaggregated")) {
                logLastAggregated = DateTimeMarshaller.deserializeDateToUTC(getEnumerationValueString(attributesMap.get("d1nodeloglastaggregated")));
            }
        } catch (Exception e) {
            log.error("Problem approving node " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
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
     * @author waltz
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
     * determine if an Administrator has approved the node after it was registered
     *
     * @param nodeIdentifier
     * @return Boolean
     * @throws ServiceFailure
     * @author waltz
     */
    public Boolean getNodeApproved(NodeReference nodeIdentifier) throws ServiceFailure {
        Boolean nodeApproved = false;
        try {
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(NodeApprovedAttribute.toLowerCase())) {
                nodeApproved = Boolean.getBoolean(getEnumerationValueString(attributesMap.get(NodeApprovedAttribute.toLowerCase())));
            }
        } catch (Exception e) {
            log.error("Problem determining state " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not determine state of : " + nodeIdentifier + " " + e.getMessage());
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
     * @author waltz
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
        } catch (Exception e) {
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
     * @author waltz
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
        } catch (Exception e) {
            log.error("Problem search Nodes for Nodelist", e);
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
     * @author waltz
     */
    public ProcessingState getProcessingState(NodeReference nodeIdentifier) throws ServiceFailure {
        ProcessingState processingState = null;
        try {
            HashMap<String, NamingEnumeration> attributesMap = buildNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey(ProcessingStateAttribute.toLowerCase())) {
                processingState = ProcessingState.convert(getEnumerationValueString(attributesMap.get(ProcessingStateAttribute.toLowerCase())));
            }
        } catch (Exception e) {
            log.error("Problem determining state " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not determine state of : " + nodeIdentifier + " " + e.getMessage());
        }
        return processingState;
    }

    /**
     * from the provided attributeMap returned from an LDAP query, fill out a Node
     * NodeServices are not including in this mapping
     *
     * @param attributesMap
     * @return Node
     * @throws NamingException
     * @author waltz
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
     * @author waltz
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
     * @author waltz
     */
    public List<ModificationItem> mapNodeModificationItemList(HashMap<String, NamingEnumeration> attributesMap, Node node) throws NamingException {
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

                        Attribute d1odeSynSchdMin = new BasicAttribute("d1NodeSynSchdMin", node.getSynchronization().getSchedule().getMin());
                        modificationItemList.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, d1odeSynSchdMin));

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

                    }
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
     * @author waltz
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
        } catch (Exception e) {
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
     * @author waltz
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
        } catch (Exception e) {
            log.error("Problem setting LogLastAggregated " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
        }
    }

    /**
     * update a registered DataONE Node to be approved
     *
     *
     * @param nodeIdentifier
     * @param approved
     * @throws ServiceFailure
     * @author waltz
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
        } catch (Exception e) {
            log.error("Problem approving node " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
        }
    }

    /**
     * create or update a DataONE Node
     *
     *
     * @param node
     * @throws ServiceFailure
     * @throws NotImplemented
     * @throws InvalidRequest
     * @throws NotFound
     * @author waltz
     */
    public void setNode(Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
        String dnNodeIdentifier = buildNodeDN(node.getIdentifier());
        try {
            DirContext ctx = getContext();
            ctx.lookup(dnNodeIdentifier);
            // got this far, must be a valid entry, so update
            try {
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
            } catch (NamingException ex1) {
                throw new ServiceFailure("0", "updateNodeCapabilities failed " + ex1.getMessage());
            }
        } catch (NamingException ex) {
            // NamingException hopefully means that it does not exist, so try a create
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
                throw new ServiceFailure("0", "Register failed " + ex1.getMessage());
            }
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
     * @author waltz
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
        } catch (Exception e) {
            log.error("Problem setting LogLastAggregated " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
        }
    }
}