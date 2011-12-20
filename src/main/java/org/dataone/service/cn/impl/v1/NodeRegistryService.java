/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.service.cn.impl.v1;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.ldap.LDAPService;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Ping;
import org.dataone.service.types.v1.Schedule;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectList;
import org.dataone.service.types.v1.Synchronization;
import org.dataone.service.util.DateTimeMarshaller;

/**
 *
 * @author waltz
 */
public class NodeRegistryService extends LDAPService {

    public static Log log = LogFactory.getLog(NodeRegistryService.class);

    public NodeRegistryService() {
        // we need to use a different base for the ids
        this.setBase(Settings.getConfiguration().getString("nodeRegistry.ldap.base"));
    }

    @Override
    public void setBase(String base) {
        this.base = base;
    }

    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        NodeList nodeList = new NodeList();

        List<Node> allNodes = this.getAllNodes();
        log.info("found " + allNodes.size() + " nodes");
        for (Node node : allNodes) {

            String nodeIdentifier = node.getIdentifier().getValue();
            log.info(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
            List<Service> serviceList = this.getAllServices(nodeIdentifier);
            if (!serviceList.isEmpty()) {
                for (Service service : serviceList) {
                    String nodeServiceId = buildNodeServiceId(service);;
                    log.info("\t has service " + nodeServiceId);
                    List<ServiceMethodRestriction> restrictionList = this.getServiceMethodRestrictions(nodeIdentifier,nodeServiceId);
                    for (ServiceMethodRestriction restrict : restrictionList) {
                        log.info("\t\t has restriction" + restrict.getMethodName());
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

    public Node getNode(NodeReference nodeIdentifier) throws ServiceFailure, NotFound {

        String dnNodeIdentifier = "cn=" + nodeIdentifier.getValue() + ",dc=dataone,dc=org";
        Node node = null;
        try {
            node = this.findNode(dnNodeIdentifier);
        } catch (NameNotFoundException ex) {
            log.warn("Node not found: " + nodeIdentifier.getValue());
            throw new NotFound("4842", ex.getMessage());
        } catch (NamingException ex) {
            throw new ServiceFailure("4842", ex.getMessage());
        }


        log.debug(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
        List<Service> serviceList = this.getAllServices(nodeIdentifier.getValue());
        if (!serviceList.isEmpty()) {
                for (Service service : serviceList) {
                    String nodeServiceId = buildNodeServiceId(service);

                    service.setRestrictionList(this.getServiceMethodRestrictions(node.getIdentifier().getValue(),nodeServiceId));
                }
            Services services = new Services();
            services.setServiceList(serviceList);
            node.setServices(services);

        }

        return node;
    }

    public NodeReference register(Node node) throws ServiceFailure {
        try {
            NodeReference nodeId = generateNodeIdentifier();
            node.setIdentifier(nodeId);
            String dnNodeIdentifier = buildNodeDN(node.getIdentifier());
            Attributes nodeAttributes = buildNodeAttributes(node);
            DirContext ctx = getContext();
            ctx.createSubcontext(dnNodeIdentifier, nodeAttributes);
            log.debug("Added Node entry " + dnNodeIdentifier);

            if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
                for (Service service : node.getServices().getServiceList()) {
                    String serviceDN = buildNodeServiceDN(node.getIdentifier(), service);
                    Attributes serviceAttributes = buildNodeServiceAttributes(node, service);
                    ctx.createSubcontext(serviceDN, serviceAttributes);
                    log.debug("Added Node Service entry " + serviceDN);
                    if ((service.getRestrictionList() != null) && (service.getRestrictionList().size() > 0)) {
                        for (ServiceMethodRestriction restriction : service.getRestrictionList()) {
                            String serviceMethodRestrictionDN = buildServiceMethodRestrictionDN(node.getIdentifier(), service, restriction);
                            Attributes serviceMethodRestrictionAttributes = buildServiceMethodRestrictionAttributes(node, service, restriction);
                            ctx.createSubcontext(serviceMethodRestrictionDN, serviceMethodRestrictionAttributes);
                            log.debug("Added Service Method Restriction entry " + serviceMethodRestrictionDN);
                        }
                    }
                }
            }
            return nodeId;
        } catch (Exception ex) {
            log.error("Problem registering node " + node.getName(), ex);
            throw new ServiceFailure("4801", ex.getMessage());
        }

    }

    public NodeReference generateNodeIdentifier() throws ServiceFailure {
        // this counting method is fine, but I believe
        // it might become very inefficient
        // once we get into the tens of thousands of nodes
        List<String> nodeIds = getAllNodeIds();
        // Generaate a unique Id that is not in current Node List
        // Combinations of the basic generation method allows for
        // 160K permutations of a string using at most 4 chars.
        // however, we will allow up to 6 chars for a max of
        // 64,000,000 but we do not expect ever to reach this max.
        // and if we do, just alter the code to extend another 2 chars
        // for a max of 25+ Billion, assuming there are datastructures
        // that can manage that # of ids
        //
        // As a note, DataONE has some reserved Ids like c1d1 that will
        // add to node list, and the generator will never get into infinite loop
        //

        int randomGenArrayLength = 4;
        if (nodeIds.size() >= 160000) {
            randomGenArrayLength = 6;
        } else if (nodeIds.size() >= 64000000) {
            // WoW 64 Million+ nodes?? we hit the jackpot!
            throw new ServiceFailure("233", "Unable to allocate 64000001th Node Id");
        }
        String newNodeId = "";
        // if we hit a duplicate we want to keep generating until a
        // unique id is found
        do {
            newNodeId = NodeIdentifierGenerator.generateId(randomGenArrayLength);
        } while (nodeIds.contains(newNodeId));
        NodeReference newNodeReference = new NodeReference();
        newNodeReference.setValue(newNodeId);

        return newNodeReference;

    }

    private Node findNode(String nodeDN) throws NotFound, NamingException, NameNotFoundException {

        HashMap<String, NamingEnumeration> attributesMap = getNodeAttributeMap(nodeDN);
        log.debug("Retrieved SubjectList for: " + nodeDN);

        if (attributesMap.isEmpty()) {
            throw new NotFound("4801", nodeDN + " not found on the server");
        }

        return this.mapNode(attributesMap);
    }

    /**
     * Find the DN for a given Identifier
     * @param pid
     * @return the DN in LDAP for the given pid
     */
    private List<Node> getAllNodes() throws ServiceFailure {
        List<Node> allNode = new ArrayList<Node>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            log.info("BASE: " + base);
            NamingEnumeration<SearchResult> results =
                    ctx.search(this.base, "(&(objectClass=d1Node)(d1NodeApproved=TRUE))", ctls);

            while (results != null && results.hasMore()) {
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();
                log.info("Search result found for: " + nodeDn);

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
    /**
     * Find the DN for a given Identifier
     * @param pid
     * @return the DN in LDAP for the given pid
     */
    public List<NodeReference> getAllPendingNodeIds() throws ServiceFailure {
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
                log.debug("Search result found for: " + nodeDn);

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
     * Find the DN for a given Identifier
     * @param pid
     * @return the DN in LDAP for the given pid
     */
    private List<String> getAllNodeIds() throws ServiceFailure {
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
                log.debug("Search result found for: " + nodeDn);

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

    private List<Service> getAllServices(String nodeIdentifier) throws ServiceFailure {
        List<Service> allServices = new ArrayList<Service>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(base, "(&(objectClass=d1NodeService)(d1NodeId=" + nodeIdentifier + "))", ctls);

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

                allServices.add(this.mapService(attributesMap));
            }
        } catch (Exception e) {
            log.error("Problem search NodesServices for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return allServices;
    }

    /**
     * Find the DN for a given Identifier
     * @param pid
     * @return the DN in LDAP for the given pid
     */
    private List<ServiceMethodRestriction> getServiceMethodRestrictions(String nodeIdentifier, String serviceIdentifier) throws ServiceFailure {
        List<ServiceMethodRestriction> serviceMethodRestrictionList = new ArrayList<ServiceMethodRestriction>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            // mapIdentity, CNIdentity-v1, R2T6, dataone.org
            // dn: d1ServiceMethodName=mapIdentity,d1NodeServiceId=CNIdentity-v1,cn=R2T6,dc=dataone,dc=org
            // d1NodeServiceId: CNIdentity-v1
            // d1AllowedSubject: cn=test3,dc=dataone,dc=org
            // d1ServiceMethodName: mapIdentity
            // objectClass: d1ServiceMethodRestriction
            // d1NodeId: R2T6

            NamingEnumeration<SearchResult> results =
                    ctx.search(this.base, "(&(&(objectClass=d1ServiceMethodRestriction)(d1NodeServiceId=" + serviceIdentifier + "))(d1NodeId=" + nodeIdentifier + "))", ctls);

            while (results != null && results.hasMore()) {
                SearchResult si = results.next();
                String nodeDn = si.getNameInNamespace();
                log.info("Search result found for: " + nodeDn);

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

                serviceMethodRestrictionList.add(this.mapServiceMethodRestriction(attributesMap));
            }
        } catch (Exception e) {
            log.error("Problem search Nodes for Nodelist", e);
            throw new ServiceFailure("-1", e.getMessage());
        }
        return serviceMethodRestrictionList;
    }

    private Node mapNode(HashMap<String, NamingEnumeration> attributesMap) throws NamingException {
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
                nodeSubject.setValue((String) subjects.next());
                node.addSubject(nodeSubject);
            }
        }
        if (attributesMap.containsKey("d1nodecontactsubject")) {
            NamingEnumeration contactSubjects = attributesMap.get("d1nodecontactsubject");
            while (contactSubjects.hasMore()) {
                Subject nodeContactSubject = new Subject();
                nodeContactSubject.setValue((String) contactSubjects.next());
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
                log.info("found a Membernode");
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

    private Service mapService(HashMap<String, String> attributesMap) {
        Service service = new Service();
        service.setName(attributesMap.get("d1nodeservicename"));
        service.setVersion(attributesMap.get("d1nodeserviceversion"));
        service.setAvailable(Boolean.valueOf(attributesMap.get("d1nodeserviceavailable")));
        return service;
    }

    private ServiceMethodRestriction mapServiceMethodRestriction(HashMap<String, NamingEnumeration> attributesMap) throws NamingException {
        ServiceMethodRestriction serviceMethodRestriction = new ServiceMethodRestriction();

        serviceMethodRestriction.setMethodName(getEnumerationValueString(attributesMap.get("d1servicemethodname")));

        if (attributesMap.containsKey("d1allowedsubject")) {
            List<Subject>  subjectList = serviceMethodRestriction.getSubjectList();

            NamingEnumeration allowSubjects = attributesMap.get("d1allowedsubject");
            while (allowSubjects.hasMore()) {
                Subject allowSubject = new Subject();
                allowSubject.setValue((String) allowSubjects.next());
                subjectList.add(allowSubject);
            }
        }

        return serviceMethodRestriction;
    }

    private Attributes buildNodeAttributes(Node node) {
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
        nodeAttributes.put(new BasicAttribute("d1NodeApproved", Boolean.toString(Boolean.FALSE).toUpperCase()));
        // Any other attributes are membernode only attributes

        if ((node.getSubjectList() != null) && (!node.getSubjectList().isEmpty())) {
            Attribute subjects = new BasicAttribute("subject");
            for (Subject subject : node.getSubjectList()) {
                subjects.add(subject.getValue());
            }
            nodeAttributes.put(subjects);
        }
        if ((node.getContactSubjectList() != null) && (!node.getContactSubjectList().isEmpty())) {
            Attribute contactSubjects = new BasicAttribute("d1NodeContactSubject");
            for (Subject contactSubject : node.getContactSubjectList()) {
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

    private Attributes buildNodeServiceAttributes(Node node, Service service) {
        Attributes serviceAttributes = new BasicAttributes();
        String nodeServiceId = buildNodeServiceId(service);
        serviceAttributes.put(new BasicAttribute("objectclass", "d1NodeService"));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceId", nodeServiceId));
        serviceAttributes.put(new BasicAttribute("d1NodeId", node.getIdentifier().getValue()));

        serviceAttributes.put(new BasicAttribute("d1NodeServiceName", service.getName()));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceVersion", service.getVersion()));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceAvailable", Boolean.toString(service.getAvailable()).toUpperCase()));
        return serviceAttributes;
    }

    private Attributes buildServiceMethodRestrictionAttributes(Node node, Service service, ServiceMethodRestriction restrict) {
        Attributes serviceAttributes = new BasicAttributes();
        String nodeServiceId = buildNodeServiceId(service);
        serviceAttributes.put(new BasicAttribute("objectclass", "d1ServiceMethodRestriction"));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceId", nodeServiceId));
        serviceAttributes.put(new BasicAttribute("d1NodeId", node.getIdentifier().getValue()));

        serviceAttributes.put(new BasicAttribute("d1ServiceMethodName", restrict.getMethodName()));
        if (restrict.getSubjectList() != null && !(restrict.getSubjectList().isEmpty())) {
            for (Subject subject : restrict.getSubjectList()) {
                serviceAttributes.put(new BasicAttribute("d1AllowedSubject", subject.getValue()));
            }
        }
        return serviceAttributes;
    }

    private String buildServiceMethodRestrictionDN(NodeReference nodeReference, Service service, ServiceMethodRestriction restrict) {
        String d1NodeServiceId = buildNodeServiceId(service);
        String serviceMethodRestrictionDN = "d1ServiceMethodName=" + restrict.getMethodName() + ",d1NodeServiceId=" + d1NodeServiceId + ",cn=" + nodeReference.getValue() + ",dc=dataone,dc=org";
        return serviceMethodRestrictionDN;
    }

    private String buildNodeServiceDN(NodeReference nodeReference, Service service) {
        String d1NodeServiceId = buildNodeServiceId(service);
        String serviceDN = "d1NodeServiceId=" + d1NodeServiceId + ",cn=" + nodeReference.getValue() + ",dc=dataone,dc=org";
        return serviceDN;
    }

    private String buildNodeDN(NodeReference nodeReference) {
        return "cn=" + nodeReference.getValue() + ",dc=dataone,dc=org";
    }

    private String buildNodeServiceId(Service service) {
        return service.getName() + "-" + service.getVersion();
    }

    private String getEnumerationValueString(NamingEnumeration namingEnum) throws NamingException {
        if (namingEnum.hasMore()) {
            return (String) namingEnum.next();
        } else {
            return "";
        }
    }

    public void updateLastHarvested(NodeReference nodeIdentifier, Date lastDateNodeHarvested) throws ServiceFailure {
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

    public void approveNode(NodeReference nodeIdentifier) throws ServiceFailure {
        try {
            String dnNodeIdentifier = buildNodeDN(nodeIdentifier);
            Attribute d1NodeApproval = new BasicAttribute("d1NodeApproved", Boolean.toString(Boolean.TRUE).toUpperCase());
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
    public Date getLogLastAggregated(NodeReference nodeIdentifier) throws ServiceFailure {
        Date logLastAggregated = null;
        try {
            HashMap<String, NamingEnumeration> attributesMap = getNodeAttributeMap(buildNodeDN(nodeIdentifier));
            if (attributesMap.containsKey("d1nodeloglastaggregated")) {
                logLastAggregated = DateTimeMarshaller.deserializeDateToUTC(getEnumerationValueString(attributesMap.get("d1nodeloglastaggregated")));
            }
        } catch (Exception e) {
            log.error("Problem approving node " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
        }
        return logLastAggregated;
    }
    public void setLogLastAggregated(NodeReference nodeIdentifier, Date logAggregationDate) throws ServiceFailure {
        try {
            String dnNodeIdentifier = buildNodeDN(nodeIdentifier);
            String strLogLastAggregated = DateTimeMarshaller.serializeDateToUTC(logAggregationDate);
            Attribute d1NodeLogLastAggregated = new BasicAttribute("d1NodeLogLastAggregated", strLogLastAggregated);
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
            log.debug("set LogLastAggregated: " + dnNodeIdentifier  + " to " + strLogLastAggregated);
        } catch (Exception e) {
            log.error("Problem setting LogLastAggregated " + nodeIdentifier, e);
            throw new ServiceFailure("4801", "Could not approve node: " + nodeIdentifier + " " + e.getMessage());
        }
    }
    public boolean updateNodeCapabilities(NodeReference nodeid, Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
        try {
            String nodeDn = buildNodeDN(nodeid);
            HashMap<String, NamingEnumeration> attributesMap = getNodeAttributeMap(nodeDn);
            ModificationItem[] modificationItems = getNodeModificationItems(attributesMap, node);
            DirContext ctx = getContext();
            ctx.modifyAttributes(nodeDn, modificationItems);

            // easiest to remove existingServices and then adding the new ones back
            List<Service> existingNodeServices = getAllServices(nodeid.getValue());
            if ((existingNodeServices != null) && !(existingNodeServices.isEmpty())) {
                for (Service removeService : existingNodeServices) {
                    String d1NodeServiceId = buildNodeServiceId(removeService);
                    List<ServiceMethodRestriction> serviceMethodRestrictionList = getServiceMethodRestrictions(nodeid.getValue(), d1NodeServiceId);
                    if ((serviceMethodRestrictionList != null) && !(serviceMethodRestrictionList.isEmpty())) {
                        for (ServiceMethodRestriction removeServiceMethodRestriction : serviceMethodRestrictionList) {
                            deleteServiceMethodRestriction(nodeid, removeService, removeServiceMethodRestriction);
                        }
                    }
                    deleteNodeService(nodeid, removeService);
                }
            }
            if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
                for (Service service : node.getServices().getServiceList()) {
                    String serviceDN = buildNodeServiceDN(node.getIdentifier(), service);
                    Attributes serviceAttributes = buildNodeServiceAttributes(node, service);
                    ctx.createSubcontext(serviceDN, serviceAttributes);
                    log.debug("updateNodeCapabilities Added Node Service entry " + serviceDN);
                    if (service.getRestrictionList() != null) {
                        for (ServiceMethodRestriction restriction : service.getRestrictionList()) {
                            String serviceMethodRestrictionDN = buildServiceMethodRestrictionDN(node.getIdentifier(), service, restriction);
                            Attributes serviceMethodRestrictionAttributes = buildServiceMethodRestrictionAttributes(node, service, restriction);
                            ctx.createSubcontext(serviceMethodRestrictionDN, serviceMethodRestrictionAttributes);
                            log.debug("updateNodeCapabilities Added Service Method Restriction entry " + serviceMethodRestrictionDN);
                        }
                    }
                }
            }
            log.debug("Updated NodeCapabilities Node: " + nodeDn);
            return true;
        } catch (NamingException ex) {
            throw new ServiceFailure("0", "updateNodeCapabilities failed " + ex.getMessage());
        }
    }

    private ModificationItem[] getNodeModificationItems(HashMap<String, NamingEnumeration> attributesMap, Node node) throws NamingException {
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
                log.error("HEY HEY HEY node state is null");
            }
            if (currentNodeState == null) {
                log.error("HEY HEY HEY currentNodeStateis null:" + getEnumerationValueString(attributesMap.get("d1nodestate")));
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
                log.info("found a Membernode");
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
        ModificationItem[] modificationArray = new ModificationItem[modificationItemList.size()];
        modificationArray = modificationItemList.toArray(modificationArray);
        return modificationArray;
    }

    private HashMap<String, NamingEnumeration> getNodeAttributeMap(String nodeDN) throws NamingException {
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
        log.debug("Retrieved SubjectList for: " + nodeDN);

        return attributesMap;

    }

    public Boolean deleteNode(NodeReference nodeReference) throws ServiceFailure {

        List<Service> services = getAllServices(nodeReference.getValue());
        if ((services != null) && (services.size() > 0)) {
            for (Service service : services) {
                log.info("deleteNode Service: " + service.getName());
                List<ServiceMethodRestriction> serviceRestrictionList = getServiceMethodRestrictions(nodeReference.getValue(), buildNodeServiceId(service));
                if (serviceRestrictionList != null) {
                    for (ServiceMethodRestriction restriction : serviceRestrictionList) {
                        log.info("deleteNode deleting " + buildServiceMethodRestrictionDN(nodeReference, service, restriction));
                        if (!deleteServiceMethodRestriction(nodeReference, service, restriction)) {

                            throw new ServiceFailure("0", "Unable to delete restriction " + buildServiceMethodRestrictionDN(nodeReference, service, restriction));
                        }
                    }
                }
                if (!deleteNodeService(nodeReference, service)) {
                    throw new ServiceFailure("0", "Unable to delete service " + buildNodeServiceDN(nodeReference, service));
                }
            }
        }
        return super.removeEntry(buildNodeDN(nodeReference));

    }

    public Boolean deleteNodeService(NodeReference nodeReference, Service service) {

        return super.removeEntry(buildNodeServiceDN(nodeReference, service));
    }

    public Boolean deleteServiceMethodRestriction(NodeReference nodeReference, Service service, ServiceMethodRestriction restrict) {

        return super.removeEntry(buildServiceMethodRestrictionDN(nodeReference, service, restrict));
    }
}
