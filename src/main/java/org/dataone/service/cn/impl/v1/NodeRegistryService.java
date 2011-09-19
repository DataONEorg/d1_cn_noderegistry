/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.service.cn.impl.v1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.dataone.service.exceptions.IdentifierNotUnique;
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
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Subject;
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
                Services services = new Services();
                services.setServiceList(serviceList);
                node.setServices(services);
            }
        }
        nodeList.setNodeList(allNodes);
        return nodeList;
    }

    public Node getNode(String nodeIdentifier) throws NotImplemented, ServiceFailure, NotFound {

        String dnNodeIdentifier = "cn=" + nodeIdentifier + ",dc=dataone,dc=org";
        Node node;
        try {
            node = this.findNode(dnNodeIdentifier);
        } catch (NamingException ex) {
            throw new ServiceFailure("4801", ex.getMessage());
        }


        log.debug(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
        List<Service> serviceList = this.getAllServices(nodeIdentifier);
        if (!serviceList.isEmpty()) {
            Services services = new Services();
            services.setServiceList(serviceList);
            node.setServices(services);
        }

        return node;
    }

    private Node findNode(String nodeDN) throws ServiceFailure, NotImplemented, NotFound, NamingException {

        HashMap<String, NamingEnumeration> attributesMap = new HashMap<String, NamingEnumeration>();
        try {
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
        } catch (Exception e) {
            String msg = "Problem looking up entry: " + nodeDN + " : " + e.getMessage();
            log.error(msg, e);
            throw new ServiceFailure("4801", msg);
        }
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
                    ctx.search(this.base, "(objectClass=d1Node)", ctls);

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
                    log.info("found attributeName: " + attributeName);
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
    private List<String> getAllNodeIds() throws ServiceFailure {
        List<String> allNodeIds = new ArrayList<String>();
        try {
            DirContext ctx = getContext();
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            NamingEnumeration<SearchResult> results =
                    ctx.search(base, "(objectClass=d1Node)", ctls);

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

    private String getEnumerationValueString(NamingEnumeration namingEnum) throws NamingException {
        if (namingEnum.hasMore()) {
            return (String) namingEnum.next();
        } else {
            return "";
        }
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
                log.debug("Search result found for: " + nodeDn);

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

    private Service mapService(HashMap<String, String> attributesMap) {
        Service service = new Service();
        service.setName(attributesMap.get("d1nodeservicename"));
        service.setVersion(attributesMap.get("d1nodeserviceversion"));
        service.setAvailable(Boolean.valueOf(attributesMap.get("d1nodeserviceavailable")));
        return service;
    }

    public void updateLastHarvested(String nodeIdentifier, Node node) throws ServiceFailure {
        try {
            String dnNodeIdentifier = "cn=" + nodeIdentifier + ",dc=dataone,dc=org";
            Attribute d1NodeLastHarvested = new BasicAttribute("d1NodeLastHarvested", DateTimeMarshaller.serializeDateToUTC(node.getSynchronization().getLastHarvested()));
            // get a handle to an Initial DirContext
            DirContext ctx = getContext();

            // construct the list of modifications to make
            ModificationItem[] mods = new ModificationItem[1];
            mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, d1NodeLastHarvested);

            // make the change
            ctx.modifyAttributes(dnNodeIdentifier, mods);
            log.debug("Updated entry: " + dnNodeIdentifier);
        } catch (Exception e) {
            log.error("Problem updating lastHarvested for node " + node.getIdentifier().getValue(), e);
            throw new ServiceFailure("4801", "Could not update account: " + e.getMessage());
        }
    }

    public NodeReference register(Node node) throws NotImplemented, ServiceFailure, IdentifierNotUnique {
        try {
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
            node.setIdentifier(newNodeReference);

            String dnNodeIdentifier = buildNodeDN(node);
            Attributes nodeAttributes = buildNodeAttributes(node);
            DirContext ctx = getContext();
            ctx.createSubcontext(dnNodeIdentifier, nodeAttributes);
            log.debug("Added Node entry " + dnNodeIdentifier);

            if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
                for (org.dataone.service.types.v1.Service service : node.getServices().getServiceList()) {
                    String serviceDN = buildNodeServiceDN(node, service);
                    Attributes serviceAttributes = buildNodeServiceAttributes(node, service);
                    ctx.createSubcontext(serviceDN, serviceAttributes);
                    log.debug("Added Node Service entry " + serviceDN);
                }
            }
            return newNodeReference;
        } catch (NamingException ex) {
            log.error("Problem registering node " + node.getName(), ex);
            throw new ServiceFailure("4801", ex.getMessage());
        }

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
        // Any other attributes are membernode only attributes

        if (!node.getSubjectList().isEmpty()) {
            Attribute subjects = new BasicAttribute("subject");
            for (Subject subject : node.getSubjectList()) {
                subjects.add(subject.getValue());
            }
            nodeAttributes.put(subjects);
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
        String nodeServiceId = service.getName() + "-" + service.getVersion();
        serviceAttributes.put(new BasicAttribute("objectclass", "d1NodeService"));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceId", nodeServiceId));
        serviceAttributes.put(new BasicAttribute("d1NodeId", node.getIdentifier().getValue()));

        serviceAttributes.put(new BasicAttribute("d1NodeServiceName", service.getName()));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceVersion", service.getVersion()));
        serviceAttributes.put(new BasicAttribute("d1NodeServiceAvailable", Boolean.toString(service.getAvailable()).toUpperCase()));
        return serviceAttributes;
    }

    public boolean updateNodeCapabilities(NodeReference nodeid, Node node) throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public Boolean deleteNode(Node node) {
        return super.removeEntry(buildNodeDN(node));
    }

    public Boolean deleteNodeService(Node node, Service service) {

        return super.removeEntry(buildNodeServiceDN(node, service));
    }
    private String buildNodeServiceDN(Node node, Service service) {
        String d1NodeServiceId = service.getName() + "-" + service.getVersion();
         String serviceDN = "d1NodeServiceId=" + d1NodeServiceId + ",cn=" + node.getIdentifier().getValue() + ",dc=dataone,dc=org";
         return serviceDN;
    }
    private String buildNodeDN(Node node) {
       return "cn=" + node.getIdentifier().getValue() + ",dc=dataone,dc=org";
    }
}
