/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.service.ldap.impl.v1;

import java.io.InputStream;
import java.util.Date;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectFormatList;
import org.dataone.service.types.v1.Ping;
import org.dataone.service.types.v1.Schedule;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Synchronization;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.DateTimeMarshaller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.filter.AndFilter;
import org.springframework.ldap.filter.EqualsFilter;
import org.springframework.stereotype.Service;

/**
 *
 * @author waltz
 */
@Service("cnCoreLDAPImpl")
@Qualifier("cnCoreLDAP")
public class CNCoreLDAPImpl implements CNCore {

    @Autowired
    @Qualifier("ldapTemplate")
    private LdapTemplate ldapTemplate;
    public static Log log = LogFactory.getLog(CNCoreLDAPImpl.class);

    @Override
    public ObjectFormatList listFormats() throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectFormat getFormat(ObjectFormatIdentifier fmtid) throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Node getNode(String nodeIdentifier) throws NotImplemented, ServiceFailure {
        NodeList nodeList = new NodeList();
        String dnNodeIdentifier = "d1NodeId=" + nodeIdentifier + ",dc=dataone";
        Node node = this.findNode(dnNodeIdentifier);


        log.debug(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
        List<org.dataone.service.types.v1.Service> serviceList = this.getAllServices(nodeIdentifier);
        if (!serviceList.isEmpty()) {
            Services services = new Services();
            services.setServiceList(serviceList);
            node.setServices(services);
        }

        return node;
    }

    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        NodeList nodeList = new NodeList();

        List<Node> allNodes = this.getAllNodes();
        for (Node node : allNodes) {

            String nodeIdentifier = node.getIdentifier().getValue();
            log.debug(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
            List<org.dataone.service.types.v1.Service> serviceList = this.getAllServices(nodeIdentifier);
            if (!serviceList.isEmpty()) {
                Services services = new Services();
                services.setServiceList(serviceList);
                node.setServices(services);
            }
        }
        nodeList.setNodeList(allNodes);
        return nodeList;
    }

    @Override
    public Identifier create(Session session, Identifier pid, InputStream object, SystemMetadata sysmeta) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * calls the Spring ldapTemplate search method to map
     * objectClass d1Node to dataone Node object
     * and return one of them in order to compose a Node object
     *
     * @author rwaltz
     */
    private Node findNode(String dn) {
        return (Node) ldapTemplate.lookup(dn, new NodeAttributesMapper());
    }

    /**
     * calls the Spring ldapTemplate search method to map
     * objectClass d1Nodes to dataone Node objects
     * and return a list of them in order to compose a NodeList object
     *
     * @author rwaltz
     */
    private List<Node> getAllNodes() {
        return ldapTemplate.search("", "(objectClass=d1Node)", new NodeAttributesMapper());
    }

    @Override
    public boolean hasReservation(Session session, Identifier pid) throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, IdentifierNotUnique, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public org.dataone.service.types.v1.Log getLogRecords(Session session, Date fromDate, Date toDate, Event event, Integer start, Integer count) throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean reserveIdentifier(Session session, Identifier pid) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier generateIdentifier(Session session, String scheme, String fragment) throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean updateSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidSystemMetadata, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier registerSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidSystemMetadata {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Used in getAllNodes search to map attributes
     * returned from the objectClass of D1Node
     * into to a dataone node class
     * 
     * @author rwaltz
     */
    private class NodeAttributesMapper implements AttributesMapper {

        @Override
        public Object mapFromAttributes(Attributes attrs) throws NamingException {
            Node node = new Node();
            //populate the mandatory attributes first
            node.setBaseURL((String) attrs.get("d1NodeBaseURL").get());
            NodeReference nodeReference = new NodeReference();
            nodeReference.setValue((String) attrs.get("d1NodeId").get());
            node.setIdentifier(nodeReference);
            node.setName((String) attrs.get("d1NodeName").get());
            node.setDescription((String) attrs.get("d1NodeDescription").get());
            node.setReplicate(Boolean.valueOf((String) attrs.get("d1NodeReplicate").get()));
            node.setSynchronize(Boolean.valueOf((String) attrs.get("d1NodeSynchronize").get()));
            node.setState(NodeState.convert((String) attrs.get("d1NodeState").get()));
            node.setType(NodeType.convert((String) attrs.get("d1NodeType").get()));

            // Here begins the optional params

            // synchronization schedules and status reports are only for MNs
            if (node.getType().compareTo(NodeType.MN) == 0) {
                // My assumption is if d1NodeSynSchdSec does not exist, then
                // the node does not have a schedule
                log.info("found a Membernode");
                Attribute d1NodeSynSchdSec = attrs.get("d1NodeSynSchdSec");
                if ((d1NodeSynSchdSec != null) && (d1NodeSynSchdSec.get() != null)) {
                    Synchronization synchronization = new Synchronization();
                    Schedule schedule = new Schedule();
                    schedule.setSec((String) attrs.get("d1NodeSynSchdSec").get());
                    schedule.setMin((String) attrs.get("d1NodeSynSchdMin").get());
                    schedule.setHour((String) attrs.get("d1NodeSynSchdHour").get());
                    schedule.setMday((String) attrs.get("d1NodeSynSchdMday").get());
                    schedule.setMon((String) attrs.get("d1NodeSynSchdMon").get());
                    schedule.setWday((String) attrs.get("d1NodeSynSchdWday").get());
                    schedule.setYear((String) attrs.get("d1NodeSynSchdYear").get());
                    synchronization.setSchedule(schedule);

                    synchronization.setLastHarvested(DateTimeMarshaller.deserializeDateToUTC((String) attrs.get("d1NodeLastHarvested").get()));
                    synchronization.setLastCompleteHarvest(DateTimeMarshaller.deserializeDateToUTC((String) attrs.get("d1NodeLastCompleteHarvest").get()));
                    node.setSynchronization(synchronization);
                }
                // MN Node Health, check ping and status
                // My assumption is if d1NodeState does not exist, then
                // the node does not have a status
                Attribute d1NodeState = attrs.get("d1NodeState");

                Attribute d1NodePingSuccess =  attrs.get("d1NodePingSuccess");
                if ((d1NodePingSuccess != null) && (d1NodePingSuccess.get() != null)) {
                    Ping ping = new Ping();
                    ping.setSuccess(Boolean.valueOf((String) attrs.get("d1NodePingSuccess").get()));
                    ping.setLastSuccess(DateTimeMarshaller.deserializeDateToUTC((String) attrs.get("d1NodePingDateChecked").get()));
                    node.setPing(ping);
                }

            }

            return node;
        }
    }

    /**
     * calls the Spring ldapTemplate search method to map
     * objectClass d1NodeService to dataone Node services
     * and return a list of them in order to compose a Services object
     * to connect to a NodeList object
     *
     * @author rwaltz
     */
    private List<org.dataone.service.types.v1.Service> getAllServices(String nodeId) {
        AndFilter filter = new AndFilter();
        filter.and(new EqualsFilter("objectclass", "d1NodeService"));
        filter.and(new EqualsFilter("d1NodeId", nodeId));

        return ldapTemplate.search("", filter.encode(), new ServiceAttributesMapper());
    }

    /**
     * Used in getAllServices search to map attributes
     * returned from the objectClass of D1NodeService
     * into to a dataone Service class
     *
     * @author rwaltz
     */
    private class ServiceAttributesMapper implements AttributesMapper {

        @Override
        public Object mapFromAttributes(Attributes attrs) throws NamingException {
            org.dataone.service.types.v1.Service service = new org.dataone.service.types.v1.Service();
            service.setName((String) attrs.get("d1NodeServiceName").get());
            service.setVersion((String) attrs.get("d1NodeServiceVersion").get());
            service.setAvailable(Boolean.valueOf((String) attrs.get("d1NodeServiceAvailable").get()));

            return service;
        }
    }

    public LdapTemplate getLdapTemplate() {
        return ldapTemplate;
    }

    public void setLdapTemplate(LdapTemplate ldapTemplate) {
        this.ldapTemplate = ldapTemplate;
    }
}
