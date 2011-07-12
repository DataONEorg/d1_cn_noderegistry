/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.service.ldap.impl;

import java.io.InputStream;
import java.util.Date;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.cn.CNCore;
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
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Node;
import org.dataone.service.types.NodeHealth;
import org.dataone.service.types.NodeList;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.NodeState;
import org.dataone.service.types.NodeType;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectFormatIdentifier;
import org.dataone.service.types.ObjectFormatList;
import org.dataone.service.types.Ping;
import org.dataone.service.types.Schedule;
import org.dataone.service.types.Services;
import org.dataone.service.types.Session;
import org.dataone.service.types.Status;
import org.dataone.service.types.Synchronization;
import org.dataone.service.types.SystemMetadata;
import org.dataone.service.types.util.ServiceTypeUtil;
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

    @Override
    public org.dataone.service.types.Log getLogRecords(Session session, Date fromDate, Date toDate, Event event, Integer start, Integer count) throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        NodeList nodeList = new NodeList();

        List<Node> allNodes = this.getAllNodes();
        for (Node node : allNodes) {

            String nodeIdentifier = node.getIdentifier().getValue();
            log.debug(nodeIdentifier + " " + node.getName() + " " + node.getBaseURL() + " " + node.getBaseURL());
            List<org.dataone.service.types.Service> serviceList = this.getAllServices(nodeIdentifier);
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
    public Identifier reserveIdentifier(Session session, Identifier pid, String scope, String format) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier create(Session session, Identifier pid, InputStream object, SystemMetadata sysmeta) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean registerSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidSystemMetadata {
        throw new UnsupportedOperationException("Not supported yet.");
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

                    synchronization.setLastHarvested(ServiceTypeUtil.deserializeDateToUTC((String) attrs.get("d1NodeLastHarvested").get()));
                    synchronization.setLastCompleteHarvest(ServiceTypeUtil.deserializeDateToUTC((String) attrs.get("d1NodeLastCompleteHarvest").get()));
                    node.setSynchronization(synchronization);
                }
                // MN Node Health, check ping and status
                // My assumption is if d1NodeState does not exist, then
                // the node does not have a status
                Attribute d1NodeState = attrs.get("d1NodeState");
                if ((d1NodeState != null) && (d1NodeState.get() != null)) {
                    NodeHealth nodeHealth = new NodeHealth();
                    nodeHealth.setState(NodeState.convert((String) d1NodeState.get()));

                    Status status = new Status();
                    status.setSuccess(Boolean.valueOf((String) attrs.get("d1NodeStatusSuccess").get()));
                    status.setDateChecked(ServiceTypeUtil.deserializeDateToUTC((String) attrs.get("d1NodeStatusDateChecked").get()));
                    nodeHealth.setStatus(status);

                    Ping ping = new Ping();
                    ping.setSuccess(Boolean.valueOf((String) attrs.get("d1NodePingSuccess").get()));
                    ping.setLastSuccess(ServiceTypeUtil.deserializeDateToUTC((String) attrs.get("d1NodePingDateChecked").get()));
                    nodeHealth.setPing(ping);

                    node.setHealth(nodeHealth);

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
    private List<org.dataone.service.types.Service> getAllServices(String nodeId) {
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
            org.dataone.service.types.Service service = new org.dataone.service.types.Service();
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
