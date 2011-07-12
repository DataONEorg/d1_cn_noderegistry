package org.dataone.cn.service.ldap.impl;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import javax.naming.directory.Attributes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.cn.CNRegister;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.Node;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.Session;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.DistinguishedName;
import javax.naming.NamingException;
import org.dataone.service.types.NodeState;
import org.dataone.service.types.NodeType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Service;

@Service("cnRegisterLDAPImpl")
@Qualifier("cnRegisterLDAP")
public class CNRegisterLDAPImpl implements CNRegister {

    public static Log log = LogFactory.getLog(CNRegisterLDAPImpl.class);
    @Autowired
    @Qualifier("ldapTemplate")
    private LdapTemplate ldapTemplate;
    static private SecureRandom random;

    static {
        // Need this or context will lowercase all the rdn s
        System.setProperty(DistinguishedName.KEY_CASE_FOLD_PROPERTY, DistinguishedName.KEY_CASE_FOLD_NONE);
        // randomly generate new Ids for register function
        try {
            random = SecureRandom.getInstance("SHA1PRNG");
        } catch (NoSuchAlgorithmException ex) {
            random = null;
            log.error(ex.getMessage());
        }
    }
    static private String alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ";
    static private String numbers = "23456789";

    @Override
    public boolean updateNodeCapabilities(Session session, NodeReference nodeid, Node node) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NodeReference register(Session session, Node node) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, IdentifierNotUnique {


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
        // for a max of 25+ Billion 
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
            newNodeId = generateId(randomGenArrayLength);
        } while (nodeIds.contains(newNodeId));

        // because we use a base DN, only need to supply the RDN
        DistinguishedName dn = new DistinguishedName();
        dn.add("d1NodeId", newNodeId);

        NodeReference newNodeReference = new NodeReference();
        newNodeReference.setValue(newNodeId);
        node.setIdentifier(newNodeReference);
        DirContextAdapter context = new DirContextAdapter(dn);

        mapNodeToContext(node, context);
        ldapTemplate.bind(dn, context, null);
        if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
            for (org.dataone.service.types.Service service : node.getServices().getServiceList()) {
                String d1NodeServiceId = service.getName() + "-" + service.getVersion();
                DistinguishedName dnService = new DistinguishedName();
                dnService.add("d1NodeId", newNodeId);
                dnService.add("d1NodeServiceId", d1NodeServiceId);
                context = new DirContextAdapter(dnService);
                mapServiceToContext(service, newNodeId, d1NodeServiceId, context);

            }
        }
        return newNodeReference;
    }

    /**
     * calls the Spring ldapTemplate search method to map
     * objectClass d1Nodes to dataone Node objects
     * and return a list of them in order to compose a NodeList object
     *
     * @author rwaltz
     */
    private List<String> getAllNodeIds() {
        return ldapTemplate.search("", "(objectClass=d1Node)", new NodeAttributesMapper());
    }

    /**
     * return nodeIds of created nodes
     * order to create a list of IDs
     *
     * @author rwaltz
     */
    private class NodeAttributesMapper implements AttributesMapper {

        @Override
        public Object mapFromAttributes(Attributes attrs) throws NamingException {
            String nodeId = (String) attrs.get("d1NodeId").get();

            return nodeId;
        }
    }

    protected void mapServiceToContext(org.dataone.service.types.Service service, String nodeId, String nodeServiceId, DirContextOperations context) {
        context.setAttributeValue("objectclass", "d1NodeService");
        context.setAttributeValue("d1NodeServiceId", nodeServiceId);
        context.setAttributeValue("d1NodeId", nodeId);

        context.setAttributeValue("d1NodeServiceName", service.getName());
        context.setAttributeValue("d1NodeServiceVersion", service.getVersion());
        context.setAttributeValue("d1NodeServiceAvailable", Boolean.toString(service.getAvailable()).toUpperCase());
    }

    protected void mapNodeToContext(Node node, DirContextOperations context) {

        context.setAttributeValue("objectclass", "d1Node");
        context.setAttributeValue("d1NodeId", node.getIdentifier().getValue());
        context.setAttributeValue("d1NodeName", node.getName());
        context.setAttributeValue("d1NodeDescription", node.getDescription());
        context.setAttributeValue("d1NodeBaseURL", node.getBaseURL());
        context.setAttributeValue("d1NodeReplicate", Boolean.toString(node.isReplicate()).toUpperCase());
        context.setAttributeValue("d1NodeSynchronize", Boolean.toString(node.isSynchronize()).toUpperCase());
        context.setAttributeValue("d1NodeType", node.getType().toString());

        // Any other attributes are membernode only attributes

        // synchronization schedules and status reports are only for MNs
        if (node.getType().compareTo(NodeType.MN) == 0) {
            // If there is  synchronization
            if (node.getSynchronization() != null) {

                context.setAttributeValue("d1NodeSynSchdSec", node.getSynchronization().getSchedule().getSec());
                context.setAttributeValue("d1NodeSynSchdMin", node.getSynchronization().getSchedule().getMin());
                context.setAttributeValue("d1NodeSynSchdHour", node.getSynchronization().getSchedule().getHour());
                context.setAttributeValue("d1NodeSynSchdMday", node.getSynchronization().getSchedule().getMday());
                context.setAttributeValue("d1NodeSynSchdMon", node.getSynchronization().getSchedule().getMon());
                context.setAttributeValue("d1NodeSynSchdWday", node.getSynchronization().getSchedule().getWday());
                context.setAttributeValue("d1NodeSynSchdYear", node.getSynchronization().getSchedule().getYear());
                context.setAttributeValue("d1NodeLastHarvested", "1900-01-01T00:00:00Z");
                context.setAttributeValue("d1NodeLastCompleteHarvest", "1900-01-01T00:00:00Z");

            }
            // MN Node Health, check ping and status
            // My assumption is if d1NodeState does not exist, then
            // the node does not have a status
            if (node.getHealth() == null) {

                context.setAttributeValue("d1NodeState", NodeState.UP.toString());
                context.setAttributeValue("d1NodeStatusSuccess", "TRUE");
                context.setAttributeValue("d1NodeStatusDateChecked", "1900-01-01T00:00:00Z");
                context.setAttributeValue("d1NodePingSuccess", "TRUE");
                context.setAttributeValue("d1NodePingDateChecked", "1900-01-01T00:00:00Z");


            } else {
                context.setAttributeValue("d1NodeState", node.getHealth().getState().toString());

                context.setAttributeValue("d1NodeStatusSuccess", Boolean.toString(node.getHealth().getStatus().getSuccess()));
                context.setAttributeValue("d1NodeStatusDateChecked", "1900-01-01T00:00:00Z");
                context.setAttributeValue("d1NodePingSuccess", Boolean.toString(node.getHealth().getPing().getSuccess()));
                context.setAttributeValue("d1NodePingDateChecked", "1900-01-01T00:00:00Z");
            }
        }
    }

    private String generateId(int randomGenArrayLength) {
        if (randomGenArrayLength == 4) {
            char randString[] = new char[4];
            randString[0] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[1] = numbers.charAt(random.nextInt(numbers.length()));
            randString[2] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[3] = numbers.charAt(random.nextInt(numbers.length()));
            return new String(randString);
        } else {
            char randString[] = new char[6];
            randString[0] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[1] = numbers.charAt(random.nextInt(numbers.length()));
            randString[2] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[3] = numbers.charAt(random.nextInt(numbers.length()));
            randString[4] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[5] = numbers.charAt(random.nextInt(numbers.length()));
            return new String(randString);
        }
    }

    public void deleteNode(Node node) {
        DistinguishedName dn = new DistinguishedName();
        dn.add("d1NodeId", node.getIdentifier().getValue());
        log.info("deleting : " + dn.toString());
        ldapTemplate.unbind(dn);
    }

    public void deleteNodeService(Node node, org.dataone.service.types.Service service) {
        String d1NodeServiceId = service.getName() + "-" + service.getVersion();
        DistinguishedName dn = new DistinguishedName();
        dn.add("d1NodeId", node.getIdentifier().getValue());
        dn.add("d1NodeServiceId", d1NodeServiceId);
        log.info("deleting : " + dn.toString());
        ldapTemplate.unbind(dn);
    }

    public LdapTemplate getLdapTemplate() {
        return ldapTemplate;
    }

    public void setLdapTemplate(LdapTemplate ldapTemplate) {
        this.ldapTemplate = ldapTemplate;
    }
}
