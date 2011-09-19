package org.dataone.cn.service.ldap.tests.v1;

import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Schedule;
import java.util.Date;
import org.dataone.service.types.v1.Synchronization;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
import java.util.List;
import org.dataone.service.types.v1.Node;
import java.util.ArrayList;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.util.TypeMarshaller;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeList;
import org.jibx.runtime.JiBXException;
import org.junit.Test;
import static org.junit.Assert.*;

public class NodeRegistryServiceTest {

    public static Log log = LogFactory.getLog(NodeRegistryServiceTest.class);
    NodeRegistryService nodeRegistryService = new NodeRegistryService();

    public NodeRegistryServiceTest() {
        org.apache.log4j.BasicConfigurator.configure();
    }

    @Test
    public void testRegisterAndDeleteNode() throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, IdentifierNotUnique, NotFound {
        List<Node> testNodeList = new ArrayList<Node>();
        Node testCNNode = new Node();
        testCNNode.setName("testThisCN");
        testCNNode.setDescription("this is a test");
        testCNNode.setBaseURL("https://test.this.stuff/cn");
        testCNNode.setReplicate(false);
        testCNNode.setSynchronize(false);
        testCNNode.setType(NodeType.CN);
        testCNNode.setState(NodeState.UP);
        NodeReference cnNodeReference = nodeRegistryService.register(testCNNode);
        assertNotNull(cnNodeReference);
        testNodeList.add(testCNNode);

        Node testMNNode = new Node();
        testMNNode.setName("testThisMN");
        testMNNode.setDescription("this is a test");
        testMNNode.setBaseURL("https://test.this.stuff/mn");
        testMNNode.setReplicate(true);
        testMNNode.setSynchronize(true);
        testMNNode.setType(NodeType.MN);
        testMNNode.setState(NodeState.UP);
        Synchronization synchronization = new Synchronization();

        synchronization.setLastCompleteHarvest(new Date());
        synchronization.setLastHarvested(new Date());
        Schedule schedule = new Schedule();
        schedule.setSec("0");
        schedule.setMin("05");
        schedule.setHour("01");
        schedule.setMon("*");
        schedule.setMday("*");
        schedule.setWday("?");
        schedule.setYear("*");
        synchronization.setSchedule(schedule);

        testMNNode.setSynchronization(synchronization);

        Service service1 = new Service();
        service1.setName("MNCore");
        service1.setVersion("0.6.4");
        service1.setAvailable(Boolean.TRUE);

        Services services = new Services();
        services.addService(service1);

        Service service2 = new Service();
        service2.setName("MNRead");
        service2.setVersion("0.6.4");
        service2.setAvailable(Boolean.TRUE);
        services.addService(service2);

        testMNNode.setServices(services);
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        assertNotNull(mnNodeReference);
        testNodeList.add(testMNNode);
        log.info("created " + testNodeList.size() + " nodes");

        // try to retrieve the node again

        Node testRetrieval = nodeRegistryService.getNode(cnNodeReference.getValue());

        assertTrue(testRetrieval.getIdentifier().getValue().equalsIgnoreCase(cnNodeReference.getValue()));


        for (Node node : testNodeList) {
            if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
                for (Service service : node.getServices().getServiceList()) {
                    nodeRegistryService.deleteNodeService(node, service);
                }
            }
            nodeRegistryService.deleteNode(node);
        }
    }

    @Test
    public void testNodeList() throws NotImplemented, ServiceFailure, IOException, JiBXException {
        // retrieve it
        try {
            NodeList nodeList = nodeRegistryService.listNodes();
            Assert.assertNotNull(nodeList);
            // serialize it and validate it
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(nodeList, outputStream);
            String nodeListString = new String(outputStream.toByteArray());
            log.info(nodeListString);
        } catch (Exception e) {
            e.printStackTrace();
            fail("testNodeList");
        }
    }
}