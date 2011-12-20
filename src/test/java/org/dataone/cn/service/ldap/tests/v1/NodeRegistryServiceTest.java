package org.dataone.cn.service.ldap.tests.v1;

import org.dataone.service.types.v1.NodeReference;
import java.util.List;
import org.dataone.service.types.v1.Node;
import java.util.ArrayList;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.util.TypeMarshaller;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.types.v1.NodeList;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Services;

public class NodeRegistryServiceTest {

    public static Log log = LogFactory.getLog(NodeRegistryServiceTest.class);
    NodeRegistryService nodeRegistryService = new NodeRegistryService();
    final static int SIZE = 16384;

    @Test
    public void testRegisterListAndDeleteNode() throws Exception {
        List<Node> testNodeList = new ArrayList<Node>();
        ByteArrayOutputStream mnNodeOutput = new ByteArrayOutputStream();
        InputStream is = this.getClass().getResourceAsStream("/org/dataone/cn/resources/samples/v1/mnNode.xml");

        BufferedInputStream bInputStream = new BufferedInputStream(is);
        byte[] barray = new byte[SIZE];
        int nRead = 0;
        while ((nRead = bInputStream.read(barray, 0, SIZE)) != -1) {
            mnNodeOutput.write(barray, 0, nRead);
        }
        bInputStream.close();
        ByteArrayInputStream bArrayInputStream = new ByteArrayInputStream(mnNodeOutput.toByteArray());
        Node testMNNode = TypeMarshaller.unmarshalTypeFromStream(Node.class, bArrayInputStream);

        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        assertNotNull(mnNodeReference);
        testMNNode.setIdentifier(mnNodeReference);
        testNodeList.add(testMNNode);


        ByteArrayOutputStream cnNodeOutput = new ByteArrayOutputStream();
        is = this.getClass().getResourceAsStream("/org/dataone/cn/resources/samples/v1/cnNode.xml");

        bInputStream = new BufferedInputStream(is);
        barray = new byte[SIZE];
        nRead = 0;
        while ((nRead = bInputStream.read(barray, 0, SIZE)) != -1) {
            cnNodeOutput.write(barray, 0, nRead);
        }
        bInputStream.close();
        bArrayInputStream = new ByteArrayInputStream(cnNodeOutput.toByteArray());
        Node testCNNode = TypeMarshaller.unmarshalTypeFromStream(Node.class, bArrayInputStream);


        NodeReference cnNodeReference = nodeRegistryService.register(testCNNode);
        assertNotNull(cnNodeReference);
        testCNNode.setIdentifier(cnNodeReference);
        testNodeList.add(testCNNode);

        Node testCNRetrieval = nodeRegistryService.getNode(cnNodeReference);

        ByteArrayOutputStream outputTestCNStream = new ByteArrayOutputStream();
        TypeMarshaller.marshalTypeToOutputStream(testCNRetrieval, outputTestCNStream);
        String testCNNodeString = new String(outputTestCNStream.toByteArray());
        log.info(testCNNodeString);

        assertTrue(testCNRetrieval.getIdentifier().getValue().equalsIgnoreCase(cnNodeReference.getValue()));
        for (Node node : testNodeList) {

            nodeRegistryService.approveNode(node.getIdentifier());
        }
        try {
            NodeList nodeList = nodeRegistryService.listNodes();
            Assert.assertNotNull(nodeList);
            // serialize it and validate it
            Assert.assertTrue(nodeList.sizeNodeList() >= 2);
            boolean foundTestCNNode = false;
            boolean foundTestCNIdentity = false;
            boolean foundTestCNRestriction = false;
            for (Node node : nodeList.getNodeList()) {
                if (node.getName().equals("localhost-cntest-1")) {
                    foundTestCNNode = true;
                    Services services = node.getServices();
                    Assert.assertTrue(services.sizeServiceList() > 0);
                    for (Service service : services.getServiceList()) {
                        if (service.getName().equals("CNIdentity")) {
                            foundTestCNIdentity = true;
                            Assert.assertTrue(service.sizeRestrictionList() > 0);
                            for (ServiceMethodRestriction restrict : service.getRestrictionList()) {
                                if (restrict.getMethodName().equals("mapIdentity")) {
                                    foundTestCNRestriction = true;
                                }
                            }
                        }

                    }
                }
            }
            Assert.assertTrue(foundTestCNNode);
            Assert.assertTrue(foundTestCNIdentity);
            Assert.assertTrue(foundTestCNRestriction);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(nodeList, outputStream);
            String nodeListString = new String(outputStream.toByteArray());
            log.info(nodeListString);
        } catch (Exception e) {
            e.printStackTrace();
            fail("testNodeList");
        }

        for (Node node : testNodeList) {

            nodeRegistryService.deleteNode(node.getIdentifier());
        }
    }
}
