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

package org.dataone.cn.service.ldap.tests.v1;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.ServiceFailure;
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
import org.jibx.runtime.JiBXException;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.cn.ldap.NodeServicesAccess;
import org.dataone.cn.ldap.ServiceMethodRestrictionsAccess;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Services;
import org.junit.Before;

public class NodeRegistryServiceTest {

    public static Log log = LogFactory.getLog(NodeRegistryServiceTest.class);
    NodeRegistryService nodeRegistryService = new NodeRegistryService();
    NodeAccess nodeAccess = new NodeAccess();
    NodeServicesAccess nodeServicesAccess = new NodeServicesAccess();
    ServiceMethodRestrictionsAccess serviceMethodRestrictionsAccess = new ServiceMethodRestrictionsAccess();
    LdapPopulationService ldapPopulationService = new LdapPopulationService();
    final static int SIZE = 16384;
    Node testMNNode;
    Node testCNNode;
    Node testMNNoSynchNode;
    @Before
    public void removeAnyTestNodes() throws IOException, InstantiationException, IllegalAccessException, JiBXException, NamingException {
        testMNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnNode.xml");
        testCNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/cnNode.xml");
        testMNNoSynchNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnNodeValidNoSynch.xml");
        
        ldapPopulationService.deleteTestNodesByName(testMNNode.getIdentifier().getValue());
        ldapPopulationService.deleteTestNodesByName(testCNNode.getIdentifier().getValue());
        ldapPopulationService.deleteTestNodesByName(testMNNoSynchNode.getIdentifier().getValue());
    }
    @Test
    public void testRegisterListAndDeleteNode() throws Exception {
        List<Node> testNodeList = new ArrayList<Node>();

        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        assertNotNull(mnNodeReference);
        testNodeList.add(testMNNode);

        NodeReference cnNodeReference = nodeRegistryService.register(testCNNode);
        testCNNode.setIdentifier(cnNodeReference);
        testNodeList.add(testCNNode);

        Node testCNRetrieval = nodeRegistryService.getNode(cnNodeReference);

        ByteArrayOutputStream outputTestCNStream = new ByteArrayOutputStream();
        TypeMarshaller.marshalTypeToOutputStream(testCNRetrieval, outputTestCNStream);
        String testCNNodeString = new String(outputTestCNStream.toByteArray());
        log.info(testCNNodeString);

        assertTrue(testCNRetrieval.getIdentifier().getValue().equalsIgnoreCase(cnNodeReference.getValue()));
        for (Node node : testNodeList) {

            nodeAccess.setNodeApproved(node.getIdentifier(), Boolean.TRUE);
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

            List<Service> services = nodeServicesAccess.getServiceList(node.getIdentifier().getValue());
            if ((services != null) && (services.size() > 0)) {
                for (Service service : services) {
                    log.debug("deleteNode Service: " + service.getName());
                    List<ServiceMethodRestriction> serviceRestrictionList = serviceMethodRestrictionsAccess.getServiceMethodRestrictionList(node.getIdentifier().getValue(), nodeServicesAccess.buildNodeServiceId(service));
                    if (serviceRestrictionList != null) {
                        for (ServiceMethodRestriction restriction : serviceRestrictionList) {
                            log.debug("deleteNode deleting " + serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(node.getIdentifier(), service, restriction));
                            if (!serviceMethodRestrictionsAccess.deleteServiceMethodRestriction(node.getIdentifier(), service, restriction)) {

                                fail( "Unable to delete restriction " + serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(node.getIdentifier(), service, restriction));
                            }
                        }
                    }
                    if (!nodeServicesAccess.deleteNodeService(node.getIdentifier(), service)) {
                        fail(  "Unable to delete service " + nodeServicesAccess.buildNodeServiceDN(node.getIdentifier(), service));
                    }
                }
            }
            nodeAccess.deleteNode(node.getIdentifier());
        }
    }
    @Test
    public void testRegisterNoSyncMNNode() throws Exception {
        // This should be able to register without error, that is all
        NodeReference cnNodeReference = nodeRegistryService.register(testMNNoSynchNode);
        ldapPopulationService.deleteTestNodesByName(testMNNoSynchNode.getIdentifier().getValue());
    }
    @Test(expected=InvalidRequest.class)
    public void testRegisterBadLocalhostNode() throws IOException, InstantiationException, IllegalAccessException, JiBXException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented  {
        Node testMNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnBadLocalhostNode.xml");

        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);

    }
    @Test(expected=InvalidRequest.class)
    public void testRegisterBadNodeId() throws IOException, InstantiationException, IllegalAccessException, JiBXException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented  {

        Node testMNNode =buildTestNode("/org/dataone/cn/resources/samples/v1/mnBadNodeId.xml");

        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);

    }
    @Test(expected=InvalidRequest.class)
    public void testRegisterBadSyncNode() throws IOException, InstantiationException, IllegalAccessException, JiBXException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented  {

        Node testMNNode =buildTestNode("/org/dataone/cn/resources/samples/v1/mnNodeFailOnSynchronization.xml");

        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);

    } 
    @Test(expected=InvalidRequest.class)
    public void testRegisterBadSyncScheduleNode() throws IOException, InstantiationException, IllegalAccessException, JiBXException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented  {

        Node testMNNode =buildTestNode("/org/dataone/cn/resources/samples/v1/mnNodeFailOnSchedule.xml");

        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);

    }
   
    private Node buildTestNode(String resourcePath) throws IOException, InstantiationException, IllegalAccessException, JiBXException {
        ByteArrayOutputStream mnNodeOutput = new ByteArrayOutputStream();
        InputStream is = this.getClass().getResourceAsStream(resourcePath);

        BufferedInputStream bInputStream = new BufferedInputStream(is);
        byte[] barray = new byte[SIZE];
        int nRead = 0;
        while ((nRead = bInputStream.read(barray, 0, SIZE)) != -1) {
            mnNodeOutput.write(barray, 0, nRead);
        }
        bInputStream.close();
        ByteArrayInputStream bArrayInputStream = new ByteArrayInputStream(mnNodeOutput.toByteArray());
        Node testNode = TypeMarshaller.unmarshalTypeFromStream(Node.class, bArrayInputStream);
        return testNode;
    }
}
