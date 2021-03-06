/**
 * This work was created by participants in the DataONE project, and is jointly copyrighted by participating
 * institutions in DataONE. For more information on DataONE, see our web site at http://dataone.org.
 *
 * Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * $Id$
 */
package org.dataone.cn.service.ldap.tests.v1;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.dataone.cn.ldap.NodeFacade;
import org.dataone.cn.ldap.NodeServicesAccess;
import org.dataone.cn.ldap.ServiceMethodRestrictionsAccess;
import org.dataone.service.cn.v1.impl.NodeRegistryServiceImpl;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeReplicationPolicy;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Schedule;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Synchronization;
import org.dataone.service.util.TypeMarshaller;
import org.dataone.exceptions.MarshallingException;
import org.junit.Before;
import org.junit.Test;


public class NodeRegistryServiceTestUnit extends AbstractLdapTestUnit {
    
    public static Log log = LogFactory.getLog(NodeRegistryServiceTestUnit.class);
    NodeRegistryServiceImpl nodeRegistryService = new NodeRegistryServiceImpl();
    NodeFacade nodeAccess = new NodeFacade();
    NodeServicesAccess nodeServicesAccess = new NodeServicesAccess();
    ServiceMethodRestrictionsAccess serviceMethodRestrictionsAccess = new ServiceMethodRestrictionsAccess();
    LdapPopulationService ldapPopulationService = new LdapPopulationService();
    final static int SIZE = 16384;
    Node testMNNode;
    Node testCNNode;
    Node testMNNoSynchNode;
    Node testMNNoRepPolicy;
    
    @Before
    public void removeAnyTestNodes() throws IOException, InstantiationException, IllegalAccessException, MarshallingException, NamingException, Exception {
        testMNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnNode.xml");
        testCNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/cnNode.xml");
        testMNNoSynchNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnNodeValidNoSynch.xml");
        testMNNoRepPolicy = buildTestNode("/org/dataone/cn/resources/samples/v1/mnNodeNoRepPolicy.xml");
        
        ldapPopulationService.deleteTestNodesByName(testMNNode.getIdentifier().getValue());
        ldapPopulationService.deleteTestNodesByName(testCNNode.getIdentifier().getValue());
        ldapPopulationService.deleteTestNodesByName(testMNNoSynchNode.getIdentifier().getValue());
        ldapPopulationService.deleteTestNodesByName(testMNNoRepPolicy.getIdentifier().getValue());
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
        
        Node testCNRetrieval = nodeRegistryService.getNodeCapabilities(cnNodeReference);
        
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
            
            // serialize it and validate it
            assertTrue(nodeList.sizeNodeList() >= 2);
            boolean foundTestCNNode = false;
            boolean foundTestCNIdentity = false;
            boolean foundTestCNRestriction = false;
            for (Node node : nodeList.getNodeList()) {
                if (node.getName().equals("localhost-cntest-1")) {
                    foundTestCNNode = true;
                    Services services = node.getServices();
                    assertTrue(services.sizeServiceList() > 0);
                    for (Service service : services.getServiceList()) {
                        if (service.getName().equals("CNIdentity")) {
                            foundTestCNIdentity = true;
                            assertTrue(service.sizeRestrictionList() > 0);
                            for (ServiceMethodRestriction restrict : service.getRestrictionList()) {
                                if (restrict.getMethodName().equals("mapIdentity")) {
                                    foundTestCNRestriction = true;
                                }
                            }
                        }
                        
                    }
                }
            }
            assertTrue(foundTestCNNode);
            assertTrue(foundTestCNIdentity);
            assertTrue(foundTestCNRestriction);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(nodeList, outputStream);
            String nodeListString = new String(outputStream.toByteArray());
            log.info(nodeListString);
        } catch (Exception e) {
            e.printStackTrace();
            fail("testNodeList");
        }
        
        for (Node node : testNodeList) {
            
            nodeAccess.deleteNode(node.getIdentifier());
        }
    }

    @Test
    public void testRegisterNoSyncMNNode() throws Exception {
        // This should be able to register without error, that is all
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNoSynchNode);
        ldapPopulationService.deleteTestNodesByName(testMNNoSynchNode.getIdentifier().getValue());
    }
    
    @Test
    public void testRegisterNoRepPolicyMNNode() throws Exception {
        // This should be able to register without error, that is all
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNoRepPolicy);
        ldapPopulationService.deleteTestNodesByName(testMNNoSynchNode.getIdentifier().getValue());
    }
    
    
    @Test
    public void testUpdateNoSyncNodeWithSyncMNNode() throws Exception {
        // This should be able to register without error, that is all
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNoSynchNode);
        Node mnRegisteredNode = nodeRegistryService.getNodeCapabilities(mnNodeReference);
        Synchronization sync = new Synchronization();
        Schedule sched = new Schedule();
        sched.setSec("30");
        sched.setMin("30");
        sched.setHour("01");
        sched.setMday("*");
        sched.setMon("*");
        sched.setWday("?");
        sched.setYear("*");
        sync.setSchedule(sched);
        sync.setLastHarvested(new Date());
        sync.setLastCompleteHarvest(new Date());
        mnRegisteredNode.setSynchronization(sync);
        mnRegisteredNode.setSynchronize(true);
        nodeRegistryService.updateNodeCapabilities(mnNodeReference, mnRegisteredNode);
        mnRegisteredNode = nodeRegistryService.getNodeCapabilities(mnNodeReference);
        try {
            assertTrue(mnRegisteredNode.getSynchronization().getSchedule() != null);
            assertTrue(mnRegisteredNode.isSynchronize());
        } catch (NullPointerException ex) {
            fail("Test misconfiguration " + ex);
        }
        ldapPopulationService.deleteTestNodesByName(testMNNoSynchNode.getIdentifier().getValue());
    }
    
    
    @Test
    public void testUpdateNoRepPolicyWithRepPolicyMNnode() throws Exception {
        // This should be able to register without error, that is all
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNoRepPolicy);
        Node mnRegisteredNode = nodeRegistryService.getNodeCapabilities(mnNodeReference);
        NodeReplicationPolicy nrp = new NodeReplicationPolicy();
        nrp.setSpaceAllocated(BigInteger.TEN);
        nrp.setMaxObjectSize(BigInteger.ONE);

        NodeReference nr1 = new NodeReference(); 
        nr1.setValue("foo");
        nrp.addAllowedNode(nr1);
        NodeReference nr2 = new NodeReference(); 
        nr2.setValue("bar");
        nrp.addAllowedNode(nr2);

        ObjectFormatIdentifier fmtid1 = new ObjectFormatIdentifier();
        fmtid1.setValue("text/xml");
        nrp.addAllowedObjectFormat(fmtid1);
        ObjectFormatIdentifier fmtid2 = new ObjectFormatIdentifier();
        fmtid2.setValue("text/csv");
        nrp.addAllowedObjectFormat(fmtid2);
        ObjectFormatIdentifier fmtid3 = new ObjectFormatIdentifier();
        fmtid3.setValue("application/octet-stream");
        nrp.addAllowedObjectFormat(fmtid3);
        
        mnRegisteredNode.setNodeReplicationPolicy(nrp);
        nodeRegistryService.updateNodeCapabilities(mnNodeReference, mnRegisteredNode);
        mnRegisteredNode = nodeRegistryService.getNodeCapabilities(mnNodeReference);
        try {
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy() != null);
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getMaxObjectSize().toString().equals("1"));
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getSpaceAllocated().toString().equals("10"));
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getAllowedNodeList().size() == 2);
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getAllowedObjectFormatList().size() == 3);
        } catch (NullPointerException ex) {
            fail("Test misconfiguration " + ex);
        }
        ldapPopulationService.deleteTestNodesByName(testMNNoRepPolicy.getIdentifier().getValue());
    }
    
    @Test
    public void testUpdateRepPolicyWithNewRepPolicy() throws Exception {
        // This should be able to register without error, that is all
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        Node mnRegisteredNode = nodeRegistryService.getNodeCapabilities(mnNodeReference);
        NodeReplicationPolicy nrp = new NodeReplicationPolicy();
        nrp.setSpaceAllocated(BigInteger.TEN);
        nrp.setMaxObjectSize(BigInteger.ONE);

        NodeReference nr1 = new NodeReference(); 
        nr1.setValue("flip");
        nrp.addAllowedNode(nr1);
        NodeReference nr2 = new NodeReference(); 
        nr2.setValue("flop");
        nrp.addAllowedNode(nr2);

        ObjectFormatIdentifier fmtid1 = new ObjectFormatIdentifier();
        fmtid1.setValue("text/xml");
        nrp.addAllowedObjectFormat(fmtid1);
        ObjectFormatIdentifier fmtid2 = new ObjectFormatIdentifier();
        fmtid2.setValue("text/csv");
        nrp.addAllowedObjectFormat(fmtid2);
        ObjectFormatIdentifier fmtid3 = new ObjectFormatIdentifier();
        fmtid3.setValue("application/octet-stream");
        nrp.addAllowedObjectFormat(fmtid3);
        
        mnRegisteredNode.setNodeReplicationPolicy(nrp);
        nodeRegistryService.updateNodeCapabilities(mnNodeReference, mnRegisteredNode);
        mnRegisteredNode = nodeRegistryService.getNodeCapabilities(mnNodeReference);
        try {
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy() != null);
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getMaxObjectSize().toString().equals("1"));
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getSpaceAllocated().toString().equals("10"));
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getAllowedNodeList().size() == 2);
            assertTrue(mnRegisteredNode.getNodeReplicationPolicy().getAllowedObjectFormatList().size() == 3);
        } catch (NullPointerException ex) {
            fail("Test misconfiguration " + ex);
        }
        ldapPopulationService.deleteTestNodesByName(testMNNoRepPolicy.getIdentifier().getValue());
    }

    @Test(expected = InvalidRequest.class)
    public void testRegisterBadLocalhostNode() throws IOException, InstantiationException, IllegalAccessException, MarshallingException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented {
        Node testMNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnBadLocalhostNode.xml");
        
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        
    }

    @Test(expected = InvalidRequest.class)
    public void testRegisterBadNodeId() throws IOException, InstantiationException, IllegalAccessException, MarshallingException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented {
        
        Node testMNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnBadNodeId.xml");
        
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        
    }

    @Test(expected = InvalidRequest.class)
    public void testRegisterBadSyncNode() throws IOException, InstantiationException, IllegalAccessException, MarshallingException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented {
        
        Node testMNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnNodeFailOnSynchronization.xml");
        
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        
    }    

    @Test(expected = InvalidRequest.class)
    public void testRegisterBadSyncScheduleNode() throws IOException, InstantiationException, IllegalAccessException, MarshallingException, ServiceFailure, IdentifierNotUnique, InvalidRequest, NotImplemented {
        
        Node testMNNode = buildTestNode("/org/dataone/cn/resources/samples/v1/mnNodeFailOnSchedule.xml");
        
        NodeReference mnNodeReference = nodeRegistryService.register(testMNNode);
        
    }
    
    private Node buildTestNode(String resourcePath) 
    throws IOException, InstantiationException, IllegalAccessException, MarshallingException 
    {
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
