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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import javax.naming.ldap.LdapContext;

import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.integ.ServerIntegrationUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.cn.ldap.NodeServicesAccess;
import org.dataone.cn.ldap.ServiceMethodRestrictionsAccess;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.util.TypeMarshaller;
import org.dataone.test.apache.directory.server.integ.ApacheDSSuiteRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 *
 * @author waltz
 */

public class HazelcastLdapStoreTestUnit extends AbstractLdapTestUnit {

    static Logger logger = Logger.getLogger(HazelcastLdapStoreTestUnit.class);
    private HazelcastInstance hazelcastInstance;
    final static int SIZE = 16384;
    NodeRegistryService nodeRegistryService = new NodeRegistryService();
    NodeAccess nodeAccess = new NodeAccess();
    NodeServicesAccess nodeServicesAccess = new NodeServicesAccess();
    ServiceMethodRestrictionsAccess serviceMethodRestrictionsAccess = new ServiceMethodRestrictionsAccess();

    @BeforeClass
    public static void beforeClass() throws Exception {
            int ldapTimeoutCount = 0;

        if (ApacheDSSuiteRunner.getLdapServer() == null) {
            throw new Exception("ApacheDSSuiteRunner was not automatically configured. FATAL ERROR!");
        }
        while (!ApacheDSSuiteRunner.getLdapServer().isStarted() && ldapTimeoutCount < 10) {
            Thread.sleep(500L);
            logger.info("LdapServer is not yet started");
            ldapTimeoutCount++;
        }
        if (!ApacheDSSuiteRunner.getLdapServer().isStarted()) {
                throw new IllegalStateException("Service is not running");
        }
        final LdapContext ctx = ServerIntegrationUtils.getWiredContext(
				ApacheDSSuiteRunner.getLdapServer(), null);
        ctx.lookup("dc=dataone,dc=org");
    }
    @Before
    public void before() throws Exception {

        if (hazelcastInstance == null) {
            InputStream is = this.getClass().getResourceAsStream("/org/dataone/cn/service/ldap/tests/config/hazelcast.xml");

            XmlConfigBuilder configBuilder = new XmlConfigBuilder(is);

            hazelcastInstance = Hazelcast.newHazelcastInstance(configBuilder.build());
        }
        
    }

    @Test
    public void updateNodeTest() throws ServiceFailure {
        NodeReference nodeReference = new NodeReference();
        nodeReference.setValue("test");
        try {
        	
        	// TODO:  why do we convert inputstream to outputstream then back again?
//            ByteArrayOutputStream mnNodeOutput = new ByteArrayOutputStream();
//            InputStream is = this.getClass().getResourceAsStream("/org/dataone/cn/resources/samples/v1/mnNode.xml");
//
//            BufferedInputStream bInputStream = new BufferedInputStream(is);
//            byte[] barray = new byte[SIZE];
//            int nRead = 0;
//            while ((nRead = bInputStream.read(barray, 0, SIZE)) != -1) {
//                mnNodeOutput.write(barray, 0, nRead);
//            }
//            bInputStream.close();
//            
//            ByteArrayInputStream bArrayInputStream = new ByteArrayInputStream(mnNodeOutput.toByteArray());
//           Node testNode = TypeMarshaller.unmarshalTypeFromStream(Node.class, bArrayInputStream);
            
            InputStream is = this.getClass().getResourceAsStream("/org/dataone/cn/resources/samples/v1/mnNode.xml");
            Node testNode = TypeMarshaller.unmarshalTypeFromStream(Node.class, is);

            IMap<NodeReference, org.dataone.service.types.v2.Node> d1NodesMap = hazelcastInstance.getMap("hzNodes");

            /* make sure that the node we plan to register is not already in the map */
            Node nullnode = d1NodesMap.get(nodeReference);
            assertNull(nullnode);
            
            // register node under original nodeID
            nodeReference = nodeRegistryService.register(testNode);
            
            // make sure that the identifier
            testNode.setIdentifier(nodeReference);
            nodeAccess.setNodeApproved(nodeReference, Boolean.TRUE);
            testNode.setReplicate(false);
            Subject contactSubject = new Subject();
            contactSubject.setValue("cn=test2,dc=dataone,dc=org");
            testNode.addContactSubject(contactSubject);
            testNode.addSubject(contactSubject);
            Subject contactSubject3 = new Subject();
            contactSubject3.setValue("cn=test3,dc=dataone,dc=org");
            testNode.addContactSubject(contactSubject3);
            testNode.addSubject(contactSubject3);
            
            d1NodesMap.get(nodeReference);
            
            d1NodesMap.put(nodeReference, TypeMarshaller.convertTypeFromType(testNode, org.dataone.service.types.v2.Node.class));

            Node node = d1NodesMap.get(nodeReference);
            assertTrue(node.isReplicate() == false);
            Services nodeServices = node.getServices();
            for (Service service : nodeServices.getServiceList()) {
                logger.info("Service: " + service.getName());
                if (service.getRestrictionList() != null) {
                    // the subjects should be 'cn=test,dc=dataone,dc=org'
                    // if the subjects change this logic
                    for (ServiceMethodRestriction serviceMethodRestriction : service.getRestrictionList()) {
                        logger.info("ServiceMethodRestriction: " + serviceMethodRestriction.getMethodName());
                        for (Subject subject : serviceMethodRestriction.getSubjectList()) {
                            assertTrue(subject.getValue().equalsIgnoreCase("cn=test,dc=dataone,dc=org"));
                        }
                    }
                }
            }
            logger.info("Node map has " + d1NodesMap.size() + " entries");
            assertTrue(d1NodesMap.size() > 0);
            Set<NodeReference> nodesMapKeys = d1NodesMap.keySet();
            for (NodeReference key : nodesMapKeys) {
                Node newnode = d1NodesMap.get(key);
                if (newnode != null) {
                    logger.info("found node with id: " + key.getValue() + " and url " + newnode.getBaseURL());
                } else {
                    logger.info("found node with id: " + key.getValue() + " but with no node!");
                }
            }
            assertTrue(nodesMapKeys.contains(nodeReference));

//            this remove method throws an UnsupportedOperationException from ConcurrentMapManager
//            nodesMapKeys.remove(nodeReference);

        } catch (Exception ex) {
            ex.printStackTrace();
            fail("loadNodeTest");
        } finally {
            List<Service> services = nodeServicesAccess.getServiceList(nodeReference.getValue());
            if ((services != null) && (services.size() > 0)) {
                for (Service service : services) {
                    logger.debug("deleteNode Service: " + service.getName());
                    List<ServiceMethodRestriction> serviceRestrictionList = serviceMethodRestrictionsAccess.getServiceMethodRestrictionList(nodeReference.getValue(), nodeServicesAccess.buildNodeServiceId(service));
                    if (serviceRestrictionList != null) {
                        for (ServiceMethodRestriction restriction : serviceRestrictionList) {
                            logger.debug("deleteNode deleting " + serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(nodeReference, service, restriction));
                            if (!serviceMethodRestrictionsAccess.deleteServiceMethodRestriction(nodeReference, service, restriction)) {

                                fail( "Unable to delete restriction " + serviceMethodRestrictionsAccess.buildServiceMethodRestrictionDN(nodeReference, service, restriction));
                            }
                        }
                    }
                    if (!nodeServicesAccess.deleteNodeService(nodeReference, service)) {
                        fail(  "Unable to delete service " + nodeServicesAccess.buildNodeServiceDN(nodeReference, service));
                    }
                }
            }
            nodeAccess.deleteNode(nodeReference);
        }
    }
}
