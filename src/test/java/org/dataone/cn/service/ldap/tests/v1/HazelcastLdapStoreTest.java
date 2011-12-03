/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.service.ldap.tests.v1;

import java.util.Set;
import java.io.ByteArrayInputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import org.dataone.service.types.v1.NodeReference;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.io.InputStream;
import org.apache.log4j.Logger;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Test;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.ServiceMethodRestriction;
import org.junit.Before;
import static org.junit.Assert.*;

/**
 *
 * @author waltz
 */
public class HazelcastLdapStoreTest {

    static Logger logger = Logger.getLogger(HazelcastLdapStoreTest.class);
    private HazelcastInstance hazelcastInstance;
    final static int SIZE = 16384;
    NodeRegistryService nodeRegistryService = new NodeRegistryService();

    /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
    /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
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
            Node testNode = TypeMarshaller.unmarshalTypeFromStream(Node.class, bArrayInputStream);

            IMap<NodeReference, Node> d1NodesMap = hazelcastInstance.getMap("hzNodes");

            Node nullnode = d1NodesMap.get(nodeReference);
            assertNull(nullnode);
            nodeReference = nodeRegistryService.register(testNode);
            testNode.setIdentifier(nodeReference);
            nodeRegistryService.approveNode(nodeReference);
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
            d1NodesMap.put(nodeReference, testNode);

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
            nodeRegistryService.deleteNode(nodeReference);
        }
    }
}
