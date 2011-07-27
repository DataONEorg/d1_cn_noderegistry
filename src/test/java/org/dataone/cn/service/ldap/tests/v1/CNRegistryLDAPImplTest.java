package org.dataone.cn.service.ldap.tests.v1;


import org.dataone.service.util.TypeMarshaller;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.annotation.Resource;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dataone.cn.service.ldap.impl.v1.CNRegisterLDAPImpl;
import org.dataone.service.cn.v1.CNCore;

import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Schedule;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Synchronization;
import org.jibx.runtime.JiBXException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:org/dataone/cn/service/ldap/tests/config/applicationContext.xml"})
public class CNRegistryLDAPImplTest {
    static final String datatypeSchemaTagUrl = "https://repository.dataone.org/software/cicore/tags/D1_SCHEMA_0_6_2/dataoneTypes.xsd";
    
    public static Log log = LogFactory.getLog(CNRegistryLDAPImplTest.class);
    CNCore cnLdapCore;
    CNRegisterLDAPImpl cnLdapRegister;
    
    /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
    @Resource
    public void setCNCore(CNCore cnLdapCore) {
        this.cnLdapCore = cnLdapCore;
    }
    /**
     *  pull in the CnRegister implementation to test against
     * @author rwaltz
     */
    @Resource
    public void setCNRegister(CNRegisterLDAPImpl cnLdapRegister) {
        this.cnLdapRegister = cnLdapRegister;
    }

    @Test
    public void testRegisterAndDeleteNode() throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, IdentifierNotUnique {
        List<Node> testNodeList = new ArrayList<Node>();
        Node testCNNode = new Node();
        testCNNode.setName("testThisCN");
        testCNNode.setDescription("this is a test");
        testCNNode.setBaseURL("https://test.this.stuff/cn");
        testCNNode.setReplicate(false);
        testCNNode.setSynchronize(false);
        testCNNode.setType(NodeType.CN);
        NodeReference cnNodeReference = cnLdapRegister.register(null, testCNNode);
        assertNotNull(cnNodeReference);
        testNodeList.add(testCNNode);

        Node testMNNode = new Node();
        testMNNode.setName("testThisMN");
        testMNNode.setDescription("this is a test");
        testMNNode.setBaseURL("https://test.this.stuff/mn");
        testMNNode.setReplicate(true);
        testMNNode.setSynchronize(true);
        testMNNode.setType(NodeType.MN);

        Synchronization synchronization = new Synchronization();

        synchronization.setLastCompleteHarvest(new Date());
        synchronization.setLastHarvested(new Date());
        Schedule schedule = new Schedule();
        schedule.setSec("0");
        schedule.setMin("05");
        schedule.setHour("01");
        schedule.setMon("*");
        schedule.setMday("*");
        schedule.setWday("*");
        schedule.setYear("*");
        synchronization.setSchedule(schedule);

        testMNNode.setSynchronization(synchronization);

        Service service1 = new Service();
        service1.setName("MNCore");
        service1.setVersion("0.5.0");
        service1.setAvailable(Boolean.TRUE);

        Services services = new Services();
        services.addService(service1);

        Service service2 = new Service();
        service2.setName("MNRead");
        service2.setVersion("0.5.0");
        service2.setAvailable(Boolean.TRUE);
        services.addService(service2);

        testMNNode.setServices(services);
        NodeReference mnNodeReference =  cnLdapRegister.register(null, testMNNode);
        assertNotNull(mnNodeReference);
        testNodeList.add(testMNNode);
        log.info("created " + testNodeList.size() + " nodes");

        for (Node node : testNodeList) {
            if ((node.getServices() != null) && (node.getServices().sizeServiceList() > 0)) {
                for (Service service: node.getServices().getServiceList()) {
                    cnLdapRegister.deleteNodeService(node, service);
                }
            }
           cnLdapRegister.deleteNode(node);
        }
    }

    @Test
    public void testNodeList() throws NotImplemented, ServiceFailure, IOException, JiBXException {
        // retrieve it
        NodeList nodeList = cnLdapCore.listNodes();
        Assert.assertNotNull(nodeList);
        // serialize it and validate it
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        TypeMarshaller.marshalTypeToOutputStream(nodeList, outputStream);
        String nodeListString = new String (outputStream.toByteArray());
        log.info(nodeListString);

    }

}


