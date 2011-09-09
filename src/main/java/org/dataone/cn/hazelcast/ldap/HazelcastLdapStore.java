/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.hazelcast.ldap;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.service.ldap.impl.v1.CNCoreLDAPImpl;
import org.dataone.cn.service.ldap.impl.v1.CNRegisterLDAPImpl;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Provide read and write access to the nodelist stored in LDAP
 * @author waltz
 */
@Service
@Qualifier("hazelcastLdapStore")
public class HazelcastLdapStore implements MapLoader, MapStore {

    public static Log log = LogFactory.getLog(HazelcastLdapStore.class);
    @Autowired
    @Qualifier("cnCoreLDAPImpl")
    private CNCoreLDAPImpl cnCoreLDAPImpl;
    
    @Autowired
    @Qualifier("cnRegisterLDAPImpl")
    private CNRegisterLDAPImpl cnRegisterLDAPImpl;

    @Override
    public Object load(Object key) {
        Node node = null;
        try {
            node = cnCoreLDAPImpl.getNode((String) key);
        } catch (NotImplemented ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
        } catch (ServiceFailure ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
        }
        return node;
    }

    @Override
    public Map loadAll(Collection keyCollection) {
        // Interpret loadAll as a way to get allNode again?
        NodeList nodeList = null;
        try {
            nodeList = cnCoreLDAPImpl.listNodes();
        } catch (NotImplemented ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
        } catch (ServiceFailure ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
        }
        List<Node> d1Nodes = nodeList.getNodeList();
        Map<String, Node> mappedStore = new ConcurrentHashMap<String, Node>();
        for (Node d1Node : d1Nodes) {
            mappedStore.put(d1Node.getIdentifier().getValue(), d1Node);
        }
        return mappedStore;
    }

    @Override
    public Set loadAllKeys() {
        // upon instantiation of this Store, or when the first time
        // a map is called, loadAllKeys will be called.
        NodeList nodeList = null;
        try {
            nodeList = cnCoreLDAPImpl.listNodes();
        } catch (NotImplemented ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
        } catch (ServiceFailure ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
        }
        List<Node> d1Nodes = nodeList.getNodeList();
        Set<String> keys = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        for (Node d1Node : d1Nodes) {
            keys.add(d1Node.getIdentifier().getValue());
        }
        return keys;
    }

    @Override
    public void store(Object key, Object value) {
        cnRegisterLDAPImpl.updateLastHarvested((String) key, (Node) value);
    }

    @Override
    public void storeAll(Map map) {
        for (Object key : map.keySet()) {
            cnRegisterLDAPImpl.updateLastHarvested((String) key, (Node) map.get(key));
        }
    }

    @Override
    public void delete(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteAll(Collection collection) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
