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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;

import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;

/**
 * Provide read and write access to the nodelist stored in LDAP
 * @author waltz
 */
public class HazelcastLdapStore implements MapLoader, MapStore {

    public static Log log = LogFactory.getLog(HazelcastLdapStore.class);
    private static NodeRegistryService nodeRegistry = new NodeRegistryService();

    @Override
    public Object load(Object key) {
        Node node = null;
        try {
            node = nodeRegistry.getNode((NodeReference) key);
        } catch (NotFound ex) {
            log.warn(ex.serialize(NotImplemented.FMT_XML));
        } catch (ServiceFailure ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }
        return node;
    }

    @Override
    public Map loadAll(Collection keyCollection) {
        // Interpret loadAll as a way to get allNode again?
        NodeList nodeList = new NodeList();
        try {
            nodeList = nodeRegistry.listNodes();
        } catch (NotImplemented ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
            throw new RuntimeException(ex.getMessage());
        } catch (ServiceFailure ex) {
            log.error(ex.serialize(ServiceFailure.FMT_XML));
            throw new RuntimeException(ex.getMessage());
        }
        List<Node> d1Nodes = nodeList.getNodeList();
        Map<NodeReference, Node> mappedStore = new ConcurrentHashMap<NodeReference, Node>();
        for (Node d1Node : d1Nodes) {
            mappedStore.put(d1Node.getIdentifier(), d1Node);
        }
        return mappedStore;
    }

    @Override
    public Set loadAllKeys() {
        // upon instantiation of this Store, or when the first time
        // a map is called, loadAllKeys will be called.
        NodeList nodeList = new NodeList();
        try {
            nodeList = nodeRegistry.listNodes();
        } catch (NotImplemented ex) {
            log.error(ex.serialize(NotImplemented.FMT_XML));
            throw new RuntimeException(ex.getMessage());
        } catch (ServiceFailure ex) {
            log.error(ex.serialize(ServiceFailure.FMT_XML));
            throw new RuntimeException(ex.getMessage());
        }
        List<Node> d1Nodes = nodeList.getNodeList();
        Set<NodeReference> keys = Collections.newSetFromMap(new ConcurrentHashMap<NodeReference, Boolean>());
        for (Node d1Node : d1Nodes) {
            keys.add(d1Node.getIdentifier());
        }
        return keys;
    }

    @Override
    public void store(Object key, Object value) {

        // Hazelcast can not use store to create an Node
        // Since keys are randomly assigned at creation,
        // any key provided by store will be lost, and the
        // key stored in the hazelcast map will be incorrect

        // furthermore, when a node is created, you should not be
        // able to 'get' the node via hazelcast map
        // until it has been approved
        // if store where able to create a node, then it
        // would be possible to get the node before
        // it is approved

        // determine if the key already exists, 
        // if not, then throw an exception
        // if so, then update
        NodeReference nodeId = (NodeReference) key;
        Node node = null;
        try {
            node = nodeRegistry.getNode(nodeId);
        } catch (ServiceFailure ex) {
            throw new RuntimeException(ex.getMessage());
        } catch (NotFound ex) {
            log.error("Node Not Found, a node must be created through Node Registry Service and approved before Hazelcast can update it");
            throw new RuntimeException(ex.getMessage());
        }

        if (node == null) {
            throw new RuntimeException("Node is null, a node must be created through Node Registry Service and approved before Hazelcast can update it");
        } else {
            try {
                // update, but no idea what the update will look like
                if (nodeRegistry.updateNodeCapabilities(nodeId, (Node) value)) {
                    log.debug("updated NodeCapabilities for " + nodeId.getValue());
                } else {
                    log.error("Failed to update NodeCapabilities for " + nodeId.getValue());
                    throw new RuntimeException("Node is null, a node must be created through Node Registry Service and approved before Hazelcast can update it");
                }
            } catch (NotImplemented ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex.getMessage());
            } catch (ServiceFailure ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex.getMessage());
            } catch (InvalidRequest ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex.getMessage());
            } catch (NotFound ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex.getMessage());
            }
        }

    }

    @Override
    public void storeAll(Map map) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    @Override
    public void delete(Object key) {
        try {
            nodeRegistry.deleteNode((NodeReference) key);
        } catch (ServiceFailure ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Override
    public void deleteAll(Collection collection) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
