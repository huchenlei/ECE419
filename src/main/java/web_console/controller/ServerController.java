package web_console.controller;

import ecs.ECS;
import ecs.RawECSNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ServerController {

    @Autowired
    private ECS ecs;

    /**
     * @return all nodes
     */
    @GetMapping("/node/all")
    public List<RawECSNode> getAllNodes() {
        return ecs.getAllNodes();
    }

    /**
     * @return nodes on the hashring, i.e. nodes that are active
     */
    @GetMapping("/node/active")
    public String getHashRing() {
        return ecs.getHashRingJson();
    }

    /**
     * @return nodes on nodeTable including STOPPED & ACTIVE servers
     */
    @GetMapping("/node/used")
    public List<RawECSNode> getusedNodes() {
        return ecs.getUsedNodes();
    }

    /**
     * @return nodes in nodePool, to be added to nodeTable
     */
    @GetMapping("/node/available")
    public List<RawECSNode> getAvailableNodes() {
        return ecs.getAvailableNodes();
    }
}
