package web_console.controller;

import ecs.ECS;
import ecs.RawECSNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import web_console.ResponseWrapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.PUT;

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
     * Query the ECSNode details based on name given
     *
     * @param name server name
     * @return node detail
     */
    @RequestMapping(value = "/node/{name}", method = GET)
    public RawECSNode getNodeByName(@PathVariable("name") String name) {
        return ecs.getNodeByName(name);
    }

    @RequestMapping(value = "/node", method = PUT)
    public ResponseWrapper createNode(
            @RequestParam String name,
            @RequestParam String host,
            @RequestParam Integer port) {
        try {
            ecs.createNode(name, host, port);
        }  catch (Exception e) {
            return new ResponseWrapper(e.getMessage(), "FAIL");
        }
        return new ResponseWrapper(null, "SUCCESS");
    }

    @RequestMapping(value = "/node/{name}/shutdown", method = GET)
    public ResponseWrapper shutDownNode(@PathVariable("name") String name) {
        try {
            if ("all".equals(name)) {
                ecs.shutdown();
            } else {
                ecs.removeNodes(Collections.singletonList(name));
            }
        } catch (Exception e) {
            return new ResponseWrapper(e.getMessage(), "FAIL");
        }
        return new ResponseWrapper(null, "SUCCESS");
    }

    @RequestMapping(value = "/node/{name}/start", method = GET)
    public ResponseWrapper startNode(@PathVariable("name") String name) {
        // TODO
        return null;
    }

    @RequestMapping(value = "/node/{name}/add", method = GET)
    public ResponseWrapper addNode(@PathVariable("name") String name) {
        // TODO
        return null;
    }
}
