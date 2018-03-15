import {Component} from '@angular/core';
import {Router} from '@angular/router';

const mocking_nodes = [{
    "status": "STOP",
    "prev": null,
    "nodeName": "server1",
    "nodePort": 50008,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "76022fb5320a82850f4f8bb039566ddd"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server2",
    "nodePort": 50001,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "dcee0277eb13b76434e8dcd31a387709"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server3",
    "nodePort": 50002,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "b3638a32c297f43aa37e63bbd839fc7e"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server4",
    "nodePort": 50003,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "a98109598267087dfc364fae4cf24578"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server5",
    "nodePort": 50004,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "da850509fc3b88a612b0bcad7a37963b"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server6",
    "nodePort": 50005,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "297e522da5461c774be1037dfb0a8226"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server7",
    "nodePort": 50006,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "8313085f59dfed5215afe928fc846355"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server8",
    "nodePort": 50007,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "209b422ec722673d3f5ed25fe49c2103"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server9",
    "nodePort": 50009,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "18d72fb047cfa96bf76b263790df36fd"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server10",
    "nodePort": 50010,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "51feb15dc634eb38a2be7d96b6cf6fa5"
}, {
    "status": "OFFLINE",
    "prev": null,
    "nodeName": "server11",
    "nodePort": 50011,
    "nodeHost": "127.0.0.1",
    "nodeHashRange": null,
    "nodeHash": "3ebf39bfa08189651d170e593782fea4"
}];

@Component({
    templateUrl: 'dashboard.component.html'
})
export class DashboardComponent {
    statusToBadge = {
        "ACTIVE": "badge-success",
        "STOP": "badge-danger",
        "OFFLINE": "badge-secondary",
    };

    nodeList: any[] = mocking_nodes;
    constructor() {
    }
}
