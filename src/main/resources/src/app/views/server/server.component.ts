import {Component, OnInit} from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {ServerService, BasicInfo} from "./server.service";

@Component({
    selector: 'app-server',
    templateUrl: './server.component.html',
    styleUrls: ['./server.component.scss'],
    providers: [ServerService]
})
export class ServerComponent implements OnInit {
    name: string;
    private sub: any;
    basicInfo: BasicInfo;

    constructor(private route: ActivatedRoute,
                private serverService: ServerService) {
        this.name = null;
    }

    ngOnInit() {
        this.sub = this.route.params.subscribe(params => {
            this.name = params['name'];
            this.updateBasicInfo();
        });
    }

    updateBasicInfo() {
        this.serverService.getBasicInfo(this.name).subscribe(
            data => this.basicInfo = <BasicInfo>{...data}
        )
    }
}
