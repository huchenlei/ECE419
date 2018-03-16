import {Injectable} from '@angular/core';
import {HttpClient} from "@angular/common/http";

const BASIC_INFO_URL = 'node/';
const DETAILED_INFO_URL = 'server/';

export interface BasicInfo {
    status: string;
    prev: BasicInfo;
    nodeName: string;
    nodePort: number;
    nodeHost: string;
    nodeHashRange: Array<string>;
    nodeHash: string;
}

@Injectable()
export class ServerService {
    constructor(private http: HttpClient) {

    }

    public getBasicInfo(name: string) {
        return this.http.get<BasicInfo>(BASIC_INFO_URL + name);
    }

    public getDetailedInfo(name: string) {
        return this.http.get(DETAILED_INFO_URL + name);
    }

}
