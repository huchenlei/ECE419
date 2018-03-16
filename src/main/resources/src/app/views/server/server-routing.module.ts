import {NgModule} from '@angular/core';
import {Routes, RouterModule} from '@angular/router';
import {ServerComponent} from "./server.component";

const routes: Routes = [
    {
        path: ':name',
        component: ServerComponent,
        data: {
            title: 'Server'
        }
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class ServerRoutingModule {
}
