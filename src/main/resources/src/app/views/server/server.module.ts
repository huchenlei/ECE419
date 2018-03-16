import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ServerRoutingModule } from './server-routing.module';
import { ServerComponent } from './server.component';

@NgModule({
  imports: [
    CommonModule,
    ServerRoutingModule
  ],
  declarations: [ServerComponent]
})
export class ServerModule { }
