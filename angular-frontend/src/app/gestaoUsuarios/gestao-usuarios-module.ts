import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { GestaoUsuariosRoutingModule } from './gestao-usuarios-routing-module';

import {MatTableModule} from '@angular/material/table';

@NgModule({
  declarations: [],
  imports: [
	CommonModule,
	GestaoUsuariosRoutingModule,
	MatTableModule
  ],
})
export class GestaoUsuariosModule {}
