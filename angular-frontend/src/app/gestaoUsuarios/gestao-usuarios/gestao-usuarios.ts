import { Component } from '@angular/core';

import { Usuario } from '../model/usuario';

import {MatTableModule} from '@angular/material/table';
import {MatToolbarModule} from '@angular/material/toolbar';

@Component({
  selector: 'app-gestao-usuarios',
  imports: [
	MatTableModule,
	MatToolbarModule
  ],
  templateUrl: './gestao-usuarios.html',
  styleUrl: './gestao-usuarios.css',
})
export class GestaoUsuarios {
	
	usuarios: Usuario[]=[];
	displayedColumns=[]=['nome','email'];
	
	constructor(){
		//this.usuarios=[];
		//this.displayedColumns=['nome','email']
	}
}
