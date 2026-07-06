import { Component } from '@angular/core';
import { Usuario } from '../model/usuario';

@Component({
  selector: 'app-gestao-usuarios',
  imports: [],
  templateUrl: './gestao-usuarios.html',
  styleUrl: './gestao-usuarios.css',
})
export class GestaoUsuarios {
	
	usuarios: Usuario[];
	displayedColumns=[];
	
	constructor(){
		this.usuarios=[];
		this.displayedColumns=['nome','email']
	}
}
