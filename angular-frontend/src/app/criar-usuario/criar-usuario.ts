import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { AuthService } from '../services/auth.service';

@Component({
  selector: 'app-criar-usuario',
  standalone: true,
  imports: [ FormsModule,
  			 CommonModule ],
  templateUrl: './criar-usuario.html',
  styleUrl: './criar-usuario.css',
})
export class CriarUsuario {
	
	nome='';
	email='';
	senha='';
	
	mensagens='';
	
	constructor(private authService: AuthService){}
	
	onSubmit(){
		this.authService.novoUsuario(this.nome, this.email, this.senha).subscribe({
			next: (response)=>{
				console.log('Usuario criado com sucesso', response);
				this.mensagens='Usuario criado com sucesso.';
			},
			error: (err)=>{
				console.error('Nao foi possivel criar este usuario', err);
				this.mensagens='Nao foi possivel criar este usuario.';
			}
		});
	}
	
}
