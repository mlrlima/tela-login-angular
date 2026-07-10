import { Component, ChangeDetectorRef } from '@angular/core';
import { Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { AuthService } from '../services/auth.service';
import { RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';


@Component({
  selector: 'app-criar-usuario',
  standalone: true,
  imports: [ FormsModule,
  			 CommonModule,
			 RouterLink,
			 MatCardModule,
			 MatFormFieldModule,
			 MatInputModule,
			 MatButtonModule,
			 FormsModule, ],
  templateUrl: './criar-usuario.html',
  styleUrl: './criar-usuario.css',
})
export class CriarUsuario {
	
	nome='';
	email='';
	senha='';
	
	mensagens='';
	
	constructor(private authService: AuthService,private router: Router, private cdr: ChangeDetectorRef){}
	
	onSubmit(){
		this.authService.novoUsuario(this.nome, this.email, this.senha).subscribe({
			next: (response)=>{
				console.log('Usuario criado com sucesso', response);
				this.mensagens='Usuario criado com sucesso.';
				this.router.navigate(['/login']);
				this.cdr.detectChanges();
			},
			error: (err)=>{
				console.error('Nao foi possivel criar este usuario', err);
				this.mensagens= err.error?.erro ?? 'Nao foi possivel criar este usuario. Verifique se as informações são válidas';
				this.cdr.detectChanges();
			}
		});
	}
	
}
