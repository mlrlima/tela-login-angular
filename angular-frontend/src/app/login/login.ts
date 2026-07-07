import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { Router, RouterLink } from '@angular/router';
import { AuthService } from '../services/auth.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [ FormsModule,
             CommonModule,
			 RouterLink ],
  templateUrl: './login.html',
  styleUrl: './login.css',
})
export class Login {

  email='';
  senha='';

  mensagens='';
  carregando=false;

  constructor(private authService: AuthService, private router: Router){}

  onSubmit(){
    this.carregando = true;
    this.mensagens = '';

    this.authService.login(this.email, this.senha).subscribe({
      next: (response) => {
        console.log('Login realizado com sucesso', response);
        this.carregando = false;
        this.router.navigate(['/criar-usuario']);
      },
      error: (err) => {
        console.error('Falha no login', err);
        this.carregando = false;

        if (err.status === 401) {
          this.mensagens = err.error?.erro ?? 'Email ou senha invalidos.';
        } else if (err.status === 0) {
          this.mensagens = 'Nao foi possivel conectar ao servidor.';
        } else {
          this.mensagens = 'Erro inesperado. Tente novamente.';
        }
      }
    });
  }

}