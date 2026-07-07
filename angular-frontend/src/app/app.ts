import { Component, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { Login } from './login/login';
import { CriarUsuario } from './criar-usuario/criar-usuario';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet,
			Login,
			CriarUsuario ],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {}
