import { Component } from '@angular/core';
import { RouterOutlet,RouterLink, Router } from '@angular/router';
import { CommonModule } from '@angular/common';
import { AuthService } from './services/auth.service';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet,
			CommonModule,
			RouterLink,
			MatIconModule,
			MatButtonModule],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  constructor(private authService: AuthService, private router: Router) {}

  get logado(): boolean {
    return this.authService.isLoggedIn();
  }

  get emailUsuario(): string | null {
    return localStorage.getItem('email');
  }

  sair(): void {
    this.authService.logout();
    this.router.navigate(['/login']);
  }
  
  get isAdmin(): boolean {
    return localStorage.getItem('role') === 'ADMIN';
  }
}