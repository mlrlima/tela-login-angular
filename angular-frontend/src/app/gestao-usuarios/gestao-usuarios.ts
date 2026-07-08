import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { UsuarioService, Usuario } from '../services/usuario';
import { MatChipsModule } from '@angular/material/chips';
import { MatCardModule } from '@angular/material/card';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';

@Component({
  selector: 'app-gestao-usuarios',
  standalone: true,
  imports: [CommonModule,
			RouterLink,
			MatCardModule,
			MatButtonModule,
			MatIconModule,
			MatProgressSpinnerModule,
			MatChipsModule,],
  templateUrl: './gestao-usuarios.html',
  styleUrl: './gestao-usuarios.css',
})
export class GestaoUsuarios implements OnInit {

  usuarios: Usuario[] = [];
  carregando = true;
  mensagens = '';

  constructor(private usuarioService: UsuarioService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.carregar();
  }

  carregar(): void {
    this.carregando = true;
    this.usuarioService.todos().subscribe({
      next: (data) => {
        this.usuarios = data;
        this.carregando = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        console.error('Erro ao carregar usuarios', err);
        this.mensagens = 'Nao foi possivel carregar os usuarios.';
        this.carregando = false;
        this.cdr.detectChanges();
      }
    });
  }

  remover(id: number | undefined): void {
    if (id == null) return;
    if (!confirm('Confirmaçao para remover este usuario')) return;

    this.usuarioService.remover(id).subscribe({
      next: () => {
        this.usuarios = this.usuarios.filter(u => u.id !== id);
        this.cdr.detectChanges();
      },
      error: (err) => {
        console.error('Erro ao remover usuario', err);
        this.mensagens = 'Nao foi possivel remover este usuario.';
        this.cdr.detectChanges();
      }
    });
  }
}