import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { UsuarioService, Usuario } from '../services/usuario';

import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatChipsModule } from '@angular/material/chips';


@Component({
  selector: 'app-gestao-usuarios',
  standalone: true,
  imports: [  CommonModule,
      FormsModule,
      RouterLink,
      MatCardModule,
      MatButtonModule,
      MatIconModule,
      MatProgressSpinnerModule,
      MatFormFieldModule,
      MatInputModule,
      MatChipsModule],
  templateUrl: './gestao-usuarios.html',
  styleUrl: './gestao-usuarios.css',
})
export class GestaoUsuarios implements OnInit {

  usuarios: Usuario[] = [];
  carregando = true;
  mensagens = '';
  usuariosFiltrados: Usuario[] = [];
   termoBusca: string = '';

  constructor(private usuarioService: UsuarioService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.carregar();
  }

  carregar(): void {
	this.carregando = true;
    this.usuarioService.todos().subscribe({
      next: (usuarios) => {
        this.usuarios = usuarios;
        this.filtrarUsuarios();
        this.carregando = false;
		this.cdr.detectChanges();
      },
      error: (err) => {
        this.mensagens = 'Erro ao carregar usuários.';
        this.carregando = false;
		this.cdr.detectChanges();
      }
    });
  }

  filtrarUsuarios(): void {
     const termo = this.termoBusca.trim().toLowerCase();

     if (!termo) {
       this.usuariosFiltrados = this.usuarios;
       return;
     }

     this.usuariosFiltrados = this.usuarios.filter(usuario =>
       usuario.nome?.toLowerCase().includes(termo) ||
       usuario.email?.toLowerCase().includes(termo) ||
       usuario.role?.toLowerCase().includes(termo)
     );
   }
  
  remover(id: number | undefined): void {
    if (id == null) return;
    if (!confirm('Confirmaçao para remover este usuario')) return;

    this.usuarioService.remover(id).subscribe({
      next: () => {
        this.usuarios = this.usuarios.filter(u => u.id !== id);
		this.filtrarUsuarios();
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