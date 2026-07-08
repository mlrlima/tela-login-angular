import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { UsuarioService, Usuario } from '../services/usuario';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';

@Component({
  selector: 'app-usuario-form',
  standalone: true,
  imports: [CommonModule,
			FormsModule,
		MatCardModule,
		MatFormFieldModule,
		MatInputModule,
		MatSelectModule,
		MatButtonModule,
		MatIconModule,
		MatProgressSpinnerModule,
		FormsModule,],
  templateUrl: './usuario-form.html',
  styleUrl: './usuario-form.css',
})
export class UsuarioForm implements OnInit {

  roles = ['USER', 'ADMIN'];

  usuario: Usuario = {
    nome: '',
    email: '',
    senha: '',
    role: 'USER'
  };

  carregando = true;
  mensagens = '';

  constructor(
    private usuarioService: UsuarioService,
    private route: ActivatedRoute,
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    const idParam = this.route.snapshot.paramMap.get('id');

    if (idParam) {
      this.usuarioService.porId(Number(idParam)).subscribe({
        next: (data) => {
          this.usuario = { ...data, senha: '' }; // limpa senha no form, so preenche se quiser trocar
          this.carregando = false;
          this.cdr.detectChanges();
        },
        error: (err) => {
          console.error('Erro ao carregar usuario', err);
          this.mensagens = 'Usuario nao encontrado.';
          this.carregando = false;
          this.cdr.detectChanges();
        }
      });
    } else {
      this.carregando = false;
    }
  }

  onSubmit(): void {
    const payload = { ...this.usuario };
    if (!payload.senha) {
      delete payload.senha; // em branco = mantem a senha atual (backend ja trata isso)
    }

    this.usuarioService.atualizar(payload).subscribe({
      next: () => {
        this.router.navigate(['/usuarios']);
      },
      error: (err) => {
        console.error('Erro ao salvar usuario', err);
        this.mensagens = err.status === 403
          ? 'Sem permissao para essa acao.'
          : 'Nao foi possivel salvar o usuario.';
        this.cdr.detectChanges();
      }
    });
  }
}