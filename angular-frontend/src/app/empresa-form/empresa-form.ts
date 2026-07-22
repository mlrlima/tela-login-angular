import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatInputModule } from '@angular/material/input';
import { MatChipsModule } from '@angular/material/chips';
import { EmpresaService, Empresa } from '../services/empresa';
import { UsuarioService } from '../services/usuario';

@Component({
  selector: 'app-empresa-form',
  standalone: true,
  imports: [CommonModule,
      FormsModule,
      RouterLink,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatProgressSpinnerModule,
    MatChipsModule],
  templateUrl: './empresa-form.html',
  styleUrl: './empresa-form.css',
})
export class EmpresaForm implements OnInit{

  empresa: Empresa = {
    nome: '',
    usuarios: [],
  };

  modoEdicao = false;
  carregando = false;
  mensagens = '';

  novoEmail = '';
  erroUsuario = '';

  constructor(
    private empresaService: EmpresaService,
    private usuarioService: UsuarioService,
    private route: ActivatedRoute,
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    const idParam = this.route.snapshot.paramMap.get('id');

    if (idParam && !isNaN(Number(idParam))) {
      this.modoEdicao = true;
      this.carregando = true;
      this.empresaService.porId(Number(idParam)).subscribe({
        next: (data) => {
          this.empresa = data;
          if (!this.empresa.usuarios) this.empresa.usuarios = [];
          this.carregando = false;
          this.cdr.detectChanges();
        },
        error: (err) => {
          console.error('Erro ao carregar empresa', err);
          this.mensagens = 'Empresa nao encontrada.';
          this.carregando = false;
          this.cdr.detectChanges();
        }
      });
    }
  }

  adicionarUsuario(): void {
    const email = this.novoEmail.trim();
    if (!email) return;

    const jaExiste = this.empresa.usuarios?.some(
      u => u.email.toLowerCase() === email.toLowerCase()
    );
    if (jaExiste) {
      this.erroUsuario = 'Usuário já adicionado.';
      return;
    }

    this.usuarioService.porEmail(email).subscribe({
      next: (usuario) => {
        if (!this.empresa.usuarios) this.empresa.usuarios = [];
        this.empresa.usuarios.push(usuario);
        this.novoEmail = '';
        this.erroUsuario = '';
        this.cdr.detectChanges();
      },
      error: (err) => {
        console.error('Erro ao buscar usuário', err);
        this.erroUsuario = 'Usuário não encontrado.';
        this.cdr.detectChanges();
      }
    });
  }

  removerUsuario(id: number): void {
    this.empresa.usuarios = this.empresa.usuarios?.filter(u => u.id !== id);
  }

  onSubmit(): void {
      const acao = this.modoEdicao
        ? this.empresaService.atualizar(this.empresa)
        : this.empresaService.criar(this.empresa);

      acao.subscribe({
        next: () => {
          this.router.navigate(['/empresas']);
        },
        error: (err) => {
          console.error('Erro ao salvar empresa', err);
          this.mensagens = err.error?.erro ?? err.status === 403
            ? 'Sem permissao para essa acao.'
            : 'Nao foi possivel salvar a empresa.';
          this.cdr.detectChanges();
        }
      });
    }
}