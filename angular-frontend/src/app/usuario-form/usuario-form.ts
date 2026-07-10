import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common'; //*ngIf, *ngFor
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, //Lê parâmetros da URL. ex: /usuarios/5
		 Router, //navegar entre páginas.
		 RouterLink } from '@angular/router'; //<button routerLink="/usuarios">
import { UsuarioService, Usuario } from '../services/usuario';
import { AuthService } from '../services/auth.service';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';

@Component({
  selector: 'app-usuario-form',
  standalone: true, //pode ser definido independente, sem ser declarado em ngmodule
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

  // true quando o usuario logado esta editando o proprio cadastro
  // (seja pela rota /perfil, seja um admin abrindo o proprio registro em /usuarios/:id)
  autoEdicao = false;

  constructor(
    private usuarioService: UsuarioService,
    private authService: AuthService,
    private route: ActivatedRoute,
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    const idParam = this.route.snapshot.paramMap.get('id'); //pega o id que esta na url
    const idLogado = localStorage.getItem('id');
	
	console.log('idParam:', idParam);
	console.log('idLogado:', idLogado);

	//se o user logado tentar acessar outro id, ele só edita ele mesmo
	// ou se nao houver id na url
	const idAlvo =
	  localStorage.getItem('role') === 'USER' || !idParam
	    ? idLogado
	    : idParam;
		
	console.log('id alvo:', idAlvo);

    if (idAlvo) {
      this.usuarioService.porId(Number(idAlvo)).subscribe({ //para dizer o que fazer quando a resposta http chegar
        next: (data) => {
						//... (spread) copia todas as propriedades de data para um novo objeto.
          this.usuario = { ...data, senha: '' }; // limpa senha no form, so preenche se quiser trocar
          this.autoEdicao = idLogado != null && String(data.id) === idLogado;
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
    const payload = { ...this.usuario }; //Cria uma cópia do objeto para nao editar diretamente
    if (!payload.senha) {
      delete payload.senha; // em branco = mantem a senha atual (backend ja trata isso)
    }

    this.usuarioService.atualizar(payload).subscribe({
      next: () => {
        if (this.autoEdicao) {
          // dados/token do usuario logado mudaram: desloga para ele entrar de novo com as novas infos
          this.authService.logout();
          this.router.navigate(['/login']);
          return;
        }
        this.router.navigate(['/usuarios']);
      },
      error: (err) => {
        console.error('Erro ao salvar usuario', err);
        this.mensagens = err.error?.erro ?? err.status === 403
          ? 'Sem permissao para essa acao.'
          : 'Nao foi possivel salvar o usuario.';
        this.cdr.detectChanges();
      }
    });
	
	this.cdr.detectChanges();
  }
}