import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { PetService, Pet } from '../services/pet';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatInputModule } from '@angular/material/input';

@Component({
  selector: 'app-pet-form',
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
		FormsModule],
  templateUrl: './pet-form.html',
  styleUrl: './pet-form.css',
})
export class PetForm implements OnInit {

  especies = ['CACHORRO','GATO','PEIXE','ROEDOR','AVE','OUTRA'];

  pet: Pet = {
    nome: '',
    especie: '',
  };

  modoEdicao = false;
  carregando = false;
  mensagens = '';

  constructor(
    private petService: PetService,
    private route: ActivatedRoute,
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    const idParam = this.route.snapshot.paramMap.get('id');

    if (idParam) {
      this.modoEdicao = true;
      this.carregando = true;
      this.petService.porId(Number(idParam)).subscribe({
        next: (data) => {
          this.pet = data;
          this.carregando = false;
          this.cdr.detectChanges();
        },
        error: (err) => {
          console.error('Erro ao carregar pet', err);
          this.mensagens = 'Pet nao encontrado.';
          this.carregando = false;
          this.cdr.detectChanges();
        }
      });
    }
  }

  onSubmit(): void {
    const acao = this.modoEdicao
      ? this.petService.atualizar(this.pet)
      : this.petService.criar(this.pet);

    acao.subscribe({
      next: () => {
        this.router.navigate(['/pets']);
      },
      error: (err) => {
        console.error('Erro ao salvar pet', err);
        this.mensagens = err.error?.erro ?? err.status === 403
          ? 'Sem permissao para essa acao.'
          : 'Nao foi possivel salvar o pet.';
        this.cdr.detectChanges();
      }
    });
  }
}