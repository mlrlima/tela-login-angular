import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, NgForm } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { PetService, Pet } from '../services/pet';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatInputModule } from '@angular/material/input';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatNativeDateModule } from '@angular/material/core';

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
		    MatDatepickerModule,
		    MatNativeDateModule],
  templateUrl: './pet-form.html',
  styleUrl: './pet-form.css',
})
export class PetForm implements OnInit {

  especies = ['CACHORRO','GATO','PEIXE','ROEDOR','AVE','OUTRA'];

  pet: Pet = {
    nome: '',
    especie: '',
	latitude: null,
	longitude: null,
	intervaloMover: 5,
	peso: 0,
  };
  dataNascimentoDate: Date | null=null;
  hoje = new Date();

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
		  
		  // converte string ISO -> Date para o datepicker
		  if (data.dataNascimento) {
		    this.dataNascimentoDate = new Date(data.dataNascimento + 'T00:00:00');
		  }
		  
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

  onSubmit(form: NgForm): void {
    if (form.invalid) {
      this.mensagens = 'Verifique se todas as informações são válidas.';
      return;
    }
	
	// converte Date -> string ISO (yyyy-MM-dd) antes de enviar
	if (this.dataNascimentoDate) {
	  const ano = this.dataNascimentoDate.getFullYear();
	  const mes = String(this.dataNascimentoDate.getMonth() + 1).padStart(2, '0');
	  const dia = String(this.dataNascimentoDate.getDate()).padStart(2, '0');
	  this.pet.dataNascimento = `${ano}-${mes}-${dia}`;
	} else {
	  this.pet.dataNascimento = undefined;
	}

    const acao = this.modoEdicao
      ? this.petService.atualizar(this.pet)
      : this.petService.criar(this.pet);
	  
    acao.subscribe({
      next: (petSalvo) => {
		if (petSalvo.id!=null && this.pet.latitude!=null && this.pet.longitude!=null) {
		        this.petService.atualizarLocalizacao(petSalvo.id, this.pet.latitude, this.pet.longitude)
		          .subscribe({
		            next: () => this.router.navigate(['/pets']),
		            error: (err) => {
		              console.error('Erro ao atualizar localizacao', err);
		              // mesmo se a localizacao falhar, o pet ja foi salvo — segue o fluxo
		              this.router.navigate(['/pets']);
		            }
		          });
		      } else {
		        this.router.navigate(['/pets']);
		      }
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