import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { PetService, Pet } from '../services/pet';
import { MatCardModule } from '@angular/material/card';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { FormsModule } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import { PetMapaDialog } from '../pet-mapa-dialog/pet-mapa-dialog';

@Component({
  selector: 'app-gestao-pets',
  standalone: true,
  imports: [CommonModule,
			RouterLink,
			MatCardModule,
			MatButtonModule,
			MatIconModule,
			MatProgressSpinnerModule,
			MatFormFieldModule,
			MatInputModule,
			FormsModule],
  templateUrl: './gestao-pets.html',
  styleUrl: './gestao-pets.css',
})
export class GestaoPets implements OnInit {

	pets: any[] = [];
	paginaAtual = 0;
	tamanhoPagina = 10;
	totalPaginas = 0;
	totalElementos = 0;

  carregando = true;
  mensagens = '';
  termoBusca= '';
  petsBuscados: Pet[] = [];

  constructor(private petService: PetService,
			private cdr: ChangeDetectorRef,
			private dialog: MatDialog) {}

    ngOnInit(): void {
      this.carregar();
    }
	
	verMapa(pet: Pet): void {
	    this.dialog.open(PetMapaDialog, {
	      width: '400px',
	      data: {
	        nome: pet.nome,
			especie: pet.especie,
	        latitude: pet.latitude,
	        longitude: pet.longitude,
	      },
	    });
	  }
	
	proximaPagina(): void {
	  if (this.paginaAtual + 1 < this.totalPaginas) {
	    this.paginaAtual++;
	    this.carregar();
	  }
	}
	paginaAnterior(): void {
	  if (this.paginaAtual > 0) {
	    this.paginaAtual--;
	    this.carregar();
	  }
	}

	buscarPets(): void {
	  const termo = this.termoBusca.trim().toLowerCase();

	  if (!termo) {
	    this.petsBuscados = this.pets;
	    return;
	  }

	  this.petsBuscados = this.pets.filter(pet =>
	    pet.nome?.toLowerCase().includes(termo) ||
	    pet.especie?.toLowerCase().includes(termo) ||
	    pet.dono?.email?.toLowerCase().includes(termo)
	  );
	}

	carregar(): void {
	    this.carregando = true;
	    this.petService.listar(this.paginaAtual, this.tamanhoPagina).subscribe({
	      next: (res) => {
	        this.pets = res.content;
			
			this.totalPaginas = res.totalPages;
			this.totalElementos = res.totalElements;
			
			this.buscarPets();
	        this.carregando = false;
	        this.cdr.detectChanges();
	      },
	      error: (err) => {
	        this.mensagens = 'Nao foi possivel carregar os pets.';
	        this.carregando = false;
	        this.cdr.detectChanges();
	      }
	    });
	  }
	
	remover(id: number | undefined): void {
	    if (id == null) return;
	    if (!confirm('Confirmaçao para remover este pet')) return;

	    this.petService.remover(id).subscribe({
	      next: () => {
	        this.carregar();
	      },
	      error: (err) => {
	        console.error('Erro ao remover pet', err);
	        this.mensagens = 'Nao foi possivel remover este pet.';
			this.cdr.detectChanges();
	      }
	    });
	  }
	}