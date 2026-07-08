import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { PetService, Pet } from '../services/pet';
import { MatCardModule } from '@angular/material/card';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';

@Component({
  selector: 'app-gestao-pets',
  standalone: true,
  imports: [CommonModule,
			RouterLink,
			MatCardModule,
			MatButtonModule,
			MatIconModule,
			MatProgressSpinnerModule,],
  templateUrl: './gestao-pets.html',
  styleUrl: './gestao-pets.css',
})
export class GestaoPets implements OnInit {

  pets: Pet[] = [];
  carregando = true;
  mensagens = '';

  constructor(private petService: PetService, private cdr: ChangeDetectorRef) {}

    ngOnInit(): void {
      this.carregar();
    }

	carregar(): void {
	    this.carregando = true;
	    this.petService.listar().subscribe({
	      next: (data) => {
	        this.pets = data;
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
	    if (!confirm('Confirmaçao para remover')) return;

	    this.petService.remover(id).subscribe({
	      next: () => {
	        this.pets = this.pets.filter(p => p.id !== id);
			this.cdr.detectChanges();
	      },
	      error: (err) => {
	        console.error('Erro ao remover pet', err);
	        this.mensagens = 'Nao foi possivel remover este pet.';
			this.cdr.detectChanges();
	      }
	    });
	  }
	}
