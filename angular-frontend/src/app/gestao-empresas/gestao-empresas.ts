import { Component,OnInit, ChangeDetectorRef } from '@angular/core';

import { EmpresaService, Empresa } from '../services/empresa';

import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatChipsModule } from '@angular/material/chips';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { MatMenuModule } from '@angular/material/menu';

@Component({
  selector: 'app-gestao-empresas',
  standalone: true,
  imports:   [  FormsModule,
	RouterLink,
	CommonModule,
        MatCardModule,
        MatButtonModule,
        MatIconModule,
        MatProgressSpinnerModule,
        MatFormFieldModule,
        MatInputModule,
		MatMenuModule,
        MatChipsModule],
  templateUrl: './gestao-empresas.html',
  styleUrl: './gestao-empresas.css',
})
export class GestaoEmpresas implements OnInit{
	
	empresas: Empresa[] = [];
	carregando = true;
	mensagens = '';
	empresasFiltradas: Empresa[] = [];
	termoBusca: string = '';

	constructor(private empresaService: EmpresaService,
			  private cdr: ChangeDetectorRef,) {}
	
	  get isAdmin(): boolean {
	    return localStorage.getItem('role') === 'ADMIN';
	  }
			  
	ngOnInit(): void {
	  this.carregar();
	}
	
	carregar(): void {
	this.carregando = true;
	  this.empresaService.todas().subscribe({
	    next: (empresas) => {
		  console.log(empresas);
	      
		  this.empresas = empresas;
	      this.filtrarEmpresas();
		  console.log(this.empresasFiltradas);
		  
	      this.carregando = false;
		this.cdr.detectChanges();
	    },
	    error: (err) => {
	      this.mensagens = 'Erro ao carregar empresas.';
	      this.carregando = false;
		this.cdr.detectChanges();
	    }
	  });
	}
	
	filtrarEmpresas(): void {
	   const termo = this.termoBusca.trim().toLowerCase();

	   if (!termo) {
	     this.empresasFiltradas = this.empresas;
	     return;
	   }

	   this.empresasFiltradas = this.empresas.filter(empresa =>
	     empresa.nome?.toLowerCase().includes(termo) 
	   );
	 }
	 
	 remover(id: number | undefined): void {
	   if (id == null) return;
	   if (!confirm('Confirmaçao para remover esta empresa')) return;

	   this.empresaService.remover(id).subscribe({
	     next: () => {
	       this.empresas = this.empresas.filter(e => e.id !== id);
	 	   this.filtrarEmpresas();
	 	
	       this.cdr.detectChanges();
	     },
	     error: (err) => {
	       console.error('Erro ao remover empresa', err);
	       this.mensagens = 'Nao foi possivel remover esta empresa.';
	       this.cdr.detectChanges();
	     }
	   });
	 }
	
}
