import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class LocalStorageService {
	
	//salvar dados, todos em formato string
	set(chave: string, valor: any): void {
	  localStorage.setItem(chave, JSON.stringify(valor));
	}
	
	// converte os dados para o tipo original
	get<T>(chave: string): T | null {
	  const dado = localStorage.getItem(chave);
	  return dado ? JSON.parse(dado) as T : null;
	}
	
	// Remove um item
	remove(chave: string): void {
	  localStorage.removeItem(chave);
	}
	
	//limpa tudo
	clear(): void {
	  localStorage.clear();
	}
	
}
