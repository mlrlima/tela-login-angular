import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface Usuario {
  id: number;
  nome: string;
  email: string;
  role: string;
}

export interface Pet {
  id?: number;
  nome: string;
  especie: string;
  dataNascimento?: string | null; // ISO format 'YYYY-MM-DD' 
  dono?: Usuario;
}

@Injectable({ providedIn: 'root' })
export class PetService {

  constructor(private http: HttpClient) {}

  listar(): Observable<Pet[]> {
    return this.http.get<Pet[]>('/api/pet/all');
  }

  porId(id: number): Observable<Pet> {
    return this.http.get<Pet>(`/api/pet/${id}`);
  }

  criar(pet: Pet): Observable<Pet> {
    return this.http.post<Pet>('/api/pet', pet);
  }

  atualizar(pet: Pet): Observable<Pet> {
    return this.http.put<Pet>('/api/pet', pet);
  }

  remover(id: number): Observable<void> {
    return this.http.delete<void>(`/api/pet/${id}`);
  }
}