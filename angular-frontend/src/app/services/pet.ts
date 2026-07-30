import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

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

  listar(page: number = 0, size: number = 10): Observable<any> {
    return this.http.get<any>(`${environment.apiUrl}/pet/all?page=${page}&size=${size}`);
  }

  porId(id: number): Observable<Pet> {
    return this.http.get<Pet>(`${environment.apiUrl}/pet/${id}`);
  }

  criar(pet: Pet): Observable<Pet> {
    return this.http.post<Pet>(`${environment.apiUrl}/pet`, pet);
  }

  atualizar(pet: Pet): Observable<Pet> {
    return this.http.put<Pet>(`${environment.apiUrl}/pet`, pet);
  }

  remover(id: number): Observable<void> {
    return this.http.delete<void>(`${environment.apiUrl}/pet/${id}`);
  }
}