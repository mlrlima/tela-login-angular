import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface Usuario {
  id?: number;
  nome: string;
  email: string;
  senha?: string;
  role: string;
}

@Injectable({ providedIn: 'root' })
export class UsuarioService {

  constructor(private http: HttpClient) {}

  todos(): Observable<Usuario[]> {
    return this.http.get<Usuario[]>('/api/usuario/all');
  }
  
  porId(id: number): Observable<Usuario> {
    return this.http.get<Usuario>(`/api/usuario/${id}`);
  }

  porEmail(email: string): Observable<Usuario> {
    return this.http.get<Usuario>('/api/usuario', { params: { email } });
  }

  atualizar(usuario: Usuario): Observable<Usuario> {
    return this.http.put<Usuario>('/api/usuario', usuario);
  }

  remover(id: number): Observable<void> {
    return this.http.delete<void>(`/api/usuario/${id}`);
  }
}