import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';
import { UsuarioRelacionado } from './empresa';

export interface Usuario {
  id?: number;
  nome: string;
  email: string;
  senha?: string;
  role: string;
  empresas?: EmpresaRelacionada[];
}

export interface EmpresaRelacionada {
    id: number;
    nome: string;
}

@Injectable({ providedIn: 'root' })
export class UsuarioService {

  constructor(private http: HttpClient) {}

  todos(page: number = 0, size: number = 10): Observable<any> { //retorna um Observable que, quando receber uma resposta do servidor, fornecerá um objeto
    return this.http.get<any>(`${environment.apiUrl}/usuario/all?page=${page}&size=${size}`);
  }
  
  porId(id: number): Observable<Usuario> {
    return this.http.get<Usuario>(`${environment.apiUrl}/usuario/${id}`);
  }

  porEmail(email: string): Observable<UsuarioRelacionado> {
    return this.http.get<UsuarioRelacionado>(`${environment.apiUrl}/usuario`, { params: { email } });
  }

  atualizar(usuario: Usuario): Observable<Usuario> {
    return this.http.put<Usuario>(`${environment.apiUrl}/usuario`, usuario);
  }

  remover(id: number): Observable<void> {
    return this.http.delete<void>(`${environment.apiUrl}/usuario/${id}`);
  }
}