import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, tap } from 'rxjs';

export interface LoginResponse {
  token: string;
  id: number;
  nome: string;
  email: string;
  role: string;
}

@Injectable({
		providedIn: 'root' 
	})
export class AuthService {

	  constructor(private http: HttpClient) {}

  login(email: string, senha: string): Observable<LoginResponse> {
    return this.http.post<LoginResponse>('/api/auth/login', { email, senha })
		.pipe(
			tap((res: any) => {
			          localStorage.setItem('token', res.token);
					  localStorage.setItem('id', res.id);
			          localStorage.setItem('nome', res.nome);
			          localStorage.setItem('email', res.email);
			          localStorage.setItem('role', res.role);
			})

		);
  }
  
  logout(): void{
	const token=localStorage.getItem('token');
	localStorage.clear();
	
	if(token){
      this.http.post('/api/auth/logout', {}, {
        headers: { Authorization: `Bearer ${token}` }
      }).subscribe();
    }
  }
  
  getToken(): string | null {
      return localStorage.getItem('token');
    }

    isLoggedIn(): boolean {
      return !!this.getToken();
    }
  
  novoUsuario(nome: string, email: string, senha: string): Observable<any> {
    return this.http.post('/api/usuario', { nome, email, senha });
  }
  
}