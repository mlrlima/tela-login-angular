import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, tap } from 'rxjs';
import { environment } from '../../environments/environment';
import { LocalStorageService } from './local-storage-service';

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

	  constructor(private http: HttpClient, private localStorageService: LocalStorageService) {}

  login(email: string, senha: string): Observable<LoginResponse> {
    return this.http.post<LoginResponse>(`${environment.apiUrl}/auth/login`, { email, senha })
		.pipe(
			tap((res: any) => {
			          this.localStorageService.set('token', res.token);
					  this.localStorageService.set('id', res.id);
			          this.localStorageService.set('nome', res.nome);
			          this.localStorageService.set('email', res.email);
			          this.localStorageService.set('role', res.role);
			})

		);
  }
  
  logout(): void{
	localStorage.clear();
  }
  
  getToken(): string | null {
      return this.localStorageService.get('token');
    }

    isLoggedIn(): boolean {
      return !!this.getToken();
    }
  
  novoUsuario(nome: string, email: string, senha: string): Observable<any> {
    return this.http.post(`${environment.apiUrl}/auth/usuario`, { nome, email, senha });
  }
  
}