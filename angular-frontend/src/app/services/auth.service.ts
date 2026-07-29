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
	      return this.http.post<LoginResponse>( `${environment.apiUrl}/auth/login`,
	        									  { email, senha },
	        									  { withCredentials: true } // manda/recebe o cookie
	      ).pipe(
			tap(res => {
					  this.localStorageService.set('id', res.id);
			          this.localStorageService.set('nome', res.nome);
			          this.localStorageService.set('email', res.email);
			          this.localStorageService.set('role', res.role);
			})

		);
  }
  
  logout(): void{
	this.http.post(`${environment.apiUrl}/auth/logout`, {}, { withCredentials: true })
	  .subscribe(() => localStorage.clear());
  }


    isLoggedIn(): boolean {
      return !!this.localStorageService.get('email');
    }
  
  novoUsuario(nome: string, email: string, senha: string): Observable<any> {
    return this.http.post(`${environment.apiUrl}/auth/usuario`, { nome, email, senha });
  }
  
}