import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, tap } from 'rxjs';
import { environment } from '../../environments/environment';

interface LoginResponse{
	token: string;
	email: string;
	role: string;
}

@Injectable({provideIn: 'root'})
export class AuthService{
	private apiUrl= `${environment.apiUrl}/auth`;
	
	constructor(private http: HttpClient){}
	
	login(email:string, senha:string): Observable <LoginResponse>{
		return this.http.post<LoginResponse>(`${this.apiUrl}/login`, {email, senha})
			.pipe(
				tap(res=> {
					localStorage.setItem('token', res.token);
					localStorage.setItem('email', res.email);
					localStorage.setItem('role', res.role);
				})
			);
	}
	
	logout(): void {
	  localStorage.removeItem('token');
	  localStorage.removeItem('email');
	  localStorage.removeItem('role');
	}

	getToken(): string | null {
	  return localStorage.getItem('token');
	}

	isLoggedIn(): boolean {
	  return !!this.getToken();
	}

	getRole(): string | null {
	  return localStorage.getItem('role');
	}
}
