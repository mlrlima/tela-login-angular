import { Injectable } from "@angular/core";
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';


export interface Empresa {
  id?: number;
  nome: string;
  usuarios?: UsuarioRelacionado[];
}

export interface UsuarioRelacionado {
    id: number;
    email: string;
}

@Injectable({ providedIn: 'root' })
export class EmpresaService {
	
	constructor(private http: HttpClient) {}
	
	todas(): Observable<Empresa[]> { //retorna um Observable que, quando receber uma resposta do servidor, fornecerá um objeto do tipo Usuario.
	  return this.http.get<Empresa[]>(`${environment.apiUrl}/empresa/all`);
	}

	porId(id: number): Observable<Empresa> {
	  return this.http.get<Empresa>(`${environment.apiUrl}/empresa/${id}`);
	}

	porNome(nome: string): Observable<Empresa> {
	  return this.http.get<Empresa>(`${environment.apiUrl}/empresa`, { params: { nome } });
	}
	
	criar(empresa: Empresa): Observable<Empresa> {
	  return this.http.post<Empresa>(`${environment.apiUrl}/empresa`, empresa);
	}

	atualizar(empresa: Empresa): Observable<Empresa> {
	  return this.http.put<Empresa>(`${environment.apiUrl}/empresa`, empresa);
	}

	remover(id: number): Observable<void> {
	  return this.http.delete<void>(`${environment.apiUrl}/empresa/${id}`);
	}
	
}
