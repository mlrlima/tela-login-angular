import {Injectable, inject} from '@angular/core';
import {HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
	providedIn: 'root'
})
export class ApiService{
	private http: HttpClient= inject(HttpClient);
	
	constructor(){}
	
	getAboutInfo(): Observable<IAbout>{
		const aboutURL: string= '${environment.apiUrl}/about.json';
		return this.http.get<IAbout>(aboutURL);
	}
	
	getExperiencesInfo(): 
}