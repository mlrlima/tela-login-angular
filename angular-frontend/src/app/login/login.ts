import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { AuthService } from '../services/auth.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [ FormsModule,
			 CommonModule ],
  templateUrl: './login.html',
  styleUrl: './login.css',
})

export class Login {
	email='';
	senha='';
	msgErro='';
	
	constructor(private authService: AuthService){}
	
	onSubmit(){
		this.authService.login(this.email, this.senha).subscribe({
			next: (response)=>{
				console.log('Login funcionou', response);
			},
			error: (err)=>{
				console.error('Login nao funcionou', err);
				this.msgErro='Login não funcionou.';
			}
		});
	}
}
