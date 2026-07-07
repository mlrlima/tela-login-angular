import { Routes } from '@angular/router';

import { Login } from './login/login';
import { CriarUsuario } from './criar-usuario/criar-usuario';

export const routes: Routes = [
	{ path: '', redirectTo: 'login', pathMatch: 'full' },
	{ path: 'login', component: Login },
    { path: 'criar-usuario', component: CriarUsuario },
];
