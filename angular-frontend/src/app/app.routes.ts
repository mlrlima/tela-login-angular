import { Routes } from '@angular/router';

import { Login } from './login/login';
import { CriarUsuario } from './criar-usuario/criar-usuario';
import { GestaoUsuarios } from './gestao-usuarios/gestao-usuarios';
import { UsuarioForm } from './usuario-form/usuario-form';
import { GestaoPets } from './gestao-pets/gestao-pets';
import { PetForm } from './pet-form/pet-form';
import { authGuard } from './guards/auth-guard';
import { adminGuard } from './guards/admin-guard-guard';

export const routes: Routes = [
	{ path: '', redirectTo: 'login', pathMatch: 'full' },
	
	{ path: 'login', component: Login },
    { path: 'criar-usuario', component: CriarUsuario },
	
	{ path: 'pets', component: GestaoPets, canActivate: [authGuard] },
	{ path: 'pets/novo', component: PetForm, canActivate: [authGuard] },
    { path: 'pets/:id', component: PetForm, canActivate: [authGuard] },
	
	{ path: 'usuarios', component: GestaoUsuarios, canActivate: [authGuard, adminGuard] },
	{ path: 'usuarios/:id', component: UsuarioForm, canActivate: [authGuard, adminGuard] }
];
