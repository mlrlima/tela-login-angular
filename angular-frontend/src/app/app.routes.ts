import { Routes } from '@angular/router';

export const routes: Routes = [
	{ path:'', pathMatch:'full', redirectTo: 'gestaoUsuarios' },
	{
	  path: 'gestaoUsuarios',
	  loadChildren: () => import('./gestaoUsuarios/gestao-usuarios-module').then(m => m.GestaoUsuariosModule)
	}
];
