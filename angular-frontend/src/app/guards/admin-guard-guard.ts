import { CanActivateFn, Router } from '@angular/router';
import { inject } from '@angular/core';
import { LocalStorageService } from '../services/local-storage-service';
import { AuthService } from '../services/auth.service';

export const adminGuard: CanActivateFn = () => {
  const router = inject(Router);
  
  const localStorageService = inject(LocalStorageService); 
  const role = localStorageService.get('role');
  
  const authService = inject(AuthService);

  if (authService.isLoggedIn() && role === 'ADMIN') return true;

  router.navigate(['/pets']);
  
  return false;
};