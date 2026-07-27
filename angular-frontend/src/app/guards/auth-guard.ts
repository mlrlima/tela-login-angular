import { CanActivateFn, Router } from '@angular/router';
import { inject } from '@angular/core';
import { LocalStorageService } from '../services/local-storage-service';

export const authGuard: CanActivateFn = () => {
  const router = inject(Router);
  
  const localStorageService = inject(LocalStorageService); 
  const token = localStorageService.get('token');

  if (token) return true;

  router.navigate(['/login']);
  return false;
};