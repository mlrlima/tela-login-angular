import { CanActivateFn, Router } from '@angular/router';
import { inject } from '@angular/core';
import { LocalStorageService } from '../services/local-storage-service';

export const adminGuard: CanActivateFn = () => {
  const router = inject(Router);
  
  const localStorageService = inject(LocalStorageService); 
  const token = localStorageService.get('token');
  const role = localStorageService.get('role');

  if (token && role === 'ADMIN') return true;

  router.navigate(['/pets']);
  
  return false;
};