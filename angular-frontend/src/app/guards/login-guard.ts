import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { LocalStorageService } from '../services/local-storage-service';

export const loginGuard: CanActivateFn = () => {
	
	const localStorageService = inject(LocalStorageService); 
	localStorageService.clear();
	
  return true;
};
