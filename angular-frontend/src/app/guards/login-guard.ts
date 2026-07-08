import { CanActivateFn, Router } from '@angular/router';

export const loginGuard: CanActivateFn = () => {
	
	localStorage.clear();
	
  return true;
};
