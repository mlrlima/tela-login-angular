import { HttpInterceptorFn } from '@angular/common/http';
import { LocalStorageService } from '../services/local-storage-service';
import { inject } from '@angular/core';

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const localStorageService = inject(LocalStorageService); 
  const token = localStorageService.get('token');

  if (token) {
    req = req.clone({
      setHeaders: { Authorization: `Bearer ${token}` }
    });
  }
  return next(req);
};