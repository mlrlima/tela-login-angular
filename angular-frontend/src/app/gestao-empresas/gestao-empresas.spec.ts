import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GestaoEmpresas } from './gestao-empresas';

describe('GestaoEmpresas', () => {
  let component: GestaoEmpresas;
  let fixture: ComponentFixture<GestaoEmpresas>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [GestaoEmpresas],
    }).compileComponents();

    fixture = TestBed.createComponent(GestaoEmpresas);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
