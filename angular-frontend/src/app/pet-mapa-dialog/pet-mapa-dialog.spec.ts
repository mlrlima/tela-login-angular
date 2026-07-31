import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PetMapaDialog } from './pet-mapa-dialog';

describe('PetMapaDialog', () => {
  let component: PetMapaDialog;
  let fixture: ComponentFixture<PetMapaDialog>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PetMapaDialog],
    }).compileComponents();

    fixture = TestBed.createComponent(PetMapaDialog);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
