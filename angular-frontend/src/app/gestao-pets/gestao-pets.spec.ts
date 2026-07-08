import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GestaoPets } from './gestao-pets';

describe('GestaoPets', () => {
  let component: GestaoPets;
  let fixture: ComponentFixture<GestaoPets>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [GestaoPets],
    }).compileComponents();

    fixture = TestBed.createComponent(GestaoPets);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
