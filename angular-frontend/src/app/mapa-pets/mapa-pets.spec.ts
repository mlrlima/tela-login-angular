import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MapaPets } from './mapa-pets';

describe('MapaPets', () => {
  let component: MapaPets;
  let fixture: ComponentFixture<MapaPets>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MapaPets],
    }).compileComponents();

    fixture = TestBed.createComponent(MapaPets);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
