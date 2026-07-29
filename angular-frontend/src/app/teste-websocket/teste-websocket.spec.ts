import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TesteWebsocket } from './teste-websocket';

describe('TesteWebsocket', () => {
  let component: TesteWebsocket;
  let fixture: ComponentFixture<TesteWebsocket>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TesteWebsocket],
    }).compileComponents();

    fixture = TestBed.createComponent(TesteWebsocket);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
