import { Component, Inject, AfterViewInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import * as L from 'leaflet';

export interface PetMapaDialogData {
  nome: string;
  latitude: number | null | undefined;
  longitude: number | null | undefined;
}

@Component({
  selector: 'app-pet-mapa-dialog',
  standalone: true,
  imports: [CommonModule,
			MatDialogModule,
			MatButtonModule,
			MatIconModule
			],
  templateUrl: './pet-mapa-dialog.html',
  styleUrl: './pet-mapa-dialog.css',
})
export class PetMapaDialog implements AfterViewInit, OnDestroy {
  
	private map?: L.Map;

  constructor( public dialogRef: MatDialogRef<PetMapaDialog>,
    @Inject(MAT_DIALOG_DATA)
	public data: PetMapaDialogData
  ) {}

  temLocalizacao(): boolean {
    return this.data.latitude!=null && this.data.longitude!=null;
  }

  ngAfterViewInit(): void {
    if (!this.temLocalizacao()) return;

    const lat = this.data.latitude as number;
    const lng = this.data.longitude as number;

    setTimeout(() => {
      this.map = L.map('pet-mapa-dialog').setView([lat, lng], 15);

      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors',
      }).addTo(this.map);

      L.marker([lat, lng]).addTo(this.map).bindPopup(this.data.nome).openPopup();

      this.map.invalidateSize();
    }, 0);
  }

  ngOnDestroy(): void {
    this.map?.remove();
  }
}