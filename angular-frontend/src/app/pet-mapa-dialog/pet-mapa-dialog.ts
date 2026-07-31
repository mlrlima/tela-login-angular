import { Component, Inject, AfterViewInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import * as L from 'leaflet';

export interface PetMapaDialogData {
  nome: string;
  especie: string;
  latitude: number | null | undefined;
  longitude: number | null | undefined;
}


// Fix do ícone padrão do Leaflet, que quebra com bundlers
const iconDefault = L.icon({
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconAnchor: [12, 41], // parte do icon que vai corresponder à localizaçao do marcador
  popupAnchor: [1, -34], //ponto que o popup deve abrir em relaçao com o iconAnchor
});
L.Marker.prototype.options.icon = iconDefault;

const iconCACHORRO = L.icon({
  iconUrl: 'assets/CACHORRO.webp',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [50, 50],
  iconAnchor: [25, 50],
  popupAnchor: [0, -45]
  });
const iconGATO = L.icon({
  iconUrl: 'assets/GATO.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [50, 50],
  iconAnchor: [25, 50],
  popupAnchor: [0, -45]
  });
const iconPEIXE = L.icon({
  iconUrl: 'assets/PEIXE.webp',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [50, 50],
  iconAnchor: [25, 50],
  popupAnchor: [0, -45]
  });
const iconROEDOR = L.icon({
  iconUrl: 'assets/ROEDOR.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [50, 50],
  iconAnchor: [25, 50],
  popupAnchor: [0, -45]
  });
const iconAVE = L.icon({
  iconUrl: 'assets/AVE.webp',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [50, 50],
  iconAnchor: [25, 50],
  popupAnchor: [0, -45]
  });
const iconOUTRA = L.icon({
  iconUrl: 'assets/OUTRA.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [50, 50],
  iconAnchor: [25, 50],
  popupAnchor: [0, -45]
  });
  
  

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

	  const icone = this.qualIcon(this.data.especie);
	  
      L.marker([lat, lng], {icon: icone}).addTo(this.map).bindPopup(this.data.nome).openPopup();

      this.map.invalidateSize();
    }, 0);
  }

  ngOnDestroy(): void {
    this.map?.remove();
  }
  
  private qualIcon(especie?: string): L.Icon {
    switch (especie) {
      case 'CACHORRO':
        return iconCACHORRO;

      case 'GATO':
        return iconGATO;

      case 'PEIXE':
        return iconPEIXE;

      case 'ROEDOR':
        return iconROEDOR;

      case 'AVE':
        return iconAVE;

      case 'OUTRA':
        return iconOUTRA;

      default:
        return iconDefault;
    }
  }
}