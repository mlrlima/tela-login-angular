import { Component, OnInit, OnDestroy, AfterViewInit } from '@angular/core';
import * as L from 'leaflet';
import { Subscription } from 'rxjs';
import { PetService, Pet } from '../services/pet';
import { WebsocketService } from '../services/websocket-service';

// Fix do ícone padrão do Leaflet, que quebra com bundlers
const iconDefault = L.icon({
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
});
L.Marker.prototype.options.icon = iconDefault;

@Component({
  selector: 'app-mapa-pets',
  standalone: true,
  imports: [],
  templateUrl: './mapa-pets.html',
  styleUrl: './mapa-pets.css',
})
export class MapaPets implements OnInit, AfterViewInit, OnDestroy {
  private map!: L.Map;
  private marcadores = new Map<number, L.Marker>();
  private sub?: Subscription;

  constructor(private petService: PetService, private ws: WebsocketService) {}

  ngOnInit() {
    this.sub = this.ws.petLocation$.subscribe((loc) => {
      if (loc) this.atualizarMarcador(loc.id, loc.latitude, loc.longitude);
    });
  }

  ngAfterViewInit() {
    this.map = L.map('mapa-pets').setView([-8.0476, -34.877], 13); // Recife como centro inicial

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
    }).addTo(this.map);

    this.petService.listar(0, 100).subscribe((pagina) => {
      const pets: Pet[] = pagina.content ?? pagina;
      pets.forEach((pet) => {
        if (pet.id != null && pet.latitude != null && pet.longitude != null) {
          this.atualizarMarcador(pet.id, pet.latitude, pet.longitude, pet.nome);
        }
      });
    });
  }

  private atualizarMarcador(id: number, lat: number, lng: number, nome?: string) {
    const existente = this.marcadores.get(id);
    if (existente) {
      existente.setLatLng([lat, lng]);
    } else {
      const marcador = L.marker([lat, lng]).addTo(this.map);
      if (nome) marcador.bindPopup(nome);
      this.marcadores.set(id, marcador);
    }
  }

  ngOnDestroy() {
    this.sub?.unsubscribe();
    this.map?.remove();
  }
}