import { Component, OnInit, OnDestroy, AfterViewInit } from '@angular/core';
import * as L from 'leaflet';
import { Subscription } from 'rxjs';
import { PetService, Pet } from '../services/pet';
import { WebsocketService } from '../services/websocket-service';

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
  selector: 'app-mapa-pets',
  standalone: true,
  imports: [],
  templateUrl: './mapa-pets.html',
  styleUrl: './mapa-pets.css',
})
export class MapaPets implements OnInit, AfterViewInit, OnDestroy {
  private map!: L.Map;
  private marcadores = new Map<number, L.Marker>(); // id do pet - marcador
  private sub?: Subscription;

  constructor(private petService: PetService, private ws: WebsocketService) {}

  ngOnInit() {
    this.sub = this.ws.petLocation$.subscribe((loc) => { //ouve o BehaviourSubject
		
		//se chegar uma localizacao nova
      if (loc) this.atualizarMarcador(loc.id, loc.latitude, loc.longitude);
    });
  }

  ngAfterViewInit() { // depois do html ja renderizado
	
	//cria o mapa
	// centro inicial = Recife
	//zoom = 13
    this.map = L.map('mapa-pets').setView([-8.0476, -34.877], 13);

	// mapa do OpenStreetMap
	// onde baixar os tiles do mapa
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
    }).addTo(this.map);

    this.petService.listar(0, 100).subscribe((pagina) => {
      const pets: Pet[] = pagina.content ?? pagina;
      pets.forEach((pet) => {
        if (pet.id != null && pet.latitude != null && pet.longitude != null) {
          this.atualizarMarcador(pet.id, pet.latitude, pet.longitude, pet.nome, pet.especie);
        }
      });
    });
  }

  private atualizarMarcador(id: number, lat: number, lng: number, nome?: string, especie?: string) {
    const existente = this.marcadores.get(id);
    if (existente) { //se o marcador ja existe, muda a posicao
      existente.setLatLng([lat, lng]);
    } else { // se nao existe, cria um novo
	 
		const icone = this.qualIcon(especie);
		
      const marcador = L.marker([lat, lng], {icon: icone}).addTo(this.map);
      if (nome) marcador.bindPopup(nome); // quando clica aparece o nome do pet
      this.marcadores.set(id, marcador);
    }
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

  ngOnDestroy() {
	// Cancela a inscrição no BehaviorSubject
    this.sub?.unsubscribe();
	
	// Remove o mapa e libera os recursos do Leaflet
    this.map?.remove();
  }
}