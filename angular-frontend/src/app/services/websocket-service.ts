import { Injectable } from '@angular/core';
import { Client } from '@stomp/stompjs';
import SockJS from 'sockjs-client';
import { BehaviorSubject } from 'rxjs'; //biblioteca para codigo assincrono e event-based
import { environment } from '../../environments/environment';

export interface PetLocation {
  id: number;
  latitude: number;
  longitude: number;
  nome: string;
  especie: string;
}

@Injectable({ providedIn: 'root' }) //websocket inicia sozinho
export class WebsocketService {
  private stompClient!: Client;
  
  // objeto do RxJS que guarda um valor atual e avisa todos os inscritos quando esse valor muda
  public message$ = new BehaviorSubject<string>('');
  public petLocationBatch$ = new BehaviorSubject<PetLocation[]>([]);

  constructor() {
    this.initializeWebSocketConnection();
  }

  private initializeWebSocketConnection() {
	
	// http://localhost:8080/tela-login-angular/ws
    const serverUrl = `${environment.apiUrl}/ws`;

    this.stompClient = new Client({
      webSocketFactory: () => new SockJS(serverUrl), //abre uma conexao
      reconnectDelay: 5000, // se cair, 5s para tentar novamente
      heartbeatIncoming: 4000, //a cada 4s espera informacoes do servidor
      heartbeatOutgoing: 4000, //a cada 4s envia infos ao servidor
    });

    this.stompClient.onConnect = () => { //cliente conectado ao servidor
      this.stompClient.subscribe('/topic/messages', (message) => { // ouve por aqui
        if (message.body) this.message$.next(message.body);
      });

	  //recebe localizacao do pet
      this.stompClient.subscribe('/topic/pet-location-lote', (message) => { // ouve por aqui
        if (message.body) { //transforma a message em json
          this.petLocationBatch$.next(JSON.parse(message.body) as PetLocation[]);
        }
      });
    };

    this.stompClient.activate();
  }

  public sendMessage(msg: string) {
    this.stompClient.publish({ destination: '/app/send-message', body: msg });
  }
}