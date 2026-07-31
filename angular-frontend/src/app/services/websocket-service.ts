import { Injectable } from '@angular/core';
import { Client } from '@stomp/stompjs';
import SockJS from 'sockjs-client';
import { BehaviorSubject } from 'rxjs';
import { environment } from '../../environments/environment';

export interface PetLocation {
  id: number;
  latitude: number;
  longitude: number;
}

@Injectable({ providedIn: 'root' })
export class WebsocketService {
  private stompClient!: Client;
  public message$ = new BehaviorSubject<string>('');
  public petLocation$ = new BehaviorSubject<PetLocation | null>(null);

  constructor() {
    this.initializeWebSocketConnection();
  }

  private initializeWebSocketConnection() {
    const serverUrl = `${environment.apiUrl}/ws`;

    this.stompClient = new Client({
      webSocketFactory: () => new SockJS(serverUrl),
      reconnectDelay: 5000,
      heartbeatIncoming: 4000,
      heartbeatOutgoing: 4000,
    });

    this.stompClient.onConnect = () => {
      this.stompClient.subscribe('/topic/messages', (message) => {
        if (message.body) this.message$.next(message.body);
      });

      this.stompClient.subscribe('/topic/pet-location', (message) => {
        if (message.body) {
          this.petLocation$.next(JSON.parse(message.body) as PetLocation);
        }
      });
    };

    this.stompClient.activate();
  }

  public sendMessage(msg: string) {
    this.stompClient.publish({ destination: '/app/send-message', body: msg });
  }
}