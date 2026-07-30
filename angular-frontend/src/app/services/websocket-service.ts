import { Injectable } from '@angular/core';
import { Client } from '@stomp/stompjs';
import * as SockJS from 'sockjs-client';
import { BehaviorSubject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class WebsocketService {
  private stompClient!: Client;
  public message$ = new BehaviorSubject<string>('');

  constructor() {
    this.initializeWebSocketConnection();
  }

  private initializeWebSocketConnection() {
    // Generate native path using current window context location 
    const isSecure = window.location.protocol === 'https:';
    const baseHttpProtocol = isSecure ? 'https://' : 'http://';
    const serverUrl = `${baseHttpProtocol}${window.location.host}/ws`;

    this.stompClient = new Client({
      webSocketFactory: () => new SockJS(serverUrl),
      debug: (str) => console.log(str),
      reconnectDelay: 5000,
      heartbeatIncoming: 4000,
      heartbeatOutgoing: 4000,
    });

    this.stompClient.onConnect = (frame) => {
      console.log('Connected: ' + frame);
      
      // Subscribe to broker channel
      this.stompClient.subscribe('/topic/messages', (message) => {
        if (message.body) {
          this.message$.next(message.body);
        }
      });
    };

    this.stompClient.activate();
  }

  public sendMessage(msg: string) {
    this.stompClient.publish({
      destination: '/app/send-message',
      body: msg
    });
  }
}
