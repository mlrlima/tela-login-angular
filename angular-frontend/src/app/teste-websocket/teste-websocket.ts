/*
import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { WebsocketService } from '../services/websocket.service';

@Component({
  selector: 'app-teste-websocket',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './teste-websocket.html',
  styleUrl: './teste-websocket.css',
}) 
export class TesteWebsocket {
	mensagens: string[] = [];

	constructor(private websocketService: WebsocketService,) {}

	conectar() {
	  this.websocketService.listen((msg: String) => {
	    console.log('Mensagem recebida:', msg);
	    this.mensagens.push(msg.toString());
	  });
	}

	enviar() {
	  this.websocketService.send("hello world");
	}
}
*/

