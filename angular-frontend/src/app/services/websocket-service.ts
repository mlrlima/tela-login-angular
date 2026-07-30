/*
import { Service } from '@angular/core';
import {CompatClient, Stomp} from '@stomp/stompjs';
import {StompSubscription} from '@stomp/stompjs/src/stomp-subscription';

export type ListenerCallBack = (message: String) => void;

@Injectable({ providedIn:'root'})
export class WebsocketService implements OnDestroy {
	
	private connection: CompatClient | undefined = undefined;
	
	private subscription: StompSubscription | undefined;
	
	constructor() {
	  this.connection = Stomp.client(`${environment.apiUrl}/websocket`);
	  this.connection.connect({}, () => {});
	}
	
	public send(str: String): void {
	  if (this.connection && this.connection.connected) {
	    this.connection.send(`/teste-dois/add_string`, {}, "hello world!!!!!!!!!!");
	  }
	}
	
	public listen(fun: ListenerCallBack): void {
	  if (this.connection) {
	    this.connection.connect({}, () => {
	      this.subscription = this.connection!.subscribe(`/teste/added_string`, message => fun(JSON.parse(message.body)));
	    }); 
	  }
	}
	
	ngOnDestroy(): void {
	  if (this.subscription) {
	    this.subscription.unsubscribe();
	  }
	}
	
}
*/