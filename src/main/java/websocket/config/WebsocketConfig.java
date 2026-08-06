package websocket.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker //Spring cria:
// servidor WebSocket, broker, suporte ao STOMP
public class WebsocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) { //recebe mensagens e envia para quem estiver inscrito
        // Enforce a simple memory-based message broker for clients to subscribe to
        config.enableSimpleBroker("/topic"); //envia para os clientes
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // The endpoint clients connect to initially
        registry.addEndpoint("/ws").withSockJS();
        //SockJS é uma biblioteca que simula WebSocket quando o navegador ou a rede não suportam WebSocket nativamente.
    }
	
}
