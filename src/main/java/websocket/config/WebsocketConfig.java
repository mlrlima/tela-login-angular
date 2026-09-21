package websocket.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker //Spring cria:
// servidor WebSocket, broker, suporte ao STOMP
public class WebsocketConfig implements WebSocketMessageBrokerConfigurer {

    @Autowired
    private JwtHandshakeInterceptor jwtHandshakeInterceptor;

    @Autowired
    private JwtStompChannelInterceptor jwtStompChannelInterceptor;

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) { //recebe mensagens e envia para quem estiver inscrito
        // broker em memoria (RAM do servidor) para os clientes se inscreverem
        // canal publico para broadcasts gerais: /topic
        // canal privado para mensagens do usuario autenticado: /user
        config.enableSimpleBroker("/topic", "/queue");
        config.setUserDestinationPrefix("/user");
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // O endpoint que os clientes conectam inicialmente
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns( // CORS, apenas permite acessos especificos 
                		"https://localhost:4200", "http://localhost:4200" /*, "https://*.seu-dominio.com"*/
                		)
                .addInterceptors(jwtHandshakeInterceptor)
                .withSockJS();
        //SockJS é uma biblioteca que simula WebSocket quando o navegador ou a rede não suportam WebSocket nativamente.
        // web server <-> web browser
        // Em producao, manter apenas origens autorizadas e usar HTTPS.
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(jwtStompChannelInterceptor);
    }
	
}
