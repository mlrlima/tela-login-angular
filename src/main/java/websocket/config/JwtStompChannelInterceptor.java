package websocket.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import repository.UsuarioRepository;
import security.service.TokenService;

@Component
// ChannelInterceptor consegue interceptar mensagens
// que passam pelos canais de mensagens do Spring Messaging.
// nesse caso, mensagens STOMP
public class JwtStompChannelInterceptor implements ChannelInterceptor {
	// protege as mensagens STOMP que passam pelo WebSocket, principalmente o comando CONNECT.

    @Autowired
    private TokenService tokenService;

    @Autowired
    private UsuarioRepository usuarioRepository;

    @Override
    //executado antes da mensagem ser enviada pelo canal.
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
    	
    	// cria um objeto que facilita acessar informações específicas do STOMP
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);

        // so executa se o comando for CONNECT
        if (accessor.getCommand() == StompCommand.CONNECT) {
            String token = resolveToken(accessor);

            if (token == null || token.isBlank()) {
                throw new AccessDeniedException("Token de autenticacao ausente para WebSocket STOMP");
            }

            if (!tokenService.tokenEstaValido(token)) {
                throw new AccessDeniedException("Token de autenticacao invalido para WebSocket STOMP");
            }

            String email = tokenService.validarToken(token);
            UserDetails usuario = usuarioRepository.findByEmail(email);
            if (usuario == null) {
                throw new AccessDeniedException("Usuario nao encontrado para o token do WebSocket");
            }

            // coloca o usuário autenticado na mensagem STOMP, para saber quem enviou
            accessor.setUser(new UsernamePasswordAuthenticationToken(usuario, null, usuario.getAuthorities()));
        }

        return message;
    }

    private String resolveToken(StompHeaderAccessor accessor) {
    	// ve se a autenticacao estar em Bearer, token ou cookie
    	
        String authorization = accessor.getFirstNativeHeader("Authorization");
        if (authorization != null && authorization.startsWith("Bearer ")) {
            return authorization.substring(7).trim();
        }

        String token = accessor.getFirstNativeHeader("token");
        if (token != null && !token.isBlank()) {
            return token;
        }

        String cookieHeader = accessor.getFirstNativeHeader("Cookie");
        if (cookieHeader != null && !cookieHeader.isBlank()) {
            for (String part : cookieHeader.split(";")) {
                String[] pair = part.trim().split("=", 2);
                if (pair.length == 2 && "token".equals(pair[0].trim())) {
                    return pair[1].trim();
                }
            }
        }

        return null;
    }
}
