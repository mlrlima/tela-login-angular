package websocket.config;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

	//evita mensagens muito grandes
    private static final int MAX_MESSAGE_SIZE_BYTES = 16_384; // 16 KB
    
    // Uma conexão pode enviar no máximo 20 mensagens a cada 10 segundos.
    private static final int MAX_MESSAGES_PER_WINDOW = 20;
    private static final long RATE_LIMIT_WINDOW_MS = 10_000L;
    
    // Se uma mesma mensagem for repetida em menos de 2 segundos, ela será rejeitada
    private static final long DUPLICATE_WINDOW_MS = 2_000L;

    // Deque<Long> -> uma fila de números, representam horários em milissegundos, em que as mensagens foram recebidas
    private final Map<String, Deque<Long>> rateLimitBySession = new ConcurrentHashMap<>();
    private final Map<String, Long> lastMessageTimeBySession = new ConcurrentHashMap<>();
    private final Map<String, String> lastMessageFingerprintBySession = new ConcurrentHashMap<>();

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
            return message;
        }

        //se for subscribe ou send, precisa ser um usuario autenticado e com papel permitido
        if (accessor.getCommand() == StompCommand.SUBSCRIBE || accessor.getCommand() == StompCommand.SEND) {
            String destination = accessor.getDestination();
            if (destination == null || destination.isBlank()) {
                throw new AccessDeniedException("Destino STOMP invalido");
            }

            if (destination != null && isProtectedDestination(destination)) {
                if (accessor.getUser() == null) {
                    throw new AccessDeniedException("Usuario nao autenticado para assinar ou enviar no destino protegido");
                }

                if (!hasAuthorizedRole(accessor.getUser())) {
                    throw new AccessDeniedException("Usuario sem papel autorizado para o destino protegido");
                }
            }

            String sessionId = accessor.getSessionId();
            if (sessionId == null || sessionId.isBlank()) {
                throw new AccessDeniedException("Sessao WebSocket invalida");
            }

            validatePayloadSizeAndAbuse(sessionId, destination, message.getPayload());
        }

        return message;
    }

    private void validatePayloadSizeAndAbuse(String sessionId, String destination, Object payload) {
        if (payload == null) {
            throw new AccessDeniedException("Mensagem WebSocket vazia");
        }

        // Transforma o payload em String.
        String payloadText = normalizePayload(payload);
        if (payloadText.isBlank()) {
            throw new AccessDeniedException("Mensagem WebSocket invalida");
        }

        
        if (payloadText.getBytes(StandardCharsets.UTF_8).length > MAX_MESSAGE_SIZE_BYTES) {
            throw new AccessDeniedException("Mensagem WebSocket excedeu o limite permitido");
        }

        enforceRateLimit(sessionId);
        rejectDuplicateMessage(sessionId, destination, payloadText);
    }

    private String normalizePayload(Object payload) {
    	// Se o payload for bytes, transforma para texto usando UTF-8.
        if (payload instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(payload).trim();
    }

    private void enforceRateLimit(String sessionId) {
    	
    	// Se ainda não existe uma fila para aquela sessão, cria.
        Deque<Long> timestamps = rateLimitBySession.computeIfAbsent(sessionId, key -> new ArrayDeque<>());

        // evita que duas threads mexam simultaneamente na mesma fila e causem inconsistências.
        synchronized (timestamps) {
            long now = System.currentTimeMillis();
            timestamps.addLast(now);

            // remove timestamps de mais de 10 segundos atras
            while (!timestamps.isEmpty() && now - timestamps.peekFirst() > RATE_LIMIT_WINDOW_MS) {
                timestamps.pollFirst();
            }

            if (timestamps.size() > MAX_MESSAGES_PER_WINDOW) {
                throw new AccessDeniedException("Limite de mensagens excedido para esta conexao WebSocket");
            }
        }
    }

    private void rejectDuplicateMessage(String sessionId, String destination, String payloadText) {
        String fingerprint = destination + ":" + payloadText;
        String key = sessionId + ":" + destination;
        long now = System.currentTimeMillis();
        Long lastTime = lastMessageTimeBySession.get(key);
        String lastFingerprint = lastMessageFingerprintBySession.get(key);

        if (lastTime != null && lastFingerprint != null && lastFingerprint.equals(fingerprint)
                && now - lastTime < DUPLICATE_WINDOW_MS) {
            throw new AccessDeniedException("Mensagem duplicada rejeitada para esta conexao WebSocket");
        }

        lastMessageFingerprintBySession.put(key, fingerprint);
        lastMessageTimeBySession.put(key, now);
    }

    private boolean isProtectedDestination(String destination) {
        return "/topic/pet-location-lote".equals(destination)
                || destination.startsWith("/topic/pet-location-lote/")
                || destination.startsWith("/user/");
    }

    // apenas permite usuarios com role
    private boolean hasAuthorizedRole(java.security.Principal principal) {
        if (principal == null) {
            return false;
        }

        if (principal instanceof org.springframework.security.core.Authentication authentication) {
            return authentication.getAuthorities().stream()
                    .anyMatch(authority -> "ROLE_ADMIN".equals(authority.getAuthority())
                            || "ROLE_USER".equals(authority.getAuthority()));
        }

        return false;
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
