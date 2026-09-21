package websocket.config;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import repository.UsuarioRepository;
import security.service.TokenService;

@Component
public class JwtHandshakeInterceptor implements HandshakeInterceptor {

    @Autowired
    private TokenService tokenService;

    @Autowired
    private UsuarioRepository usuarioRepository;

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
            WebSocketHandler wsHandler, Map<String, Object> attributes) {

        String token = extractToken(request);
        if (token == null || token.isBlank()) {
        	//se token nao existe ou esta vazio, proibe a conexao
            return false;
        }

        try {
            if (!tokenService.tokenEstaValido(token)) {
                return false;
            }

            String email = tokenService.validarToken(token);
            UserDetails usuario = usuarioRepository.findByEmail(email);
            if (usuario == null) {
                return false;
            }

            //colocando autenticacao do usuario nos atributos do handshake
            attributes.put("user", new UsernamePasswordAuthenticationToken(usuario, null, usuario.getAuthorities()));
            return true;
        } catch (RuntimeException ex) {
            return false;
        }
    }

    //implementação obrigatória da interface.
    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
            WebSocketHandler wsHandler, Exception exception) {
        // sem ação adicional
    }

    private String extractToken(ServerHttpRequest request) {
    	
    	// se nao consegue acessar HttpServletRequest, token eh nulo
        if (!(request instanceof ServletServerHttpRequest servletRequest)) {
            return null;
        }

        HttpServletRequest httpRequest = servletRequest.getServletRequest();

        if (httpRequest.getCookies() != null) {
            for (Cookie cookie : httpRequest.getCookies()) {
                if ("token".equals(cookie.getName())) {
                    return cookie.getValue();
                }
            }
        }

        String authorization = httpRequest.getHeader("Authorization");
        if (authorization != null && authorization.startsWith("Bearer ")) {
            return authorization.substring(7).trim();
        }

        return null;
    }
}
