package security;

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

import model.Role;
import model.Usuario;
import repository.UsuarioRepository;

@Component
public class AuthInterceptor implements HandlerInterceptor {

    private final UsuarioRepository usuarioRepository;

    public AuthInterceptor(UsuarioRepository usuarioRepository) {
        this.usuarioRepository = usuarioRepository;
    }

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws IOException {

        if (!(handler instanceof HandlerMethod)) {
            return true;
        }

        HandlerMethod hm = (HandlerMethod) handler;

        Secured secured = hm.getMethodAnnotation(Secured.class);

        if (secured == null) {
            secured = hm.getBeanType().getAnnotation(Secured.class);
        }

        if (secured == null) {
            return true;
        }

        String header = request.getHeader("Authorization");

        if (header == null || !header.startsWith("Bearer ")) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                    "Token ausente");
            return false;
        }

        String token = header.substring(7);

        Long usuarioId = GeradorToken.validar(token);

        if (usuarioId == null) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                    "Token inválido ou expirado");
            return false;
        }

        Usuario usuario = usuarioRepository.findById(usuarioId)
        		.orElseThrow(() -> new RuntimeException("Pet não encontrado"));

        
        Role[] rolesPermitidas = secured.value();

        if (rolesPermitidas.length > 0) {

            boolean permitido = false;

            for (Role role : rolesPermitidas) {
                if (role == usuario.getRole()) {
                    permitido = true;
                    break;
                }
            }

            if (!permitido) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN,
                        "Sem permissão");
                return false;
            }
        }

        request.setAttribute("usuarioLogado", usuario);

        return true;
    }
}