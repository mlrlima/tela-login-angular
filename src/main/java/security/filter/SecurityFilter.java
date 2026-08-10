package security.filter;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import repository.UsuarioRepository;
import security.service.TokenService;

@Component
public class SecurityFilter extends OncePerRequestFilter{

	@Autowired
	TokenService tokenService;
	
	@Autowired
	UsuarioRepository usuarioRepository;
	
	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
			throws ServletException, IOException {
		
		var token=this.recoverToken(request);

		if(token!=null && tokenService.tokenEstaValido(token)) { //verifica se o token nao eh nulo nem foi invalidado
			
			//valida a autenticidade do toke, retornando o email do usuario
			var email= tokenService.validarToken(token);
			UserDetails usuario =usuarioRepository.findByEmail(email);
			if(usuario!=null) {
				// principal, credenciais (senha) (nulo, para nao expor), autoridades (ADMIN, USER)
				var autenticacao=new UsernamePasswordAuthenticationToken(usuario, null, usuario.getAuthorities());
				SecurityContextHolder.getContext().setAuthentication(autenticacao);
			}
		}
		
		// passa pro proximo filtro
		filterChain.doFilter(request, response);
		
		
	}
	
	private String recoverToken(HttpServletRequest request) {
		if(request.getCookies() == null) return null;
		
		for (Cookie cookie : request.getCookies()) {
			if ("token".equals(cookie.getName())) {
				return cookie.getValue();
			}
		}
		return null;
	}
	
}
