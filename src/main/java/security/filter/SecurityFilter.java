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

		if(token!=null && tokenService.tokenEstaValido(token)) {
			var email= tokenService.validarToken(token);
			UserDetails usuario =usuarioRepository.findByEmail(email);
			
			var autenticacao=new UsernamePasswordAuthenticationToken(usuario, null, usuario.getAuthorities());
			SecurityContextHolder.getContext().setAuthentication(autenticacao);
		}
		
		// passa pro proximo filtro
		filterChain.doFilter(request, response);
		
		
	}
	
	private String recoverToken(HttpServletRequest request) {
		var authHeader= request.getHeader("Authorization");
		
		if(authHeader==null) return null;
		
		//deixar apenas o token
		return authHeader.replace("Bearer ", "");
	}
	
}
