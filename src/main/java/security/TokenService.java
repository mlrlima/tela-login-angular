package security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.auth0.jwt.exceptions.JWTVerificationException;

import model.Usuario;

@Service
public class TokenService {
	
	@Value("${api.security.token.secret}")
	private String secret;

	public String gerarToken(Usuario usuario) {
		try {
			Algorithm algorithm= Algorithm.HMAC256(secret); //gera algoritmo com senha(secret) unica
			String token =JWT.create()
					.withIssuer("tela-login-angular")
					.withSubject(usuario.getEmail())
					.sign(algorithm);
			return token;
					
		}catch(JWTCreationException ex) {
			throw new RuntimeException("Erro ao gerar token", ex);
		}
	}
	
	public String validarToken(String token) {
		try {
			Algorithm algorithm =Algorithm.HMAC256(secret);
			return JWT.require(algorithm)
					.withIssuer("tela-login-angular")
					.build()
					.verify(token)
					.getSubject();
					
		}catch(JWTVerificationException ex) {
			throw new RuntimeException("Nao foi possivel validar token", ex);
		}
	}
}
