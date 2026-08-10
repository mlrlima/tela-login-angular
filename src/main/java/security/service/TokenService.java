package security.service;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.auth0.jwt.exceptions.JWTVerificationException;

import model.Usuario;

@Service
public class TokenService {
	
	// para tokens que foram invalidados
	//ConcurrentHashMap para atender várias requisições simultaneamente.
	private final Set<String> tokenBlacklist = ConcurrentHashMap.newKeySet();
	
	@Value("${api.security.token.secret}") //pega do application.properties
	private String secret;

	public String gerarToken(Usuario usuario) {
		try {
			Algorithm algorithm= Algorithm.HMAC256(secret); //gera algoritmo com senha(secret) unica
			
			Date data=new Date();
			String token =JWT.create()
					.withIssuer("tela-login-angular") // quem emitiu o token
					.withSubject(usuario.getEmail())
					.withIssuedAt(data)
					.withExpiresAt(new Date(data.getTime()+ (60000*30))) //30 minutos
					.sign(algorithm);
			return token;
					
		}catch(JWTCreationException ex) {
			throw new RuntimeException("Erro ao gerar token", ex);
		}
	}
	
	public String validarToken(String token) { //retorna o email do usuario
		try {
			Algorithm algorithm =Algorithm.HMAC256(secret);
			return JWT.require(algorithm)
					.withIssuer("tela-login-angular")
					.build() //Cria o objeto que será usado para validar.
					.verify(token)
					.getSubject();
					
		}catch(JWTVerificationException ex) {
			throw new RuntimeException("Nao foi possivel validar token", ex);
		}
	}
	
    public void invalidarToken(String token) {
        tokenBlacklist.add(token);
    }

    public boolean tokenEstaValido(String token) {
        return !tokenBlacklist.contains(token);
    }
}
