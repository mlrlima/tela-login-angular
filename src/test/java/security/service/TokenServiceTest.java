package security.service;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Date;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;

import model.Role;
import model.Usuario;

@ExtendWith(MockitoExtension.class)
class TokenServiceTest {

	private static final String SECRET = "segredo-de-teste-123";

	private TokenService tokenService;
	private Usuario usuario;

	@BeforeEach
	void setUp() {
		tokenService = new TokenService();
		// o secret normalmente vem do application.properties via @Value,
		// entao aqui seta manualmente pra poder testar isolado
		ReflectionTestUtils.setField(tokenService, "secret", SECRET);

		usuario = new Usuario();
		usuario.setId(1L);
		usuario.setEmail("user@teste.com");
		usuario.setRole(Role.USER);
	}

	// String gerarToken(Usuario usuario)====================================================

	@Test
	void gerarToken_cria_token_que_valida_para_o_email_do_usuario() {
		//act
		String token = tokenService.gerarToken(usuario);
		String emailValidado = tokenService.validarToken(token);

		//assert
		assertNotNull(token);
		assertEquals(usuario.getEmail(), emailValidado);
	}

	@Test
	void gerarToken_cria_tokens_diferentes_para_usuarios_diferentes() {
		//arrange
		Usuario outroUsuario = new Usuario();
		outroUsuario.setId(2L);
		outroUsuario.setEmail("outro@teste.com");
		outroUsuario.setRole(Role.USER);
 
		//act
		String token1 = tokenService.gerarToken(usuario);
		String token2 = tokenService.gerarToken(outroUsuario);
 
		//assert
		assertNotEquals(token1, token2);
	}

	// String validarToken(String token)=====================================================

	@Test
	void validarToken_com_token_mal_formado_lanca_excecao() {
		//act & assert
		assertThrows(RuntimeException.class,
				() -> tokenService.validarToken("isso-nao-eh-um-jwt"));
	}

	@Test
	void validarToken_com_assinatura_diferente_lanca_excecao() {
		//arrange: token assinado com um secret diferente do configurado no service
		Algorithm outroAlgoritmo = Algorithm.HMAC256("outro-secret-completamente-diferente");
		String tokenAdulterado = JWT.create()
				.withIssuer("tela-login-angular")
				.withSubject(usuario.getEmail())
				.withIssuedAt(new Date())
				.withExpiresAt(new Date(System.currentTimeMillis() + 60000))
				.sign(outroAlgoritmo);

		//act & assert
		assertThrows(RuntimeException.class,
				() -> tokenService.validarToken(tokenAdulterado));
	}

	@Test
	void validarToken_com_issuer_diferente_lanca_excecao() {
		//arrange: token assinado com o secret certo, mas issuer diferente
		Algorithm algorithm = Algorithm.HMAC256(SECRET);
		String tokenComIssuerErrado = JWT.create()
				.withIssuer("outro-sistema-qualquer")
				.withSubject(usuario.getEmail())
				.withIssuedAt(new Date())
				.withExpiresAt(new Date(System.currentTimeMillis() + 60000))
				.sign(algorithm);

		//act & assert
		assertThrows(RuntimeException.class,
				() -> tokenService.validarToken(tokenComIssuerErrado));
	}

	@Test
	void validarToken_com_token_expirado_lanca_excecao() {
		//arrange: token ja expirado (expiresAt no passado)
		Algorithm algorithm = Algorithm.HMAC256(SECRET);
		String tokenExpirado = JWT.create()
				.withIssuer("tela-login-angular")
				.withSubject(usuario.getEmail())
				.withIssuedAt(new Date(System.currentTimeMillis() - 120000))
				.withExpiresAt(new Date(System.currentTimeMillis() - 60000))
				.sign(algorithm);

		//act & assert
		assertThrows(RuntimeException.class,
				() -> tokenService.validarToken(tokenExpirado));
	}


	// void invalidarToken(String token) 
	// boolean tokenEstaValido(String token) ================================================

	@Test
	void token_recem_gerado_esta_valido_por_padrao() {
		//arrange
		String token = tokenService.gerarToken(usuario);

		//act & assert
		assertTrue(tokenService.tokenEstaValido(token));
	}

	@Test
	void invalidarToken_faz_tokenEstaValido_retornar_false() {
		//arrange
		String token = tokenService.gerarToken(usuario);
		assertTrue(tokenService.tokenEstaValido(token)); // garante que era valido antes

		//act
		tokenService.invalidarToken(token);

		//assert
		assertFalse(tokenService.tokenEstaValido(token));
	}

	@Test
	void invalidarToken_nao_afeta_outros_tokens() {
		//arrange
		
		// usa usuarios diferentes pra garantir tokens diferentes,
		// independente do timing (os claims de tempo sao truncados pra segundos,
		// entao dois tokens do MESMO usuario gerados no mesmo segundo ficam identicos)
		Usuario outroUsuario = new Usuario();
		outroUsuario.setId(2L);
		outroUsuario.setEmail("outro@teste.com");
		outroUsuario.setRole(Role.USER);
 
		String token1 = tokenService.gerarToken(usuario);
		String token2 = tokenService.gerarToken(outroUsuario);
		assertNotEquals(token1, token2); // pre-condicao: tem que ser tokens diferentes
 
		//act
		tokenService.invalidarToken(token1);
 
		//assert
		assertFalse(tokenService.tokenEstaValido(token1));
		assertTrue(tokenService.tokenEstaValido(token2));
	}
}