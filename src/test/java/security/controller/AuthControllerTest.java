package security.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import jakarta.servlet.http.HttpServletResponse;
import model.Role;
import model.Usuario;
import repository.UsuarioRepository;
import security.DTO.AuthDTO;
import security.DTO.LoginResponseDTO;
import security.DTO.NovoUsuarioDTO;
import security.service.TokenService;

@ExtendWith(MockitoExtension.class)
class AuthControllerTest {

	@Mock
	private AuthenticationManager authenticationManager;

	@Mock
	private UsuarioRepository repository;

	@Mock
	private TokenService tokenService;

	@InjectMocks
	private AuthController authController;

	@Mock
	private HttpServletResponse response;

	@Mock
	private Authentication authentication;

	private Usuario usuario;

	@BeforeEach
	void setUp() {
		usuario = new Usuario();
		usuario.setId(1L);
		usuario.setNome("Mock");
		usuario.setEmail("mock@teste.com");
		usuario.setRole(Role.USER);
	}


	// ResponseEntity<Void> logout(HttpServletResponse response)=============================

	@Test
	void logout_limpa_o_cookie_de_token_e_retorna_200() {
		//act
		ResponseEntity<Void> resultado = authController.logout(response);

		//assert
		assertEquals(HttpStatus.OK, resultado.getStatusCode());

		ArgumentCaptor<String> cookieCaptor = ArgumentCaptor.forClass(String.class);
		verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieCaptor.capture());

		String cookie = cookieCaptor.getValue();
		assertTrue(cookie.contains("token=")); // cookie com nome "token"
		assertTrue(cookie.contains("Max-Age=0")); // expira imediatamente, ou seja, limpa
	}


	// ResponseEntity login(AuthDTO dados, HttpServletResponse response)=====================

	@Test
	void login_com_credenciais_corretas_seta_cookie_e_retorna_dados_do_usuario() {
		//arrange
		AuthDTO dados = new AuthDTO("mock@teste.com", "1234");

		when(authenticationManager.authenticate(any())).thenReturn(authentication);
		when(authentication.getPrincipal()).thenReturn(usuario);
		when(tokenService.gerarToken(usuario)).thenReturn("jwt-fake-token");

		//act
		ResponseEntity<?> resultado = authController.login(dados, response);

		//assert
		assertEquals(HttpStatus.OK, resultado.getStatusCode());

		LoginResponseDTO corpo = (LoginResponseDTO) resultado.getBody();
		assertEquals(usuario.getId(), corpo.id());
		assertEquals(usuario.getNome(), corpo.nome());
		assertEquals(usuario.getEmail(), corpo.email());
		assertEquals(usuario.getRole(), corpo.role());

		ArgumentCaptor<String> cookieCaptor = ArgumentCaptor.forClass(String.class);
		verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieCaptor.capture());
		assertTrue(cookieCaptor.getValue().contains("token=jwt-fake-token"));
	}

	@Test
	void login_com_credenciais_erradas_propaga_excecao_e_nao_seta_cookie() {
		//arrange
		AuthDTO dados = new AuthDTO("mock@teste.com", "senhaErrada");

		when(authenticationManager.authenticate(any())).thenThrow(new BadCredentialsException("Credenciais inválidas"));

		//act & assert
		assertThrows(BadCredentialsException.class,
				() -> authController.login(dados, response));

		verify(response, never()).addHeader(eq(HttpHeaders.SET_COOKIE), any());
		verify(tokenService, never()).gerarToken(any());
	}


	// ResponseEntity novoUsuario(NovoUsuarioDTO dados)======================================

	@Test
	void novoUsuario_com_email_ja_cadastrado_retorna_badRequest() {
		//arrange
		NovoUsuarioDTO dados = new NovoUsuarioDTO("Teste", "mock@teste.com", "senha123");
		when(repository.findByEmail("mock@teste.com")).thenReturn(usuario);

		//act
		ResponseEntity<?> resultado = authController.novoUsuario(dados);

		//assert
		assertEquals(HttpStatus.BAD_REQUEST, resultado.getStatusCode());
		verify(repository, never()).save(any());
	}

	@Test
	void novoUsuario_com_senha_muito_curta_retorna_badRequest_com_mensagem() {
		//arrange
		NovoUsuarioDTO dados = new NovoUsuarioDTO("Nova", "nova@teste.com", "123");
		when(repository.findByEmail("nova@teste.com")).thenReturn(null);

		//act
		ResponseEntity<?> resultado = authController.novoUsuario(dados);

		//assert
		assertEquals(HttpStatus.BAD_REQUEST, resultado.getStatusCode());
		verify(repository, never()).save(any());
	}

	@Test
	void novoUsuario_valido_salva_com_senha_criptografada_e_role_USER() {
		//arrange
		NovoUsuarioDTO dados = new NovoUsuarioDTO("Nova Pessoa", "nova@teste.com", "senha123");
		when(repository.findByEmail("nova@teste.com")).thenReturn(null);

		//act
		ResponseEntity<?> resultado = authController.novoUsuario(dados);

		//assert
		assertEquals(HttpStatus.OK, resultado.getStatusCode());

		ArgumentCaptor<Usuario> usuarioCaptor = ArgumentCaptor.forClass(Usuario.class);
		verify(repository).save(usuarioCaptor.capture());

		Usuario salvo = usuarioCaptor.getValue();
		assertNull(salvo.getId());
		assertEquals("Nova Pessoa", salvo.getNome());
		assertEquals("nova@teste.com", salvo.getEmail());
		assertEquals(Role.USER, salvo.getRole());
		assertNotEquals("senha123", salvo.getSenha()); // nao pode salvar em texto puro
		assertTrue(new BCryptPasswordEncoder().matches("senha123", salvo.getSenha()));
	}
}