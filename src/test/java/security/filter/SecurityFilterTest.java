package security.filter;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import model.Role;
import model.Usuario;
import repository.UsuarioRepository;
import security.service.TokenService;

@ExtendWith(MockitoExtension.class)
class SecurityFilterTest {

	// nao usa @InjectMocks aqui porque precisa controlar exatamente
	// quais dependencias o filtro recebe
	private SecurityFilter securityFilter;

	private TokenService tokenService;
	private UsuarioRepository usuarioRepository;

	@Mock
	private HttpServletRequest request;

	@Mock
	private HttpServletResponse response;

	@Mock
	private FilterChain filterChain;

	@Mock
	private SecurityContext securityContext;

	private MockedStatic<SecurityContextHolder> securityContextHolderMock;

	private Usuario usuario;

	@BeforeEach
	void setUp() {
		tokenService = mock(TokenService.class);
		usuarioRepository = mock(UsuarioRepository.class);

		securityFilter = new SecurityFilter();
		// os campos sao @Autowired via injecao de campo, entao seta na mao
		ReflectionTestUtils.setField(securityFilter, "tokenService", tokenService);
		ReflectionTestUtils.setField(securityFilter, "usuarioRepository", usuarioRepository);

		usuario = new Usuario();
		usuario.setId(1L);
		usuario.setEmail("user@teste.com");
		usuario.setRole(Role.USER);

		securityContextHolderMock = mockStatic(SecurityContextHolder.class);
		securityContextHolderMock.when(SecurityContextHolder::getContext).thenReturn(securityContext);
	}

	@AfterEach
	void tearDown() {
		securityContextHolderMock.close();
	}

	@Test
	void sem_cookies_apenas_continua_o_filtro_sem_autenticar() throws Exception {
		//arrange
		when(request.getCookies()).thenReturn(null);

		//act
		securityFilter.doFilterInternal(request, response, filterChain);

		//assert
		verify(filterChain).doFilter(request, response);
		verify(securityContext, never()).setAuthentication(any());
		verifyNoInteractions(tokenService);
	}

	@Test
	void cookie_sem_o_nome_token_apenas_continua_o_filtro_sem_autenticar() throws Exception {
		//arrange
		Cookie cookieQualquer = new Cookie("outroNome", "algumValor");
		when(request.getCookies()).thenReturn(new Cookie[] { cookieQualquer });

		//act
		securityFilter.doFilterInternal(request, response, filterChain);

		//assert
		verify(filterChain).doFilter(request, response);
		verify(securityContext, never()).setAuthentication(any());
		verifyNoInteractions(tokenService);
	}

	@Test
	void token_invalido_ou_na_blacklist_nao_autentica_mas_continua_o_filtro() throws Exception {
		//arrange
		Cookie cookieToken = new Cookie("token", "token-invalido-ou-invalidado");
		when(request.getCookies()).thenReturn(new Cookie[] { cookieToken });
		when(tokenService.tokenEstaValido("token-invalido-ou-invalidado")).thenReturn(false);

		//act
		securityFilter.doFilterInternal(request, response, filterChain);

		//assert
		verify(filterChain).doFilter(request, response);
		verify(securityContext, never()).setAuthentication(any());
		verify(tokenService, never()).validarToken(any()); // nem chega a validar, ja falhou antes
	}

	@Test
	void token_valido_e_usuario_encontrado_autentica_e_continua_o_filtro() throws Exception {
		//arrange
		Cookie cookieToken = new Cookie("token", "token-valido");
		when(request.getCookies()).thenReturn(new Cookie[] { cookieToken });
		when(tokenService.tokenEstaValido("token-valido")).thenReturn(true);
		when(tokenService.validarToken("token-valido")).thenReturn("user@teste.com");
		when(usuarioRepository.findByEmail("user@teste.com")).thenReturn(usuario);

		//act
		securityFilter.doFilterInternal(request, response, filterChain);

		//assert
		verify(securityContext).setAuthentication(argThat(auth ->
				auth instanceof UsernamePasswordAuthenticationToken
				&& auth.getPrincipal() == usuario
		));
		verify(filterChain).doFilter(request, response);
	}

	@Test
	void token_valido_mas_usuario_nao_encontrado_nao_autentica_mas_continua_o_filtro() throws Exception {
		//arrange
		Cookie cookieToken = new Cookie("token", "token-valido");
		when(request.getCookies()).thenReturn(new Cookie[] { cookieToken });
		when(tokenService.tokenEstaValido("token-valido")).thenReturn(true);
		when(tokenService.validarToken("token-valido")).thenReturn("naoexiste@teste.com");
		when(usuarioRepository.findByEmail("naoexiste@teste.com")).thenReturn(null);

		//act
		securityFilter.doFilterInternal(request, response, filterChain);

		//assert
		verify(securityContext, never()).setAuthentication(any());
		verify(filterChain).doFilter(request, response);
	}

	@Test
	void multiplos_cookies_recupera_apenas_o_cookie_de_nome_token() throws Exception {
		//arrange
		Cookie outroCookie = new Cookie("JSESSIONID", "abc123");
		Cookie cookieToken = new Cookie("token", "token-valido");
		when(request.getCookies()).thenReturn(new Cookie[] { outroCookie, cookieToken });
		when(tokenService.tokenEstaValido("token-valido")).thenReturn(true);
		when(tokenService.validarToken("token-valido")).thenReturn("user@teste.com");
		when(usuarioRepository.findByEmail("user@teste.com")).thenReturn(usuario);

		//act
		securityFilter.doFilterInternal(request, response, filterChain);

		//assert
		verify(securityContext).setAuthentication(any());
		verify(filterChain).doFilter(request, response);
	}
}