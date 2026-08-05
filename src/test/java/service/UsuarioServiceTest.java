package service;
 
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
 
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
 
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
 
import dto.UsuarioResponseDTO;
import exception.GlobalExceptionHandler;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;
import repository.UsuarioRepository;

// OBS: as anotacoes de cache (@Cacheable, @CacheEvict, @Caching) so funcionam
// atraves do proxy do Spring, entao nao entram nesses testes unitarios
// (pra testar cache de verdade seria necessario um @SpringBootTest / contexto real)

@ExtendWith(MockitoExtension.class)
class UsuarioServiceTest{
	

	@Mock
	private UsuarioRepository usuarioRepository;
	@Mock
	private EmpresaRepository empresaRepository;
 
	@Mock
	private PetService petService;
	@InjectMocks
	private UsuarioService usuarioService;
 
	@Mock
	private SecurityContext securityContext;
 
	@Mock
	private Authentication authentication;
 
	private MockedStatic<SecurityContextHolder> securityContextHolderMock;
 
	private Usuario admin;
	private Usuario userComum;
 
	@BeforeEach //funcao eh chamada antes de cada uma
	void setUp() {
		// cria ADMIN
		admin = new Usuario();
		admin.setId(1L);
		admin.setEmail("admin@teste.com");
		admin.setSenha("1234");
		admin.setRole(Role.ADMIN);
		admin.setEmpresas(new HashSet<Empresa>());
 
		//cria USER comum
		userComum = new Usuario();
		userComum.setId(2L);
		userComum.setEmail("user@teste.com");
		userComum.setSenha("4321");
		userComum.setRole(Role.USER);
		userComum.setEmpresas(new HashSet<Empresa>());
 
		// mocka o SecurityContextHolder estatico pra sempre devolver o securityContext mockado
		securityContextHolderMock = mockStatic(SecurityContextHolder.class);
		securityContextHolderMock.when(SecurityContextHolder::getContext).thenReturn(securityContext);
	}
	
	@AfterEach
	void tearDown() {
		securityContextHolderMock.close();
	}
	
	
	// simula um usuario logado no contexto de seguranca
	private void logarComo(Usuario usuario) {
		when(securityContext.getAuthentication()).thenReturn(authentication);
		when(authentication.isAuthenticated()).thenReturn(true);
		when(authentication.getPrincipal()).thenReturn(usuario);
	}
	
	// simula ninguem logado (sem token / nao autenticado)
	private void semUsuarioLogado() {
		when(securityContext.getAuthentication()).thenReturn(null);
	}
	
	
	// boolean isAdminLogado() ======================================
	 
	@Test
	void isAdminLogado_retorna_true_para_admin() {
		//arrange
		logarComo(admin);
 
		//act & assert
		assertTrue(usuarioService.isAdminLogado());
	}
	
	@Test
	void isAdminLogado_retorna_false_para_user_comum() {
		//arrange
		logarComo(userComum);
 
		//act & assert
		assertFalse(usuarioService.isAdminLogado());
	}
	
	@Test
	void isAdminLogado_retorna_false_quando_ninguem_esta_logado() {
		//arrange
		semUsuarioLogado();
 
		//act & assert
		assertFalse(usuarioService.isAdminLogado());
	}
	
	
	// Page<UsuarioResponseDTO> getAllUsuarios(Pageable pageable) ==============
	
	@Test
	void admin_retorna_todos_os_usuarios_paginado() {
		//arrange
		logarComo(admin);
		Pageable pageable = PageRequest.of(0, 10);
		when(usuarioRepository.findAll(pageable))
				.thenReturn(new PageImpl<>(Arrays.asList(admin, userComum), pageable, 2));
 
		//act
		Page<UsuarioResponseDTO> resultado = usuarioService.getAllUsuarios(pageable);
 
		//assert
		
		//qtd de usuarios retornados
		assertEquals(2, resultado.getContent().size()); 
		//verifica se cada usuario retornado esta correto
		assertEquals(admin.getId(), resultado.getContent().get(0).getId());
		assertEquals(userComum.getId(), resultado.getContent().get(1).getId());
		//verifica se o metodo foi chamado
		verify(usuarioRepository).findAll(pageable);
	}
	
	@Test
	void user_retorna_apenas_si_mesmo_paginado() {
		//arrange
		logarComo(userComum);
		Pageable pageable = PageRequest.of(0, 10);
		when(usuarioRepository.findById(userComum.getId())).thenReturn(Optional.of(userComum));
 
		//act
		Page<UsuarioResponseDTO> resultado = usuarioService.getAllUsuarios(pageable);
 
		//assert
		
		//deve retornar apenas um usuario, ele mesmo
		assertEquals(1, resultado.getContent().size());
		assertEquals(userComum.getId(), resultado.getContent().get(0).getId());
		//verifica se o findAll NAO foi chamado
		verify(usuarioRepository, never()).findAll(any(Pageable.class));
	}
	
	@Test
	void getAllUsuarios_lanca_excecao_quando_usuario_logado_nao_existe_mais() {
		//arrange
		logarComo(userComum);
		Pageable pageable = PageRequest.of(0, 10);
		when(usuarioRepository.findById(userComum.getId())).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> usuarioService.getAllUsuarios(pageable));
	}
	
	
	// UsuarioResponseDTO createUsuario(Usuario usuario) ========================
	
	@Test
	void salva_com_role_USER() {
		//arrange
		Usuario novo = new Usuario();
		novo.setId(99L); // deve ser ignorado / zerado pelo service
		novo.setEmail("novo@teste.com");
		novo.setSenha("senha");
		novo.setRole(Role.ADMIN); // tentando burlar, deve virar USER
		novo.setEmpresas(new HashSet<Empresa>());
 
		when(usuarioRepository.save(any(Usuario.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		UsuarioResponseDTO resultado = usuarioService.createUsuario(novo);
 
		//assert
		
		//verifica se a role eh USER
		assertEquals(Role.USER, novo.getRole()); 
		//a funcao deve zerar o id
		assertNull(novo.getId()); 
		
		assertEquals("novo@teste.com", resultado.getEmail());
		
		//verifica se a funcao foi chamada
		verify(usuarioRepository).save(novo);
		
		// lista de empresas vazia
		verify(empresaRepository, never()).findAllById(anySet()); 
	}
	
	@Test
	void createUsuario_vincula_empresas_existentes() {
		//arrange
		Empresa empresaStub = new Empresa();
		empresaStub.setId(10L); // vem do JSON so com o id
 
		Empresa empresaGerenciada = new Empresa();
		empresaGerenciada.setId(10L);
		empresaGerenciada.setNome("Petshop Central");
 
		Usuario novo = new Usuario();
		novo.setEmail("novo@teste.com");
		novo.setSenha("senha");
		Set<Empresa> empresasDoJson = new HashSet<>();
		empresasDoJson.add(empresaStub);
		novo.setEmpresas(empresasDoJson);
 
		when(empresaRepository.findAllById(anySet())).thenReturn(Arrays.asList(empresaGerenciada));
		when(usuarioRepository.save(any(Usuario.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		usuarioService.createUsuario(novo);
 
		//assert
		
		//verifica se a lista de empresas tem apenas 1
		assertEquals(1, novo.getEmpresas().size());
		// verifica se a empresa rertornada esta correta
		assertTrue(novo.getEmpresas().contains(empresaGerenciada));
	}

	
	// Usuario buscarUsuarioNoCache(Long id) ====================================
	
	@Test
	void buscarUsuarioNoCache_retorna_usuario_quando_encontrado() {
		//arrange
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
 
		//act
		Usuario resultado = usuarioService.buscarUsuarioNoCache(2L);
 
		//assert
		assertEquals(userComum.getId(), resultado.getId());
	}
	
	@Test
	void buscarUsuarioNoCache_lanca_excecao_quando_nao_encontrado() {
		//arrange
		when(usuarioRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> usuarioService.buscarUsuarioNoCache(999L));
	}
	
	
	// UsuarioResponseDTO getUsuarioById(Long id) ===============================
	
	@Test
	void admin_retorna_qualquer_user_por_id() {
		//arrange
		logarComo(admin);
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
 
		//act
		UsuarioResponseDTO resultado = usuarioService.getUsuarioById(2L);
 
		//assert
		assertEquals(userComum.getId(), resultado.getId());
	}

	@Test
	void user_retorna_si_mesmo_por_id() {
		//arrange
		logarComo(userComum);
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
 
		//act
		UsuarioResponseDTO resultado = usuarioService.getUsuarioById(2L);
 
		//assert
		assertEquals(userComum.getId(), resultado.getId());
	}
	
	@Test
	void user_nao_retorna_outro_usuario_por_id() {
		//arrange
		logarComo(userComum);
		when(usuarioRepository.findById(1L)).thenReturn(Optional.of(admin));
 
		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> usuarioService.getUsuarioById(1L));
	}
	
	@Test
	void getUsuarioById_lanca_excecao_quando_nao_encontrado() {
		//arrange
		when(usuarioRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> usuarioService.getUsuarioById(999L));
	}

	// UsuarioRelacionadoDTO getUsuarioByEmail(String email) =====================
	
	@Test
	void admin_busca_qualquer_usuario_por_email() {
		//arrange
		logarComo(admin);
		when(usuarioRepository.findByEmail("user@teste.com")).thenReturn(userComum);
 
		//act
		var resultado = usuarioService.getUsuarioByEmail("user@teste.com");
 
		//assert
		assertEquals(userComum.getId(), resultado.getId());
	}

	@Test
	void user_busca_a_si_mesmo_por_email() {
		//arrange
		logarComo(userComum);
		when(usuarioRepository.findByEmail("user@teste.com")).thenReturn(userComum);
 
		//act
		var resultado = usuarioService.getUsuarioByEmail("user@teste.com");
 
		//assert
		assertEquals(userComum.getId(), resultado.getId());
	}

	@Test
	void user_nao_busca_outro_usuario_por_email() {
		//arrange
		logarComo(userComum);
		when(usuarioRepository.findByEmail("admin@teste.com")).thenReturn(admin);
 
		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> usuarioService.getUsuarioByEmail("admin@teste.com"));
	}
	
	@Test
	void getUsuarioByEmail_lanca_excecao_quando_nao_encontrado() {
		//arrange
		when(usuarioRepository.findByEmail("naoexiste@teste.com")).thenReturn(null);
 
		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> usuarioService.getUsuarioByEmail("naoexiste@teste.com"));
	}
	
	
	// Usuario getUsuarioByEmailAndSenha(String email, String senha) =============
	
	@Test
	void login_com_credenciais_corretas_retorna_usuario() {
		//arrange
		when(usuarioRepository.findByEmail("admin@teste.com")).thenReturn(admin);
 
		//act
		Usuario resultado = usuarioService.getUsuarioByEmailAndSenha("admin@teste.com", "1234");
 
		//assert
		assertNotNull(resultado);
		assertEquals(admin.getId(), resultado.getId());
	}
	
	@Test
	void login_com_senha_errada_retorna_null() {
		//arrange
		when(usuarioRepository.findByEmail("admin@teste.com")).thenReturn(admin);
 
		//act
		Usuario resultado = usuarioService.getUsuarioByEmailAndSenha("admin@teste.com", "senhaErrada");
 
		//assert
		assertNull(resultado);
	}
	
	@Test
	void login_com_email_inexistente_retorna_null() {
		//arrange
		when(usuarioRepository.findByEmail("naoexiste@teste.com")).thenReturn(null);
 
		//act
		Usuario resultado = usuarioService.getUsuarioByEmailAndSenha("naoexiste@teste.com", "1234");
 
		//assert
		assertNull(resultado);
	}
	
	
	// UsuarioResponseDTO updateUsuario(Usuario usuario)==========================
	
	@Test
	void senha_vazia_mantem_senha_atual() {
		//arrange
		logarComo(admin);
 
		Usuario alteracao = new Usuario();
		alteracao.setId(2L);
		alteracao.setEmail("user_editado@teste.com");
		alteracao.setSenha(""); // senha em branco
		alteracao.setRole(Role.USER);
		alteracao.setEmpresas(new HashSet<Empresa>());
 
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
		when(usuarioRepository.save(any(Usuario.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		usuarioService.updateUsuario(alteracao);
 
		//assert
		assertEquals(userComum.getSenha(), alteracao.getSenha());
		verify(usuarioRepository).save(alteracao);
	}
	
	@Test
	void senha_nova_muito_curta_lanca_excecao() {
		//arrange
		logarComo(admin);
 
		Usuario alteracao = new Usuario();
		alteracao.setId(2L);
		alteracao.setSenha("123"); // menos de 4 caracteres
		alteracao.setEmpresas(new HashSet<Empresa>());
 
		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> usuarioService.updateUsuario(alteracao));
 
		verify(usuarioRepository, never()).save(any());
	}

	@Test
	void senha_nova_valida_eh_criptografada_com_bcrypt() {
		//arrange
		logarComo(admin);
 
		Usuario alteracao = new Usuario();
		alteracao.setId(2L);
		alteracao.setSenha("novaSenha123");
		alteracao.setEmpresas(new HashSet<Empresa>());
 
		when(usuarioRepository.save(any(Usuario.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		usuarioService.updateUsuario(alteracao);
 
		//assert
		
		// nao pode ficar em texto puro
		assertNotEquals("novaSenha123", alteracao.getSenha());
		// verifica se a senha criptografada eh a mesma
		assertTrue(new BCryptPasswordEncoder().matches("novaSenha123", alteracao.getSenha()));
		// nao precisa buscar senha antiga
		verify(usuarioRepository, never()).findById(any()); 
	}
	
	@Test
	void user_nao_atualiza_outro_usuario() {
		//arrange
		logarComo(userComum);
 
		Usuario alteracao = new Usuario();
		alteracao.setId(1L); // tentando editar o admin
		alteracao.setSenha("qualquer123");
 
		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> usuarioService.updateUsuario(alteracao));
 
		verify(usuarioRepository, never()).save(any());
	}
	
	
	// void deleteUsuario(Long id) ===============================================
	
	@Test
	void admin_deleta_qualquer_usuario() {
		//arrange
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
		logarComo(admin);
 
		//act
		usuarioService.deleteUsuario(2L);
 
		//assert
		
		//verifica se a funcao de deletar pets do usuario foi chamada
		verify(petService).deletePetsUsuario(userComum);
		verify(usuarioRepository).delete(userComum);
	}

	@Test
	void user_nao_deleta_outro_usuario() {
		//arrange
		when(usuarioRepository.findById(1L)).thenReturn(Optional.of(admin));
		logarComo(userComum);
 
		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> usuarioService.deleteUsuario(1L));
 
		verify(petService, never()).deletePetsUsuario(any());
		verify(usuarioRepository, never()).delete(any());
	}

	@Test
	void deleteUsuario_lanca_excecao_quando_nao_encontrado() {
		//arrange
		when(usuarioRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> usuarioService.deleteUsuario(999L));
 
		verify(usuarioRepository, never()).delete(any());
	}


	
}