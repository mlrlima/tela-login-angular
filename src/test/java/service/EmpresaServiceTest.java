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

import dto.EmpresaResponseDTO;
import exception.GlobalExceptionHandler;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;
import repository.UsuarioRepository;

// OBS: as anotacoes de cache (@Cacheable, @CacheEvict, @Caching) so funcionam
// atraves do proxy do Spring, entao nao entram nesses testes unitarios

@ExtendWith(MockitoExtension.class)
class EmpresaServiceTest {

	@Mock
	private EmpresaRepository empresaRepository;
	@Mock
	private UsuarioRepository usuarioRepository;

	@InjectMocks
	private EmpresaService empresaService;

	@Mock
	private SecurityContext securityContext;

	@Mock
	private Authentication authentication;

	private MockedStatic<SecurityContextHolder> securityContextHolderMock;

	private Usuario admin;
	private Usuario userComum;
	private Empresa empresa;

	@BeforeEach //funcao eh chamada antes de cada uma
	void setUp() {
		// cria ADMIN
		admin = new Usuario();
		admin.setId(1L);
		admin.setEmail("admin@teste.com");
		admin.setRole(Role.ADMIN);

		// cria USER comum
		userComum = new Usuario();
		userComum.setId(2L);
		userComum.setEmail("user@teste.com");
		userComum.setRole(Role.USER);

		// empresa base usada nos testes
		empresa = new Empresa();
		empresa.setId(10L);
		empresa.setNome("Petshop Central");
		empresa.setUsuarios(new HashSet<Usuario>());

		// mocka o SecurityContextHolder estatico pra sempre devolver nosso securityContext mockado
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


	// Page<EmpresaResponseDTO> getAllEmpresas(Pageable pageable)============================

	@Test
	void getAllEmpresas_retorna_todas_as_empresas_paginado() {
		//arrange
		Empresa empresa2 = new Empresa();
		empresa2.setId(11L);
		empresa2.setNome("Petshop Norte");
		empresa2.setUsuarios(new HashSet<Usuario>());

		logarComo(admin);
		
		Pageable pageable = PageRequest.of(0, 10);
		when(empresaRepository.findAll(pageable))
				.thenReturn(new PageImpl<>(Arrays.asList(empresa, empresa2), pageable, 2));

		//act
		Page<EmpresaResponseDTO> resultado = empresaService.getAllEmpresas(pageable);

		//assert
		assertEquals(2, resultado.getContent().size());
		assertEquals(empresa.getId(), resultado.getContent().get(0).getId());
		assertEquals(empresa2.getId(), resultado.getContent().get(1).getId());
		verify(empresaRepository).findAll(pageable);
	}

	// EmpresaResponseDTO createEmpresa(Empresa empresa)=====================================

	@Test
	void createEmpresa_zera_id_e_salva() {
		//arrange
		Empresa nova = new Empresa();
		nova.setId(999L); // deve ser ignorado / zerado pelo service
		nova.setNome("Petshop Novo");
		nova.setUsuarios(new HashSet<Usuario>());

		logarComo(admin);
		when(empresaRepository.save(any(Empresa.class))).thenAnswer(inv -> inv.getArgument(0));

		//act
		EmpresaResponseDTO resultado = empresaService.createEmpresa(nova);

		//assert
		assertNull(nova.getId());
		assertEquals("Petshop Novo", resultado.getNome());
		verify(empresaRepository).save(nova);
	}

	@Test
	void createEmpresa_vincula_usuarios_existentes() {
		//arrange
		Usuario usuarioStub = new Usuario();
		usuarioStub.setId(2L); // vem do JSON so com o id preenchido

		Empresa nova = new Empresa();
		nova.setNome("Petshop Vinculado");
		Set<Usuario> usuariosDoJson = new HashSet<>();
		usuariosDoJson.add(usuarioStub);
		nova.setUsuarios(usuariosDoJson);
		
		logarComo(admin);

		when(usuarioRepository.findAllById(anySet())).thenReturn(Arrays.asList(userComum));
		when(empresaRepository.save(any(Empresa.class))).thenAnswer(inv -> inv.getArgument(0));

		//act
		empresaService.createEmpresa(nova);

		//assert
		assertEquals(1, nova.getUsuarios().size());
		assertTrue(nova.getUsuarios().contains(userComum));
	}

	@Test
	void createEmpresa_lista_de_usuarios_vazia_fica_vazia() {
		//arrange
		Empresa nova = new Empresa();
		nova.setNome("Petshop Sem Usuarios");
		nova.setUsuarios(new HashSet<Usuario>());

		logarComo(admin);
		when(empresaRepository.save(any(Empresa.class))).thenAnswer(inv -> inv.getArgument(0));

		//act
		empresaService.createEmpresa(nova);

		//assert
		assertNotNull(nova.getUsuarios());
		assertTrue(nova.getUsuarios().isEmpty());
		verify(usuarioRepository, never()).findAllById(anySet());
	}

	@Test
	void createEmpresa_usuario_inexistente_lanca_excecao() {
		//arrange
		Usuario usuarioStub = new Usuario();
		usuarioStub.setId(999L); // id que nao existe no banco

		Empresa nova = new Empresa();
		nova.setNome("Petshop Invalido");
		Set<Usuario> usuariosDoJson = new HashSet<>();
		usuariosDoJson.add(usuarioStub);
		nova.setUsuarios(usuariosDoJson);
		
		logarComo(admin);

		// repository nao encontra nenhum dos ids informados
		when(usuarioRepository.findAllById(anySet())).thenReturn(Arrays.asList());

		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> empresaService.createEmpresa(nova));

		verify(empresaRepository, never()).save(any());
	}

	// EmpresaResponseDTO getEmpresaById(Long id)============================================

	@Test
	void admin_busca_empresa_por_id() {
		//arrange
		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
		logarComo(admin);

		//act
		EmpresaResponseDTO resultado = empresaService.getEmpresaById(10L);

		//assert
		assertEquals(empresa.getId(), resultado.getId());
	}

	@Test
	void user_nao_busca_empresa_por_id() {
		//arrange
		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
		logarComo(userComum);

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> empresaService.getEmpresaById(10L));
	}

	@Test
	void getEmpresaById_lanca_excecao_quando_nao_encontrada() {
		//arrange
		when(empresaRepository.findById(999L)).thenReturn(Optional.empty());

		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> empresaService.getEmpresaById(999L));
	}

	// EmpresaResponseDTO getEmpresaByNome(String nome) =====================================

	@Test
	void admin_busca_empresa_por_nome() {
		//arrange
		when(empresaRepository.findByNome("Petshop Central")).thenReturn(Optional.of(empresa));
		logarComo(admin);

		//act
		EmpresaResponseDTO resultado = empresaService.getEmpresaByNome("Petshop Central");

		//assert
		assertEquals(empresa.getNome(), resultado.getNome());
	}

	@Test
	void user_nao_busca_empresa_por_nome() {
		//arrange
		when(empresaRepository.findByNome("Petshop Central")).thenReturn(Optional.of(empresa));
		logarComo(userComum);

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> empresaService.getEmpresaByNome("Petshop Central"));
	}

	@Test
	void getEmpresaByNome_lanca_excecao_quando_nao_encontrada() {
		//arrange
		when(empresaRepository.findByNome("Nao Existe")).thenReturn(Optional.empty());

		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> empresaService.getEmpresaByNome("Nao Existe"));
	}

	// EmpresaResponseDTO updateEmpresa(Empresa empresa) ====================================

	@Test
	void admin_atualiza_empresa() {
		//arrange
		logarComo(admin);
		when(empresaRepository.save(any(Empresa.class))).thenAnswer(inv -> inv.getArgument(0));

		empresa.setNome("Petshop Editado");

		//act
		EmpresaResponseDTO resultado = empresaService.updateEmpresa(empresa);

		//assert
		assertEquals("Petshop Editado", resultado.getNome());
		verify(empresaRepository).save(empresa);
	}

	@Test
	void user_nao_atualiza_empresa() {
		//arrange
		logarComo(userComum);

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> empresaService.updateEmpresa(empresa));

		verify(empresaRepository, never()).save(any());
	}

	// void deleteEmpresa(Long id)

	@Test
	void admin_deleta_empresa() {
		//arrange
		Set<Usuario> usuariosVinculados = new HashSet<>();
		usuariosVinculados.add(userComum);
		userComum.setEmpresas(new HashSet<Empresa>());
		userComum.getEmpresas().add(empresa);
		empresa.setUsuarios(usuariosVinculados);

		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
		logarComo(admin);

		//act
		empresaService.deleteEmpresa(10L);

		//assert
		assertFalse(userComum.getEmpresas().contains(empresa));
		verify(empresaRepository).delete(empresa);
	}

	@Test
	void user_nao_deleta_empresa() {
		//arrange
		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
		logarComo(userComum);

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> empresaService.deleteEmpresa(10L));

		verify(empresaRepository, never()).delete(any());
	}

	@Test
	void deleteEmpresa_lanca_excecao_quando_nao_encontrada() {
		//arrange
		when(empresaRepository.findById(999L)).thenReturn(Optional.empty());

		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> empresaService.deleteEmpresa(999L));

		verify(empresaRepository, never()).delete(any());
	}
}