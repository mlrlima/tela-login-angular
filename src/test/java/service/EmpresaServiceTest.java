package service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import dto.EmpresaResponseDTO;
import jakarta.servlet.http.HttpServletRequest;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;
import repository.UsuarioRepository;

@ExtendWith(MockitoExtension.class)
public class EmpresaServiceTest {

	@Mock
	private EmpresaRepository empresaRepository;
	
	@InjectMocks
	private EmpresaService empresaService;
	
	@Mock
	private HttpServletRequest request;
	
	@Mock
	private UsuarioRepository usuarioRepository;
	
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
	}
	
	// List<EmpresaResponseDTO> getAllEmpresas(HttpServletRequest request)
	
	@Test
	void retorna_todas_as_empresas() {
		//arrange
		Empresa empresa2 = new Empresa();
		empresa2.setId(11L);
		empresa2.setNome("Petshop Norte");
		empresa2.setUsuarios(new HashSet<Usuario>());
 
		when(empresaRepository.findAll()).thenReturn(Arrays.asList(empresa, empresa2));
 
		//act
		List<EmpresaResponseDTO> resultado = empresaService.getAllEmpresas(request);
 
		//assert
		assertEquals(2, resultado.size()); //ve se tem duas empresas
		//verifica cada uma
		assertEquals(empresa.getId(), resultado.get(0).getId());
		assertEquals(empresa2.getId(), resultado.get(1).getId());
		verify(empresaRepository).findAll();
	}
	
	
	// EmpresaResponseDTO createEmpresa(Empresa empresa, HttpServletRequest request)
	 
	@Test
	void zera_id_e_salva() {
		//arrange
		Empresa nova = new Empresa();
		nova.setId(999L); // deve ser ignorado / zerado pelo service
		nova.setNome("Petshop Novo");
		nova.setUsuarios(new HashSet<Usuario>());
 
		when(empresaRepository.save(any(Empresa.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		EmpresaResponseDTO resultado = empresaService.createEmpresa(nova, request);
 
		//assert
		assertNull(nova.getId());
		assertEquals("Petshop Novo", resultado.getNome());
		
		assertNotNull(resultado.getUsuarios());
		assertTrue(resultado.getUsuarios().isEmpty()); //verifica se a lista de usuarios esta vazia
		
		verify(empresaRepository).save(nova);
	}
	
	@Test
	void vincula_usuarios_existentes() {
		//arrange
		Usuario usuarioStub = new Usuario();
		usuarioStub.setId(2L); // vem do JSON so com o id preenchido
 
		Empresa nova = new Empresa();
		nova.setNome("Petshop Vinculado");
		Set<Usuario> usuariosDoJson = new HashSet<>();
		usuariosDoJson.add(usuarioStub); //adiciona o userComum
		nova.setUsuarios(usuariosDoJson);
 
		when(usuarioRepository.findAllById(anySet())).thenReturn(Arrays.asList(userComum));
		when(empresaRepository.save(any(Empresa.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		empresaService.createEmpresa(nova, request);
 
		//assert
		assertEquals(1, nova.getUsuarios().size());
		assertTrue(nova.getUsuarios().contains(userComum)); //verifica se o usuario ta na lista
	}

	@Test
	void vincula_usuario_inexistente_lanca_excecao() {
		//arrange
		Usuario usuarioStub = new Usuario();
		usuarioStub.setId(999L); // id que nao existe no banco
 
		Empresa nova = new Empresa();
		nova.setNome("Petshop Invalido");
		Set<Usuario> usuariosDoJson = new HashSet<>();
		usuariosDoJson.add(usuarioStub);
		nova.setUsuarios(usuariosDoJson);
 
		// repository nao encontra nenhum dos ids informados
		when(usuarioRepository.findAllById(anySet())).thenReturn(Arrays.asList());
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.createEmpresa(nova, request));
 
		verify(empresaRepository, never()).save(any());
	}
	
	
	// EmpresaResponseDTO getEmpresaById(Long id, HttpServletRequest request)
	 
	@Test
	void admin_busca_por_id() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
 
		//act
		EmpresaResponseDTO resultado = empresaService.getEmpresaById(10L, request);
 
		//assert
		assertEquals(empresa.getId(), resultado.getId());
	}
	
	@Test
	void user_nao_busca_empresa_por_id() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.getEmpresaById(10L, request));
	}
	
	@Test
	void throw_exception_id_inexistente() {
		//arrange
		when(empresaRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.getEmpresaById(999L, request));
	}
	
	// EmpresaResponseDTO getEmpresaByNome(String nome, HttpServletRequest request)
	 
	@Test
	void admin_busca_empresa_por_nome() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(empresaRepository.findByNome("Petshop Central")).thenReturn(Optional.of(empresa));
 
		//act
		EmpresaResponseDTO resultado = empresaService.getEmpresaByNome("Petshop Central", request);
 
		//assert
		assertEquals(empresa.getNome(), resultado.getNome());
	}
	
	@Test
	void user_nao_busca_empresa_por_nome() {
		//arrange
		when(empresaRepository.findByNome("Petshop Central")).thenReturn(Optional.of(empresa));
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.getEmpresaByNome("Petshop Central", request));
	}
	
	@Test
	void throw_exception_busca_por_nome_inexistente() {
		//arrange
		when(empresaRepository.findByNome("Nao Existe")).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.getEmpresaByNome("Nao Existe", request));
	}
	
	// EmpresaResponseDTO updateEmpresa(Empresa empresa, HttpServletRequest request)
	 
	@Test
	void admin_atualiza_empresa() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(empresaRepository.save(any(Empresa.class))).thenAnswer(inv -> inv.getArgument(0));
 
		empresa.setNome("Petshop Editado");
 
		//act
		EmpresaResponseDTO resultado = empresaService.updateEmpresa(empresa, request);
 
		//assert
		assertEquals("Petshop Editado", resultado.getNome());
		verify(empresaRepository).save(empresa);
	}
	
	@Test
	void user_nao_atualiza_empresa() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.updateEmpresa(empresa, request));
 
		verify(empresaRepository, never()).save(any());
	}
	
	// void deleteEmpresa(Long id, HttpServletRequest request)
	 
	@Test
	void admin_deleta_empresa() {
		//arrange
		Set<Usuario> usuariosVinculados = new HashSet<>();
		usuariosVinculados.add(userComum); //adicionando usuario relacionado
		userComum.setEmpresas(new HashSet<Empresa>());
		userComum.getEmpresas().add(empresa);
		empresa.setUsuarios(usuariosVinculados);
 
		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
 
		//act
		empresaService.deleteEmpresa(10L, request);
 
		//assert
		assertFalse(userComum.getEmpresas().contains(empresa)); //verifica se a empresa esta ainda vinculada ao usuario
		verify(empresaRepository).delete(empresa);
	}
	
	@Test
	void user_nao_deleta_empresa() {
		//arrange
		when(empresaRepository.findById(10L)).thenReturn(Optional.of(empresa));
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.deleteEmpresa(10L, request));
 
		verify(empresaRepository, never()).delete(any());
	}
	
	@Test
	void throw_exception_deletar_id_inexistente() {
		//arrange
		when(empresaRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> empresaService.deleteEmpresa(999L, request));
 
		verify(empresaRepository, never()).delete(any());
	}
	
}
