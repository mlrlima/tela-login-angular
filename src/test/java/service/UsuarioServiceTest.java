package service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import dto.UsuarioResponseDTO;
import jakarta.servlet.http.HttpServletRequest;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.UsuarioRepository;

@ExtendWith(MockitoExtension.class)
class UsuarioServiceTest{
	
	@Mock
	private UsuarioRepository usuarioRepository;
 
	@InjectMocks
	private UsuarioService usuarioService;
 
	@Mock
	private HttpServletRequest request;
	
	@Mock
	private PetService petService;
	
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
	}
	
	
	//List<UsuarioResponseDTO> getAllUsuarios(HttpServletRequest request)
 
	@Test
	void admin_retorna_todos_os_usuarios() {
		//arrange
		
		// admin esta logado
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		//a lista retorna todos os usuarios
		when(usuarioRepository.findAll()).thenReturn(Arrays.asList(admin, userComum));
		
		//act
		
		// resultado real
		List<UsuarioResponseDTO> resultado = usuarioService.getAllUsuarios(request);
		
		//assert
		
		//verifica o tamanho da lista
		assertEquals(2, resultado.size());
		//verifica se os dois usuarios da lista estao corretos
		assertEquals(admin.getId(), resultado.get(0).getId());
		assertEquals(userComum.getId(), resultado.get(1).getId());
		//verifica se o metodo foi chamado durante a execucao
		verify(usuarioRepository).findAll();
	}
	
	@Test
	void user_retorna_apenas_si_mesmo() {
		//ARRANGE
		
		//userComum esta logado
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
		
		//ACT
		
		// resultado real
		List<UsuarioResponseDTO> resultado = usuarioService.getAllUsuarios(request);
		
		//ASSERT
		
		//verifica o tamanho da lista
		assertEquals(1, resultado.size());
		//verifica se o usuario retornado eh ele mesmo
		assertEquals(userComum.getId(), resultado.get(0).getId());
		//verifica se a funcao NAO foi chamada, pois
		//ela so eh chamada se o logado for ADMIN
		verify(usuarioRepository, never()).findAll();
	}
	
	
	// UsuarioResponseDTO createUsuario(Usuario usuario)
	
	@Test
	void salva_com_role_USER() {
		//arrange
		
		Usuario novo = new Usuario();
		novo.setId(99L); // deve ser ignorado / zerado pelo service
		novo.setEmail("novo@teste.com");
		novo.setSenha("senha");
		novo.setRole(Role.ADMIN); // tentando burlar, deve virar USER
		novo.setEmpresas(new HashSet<Empresa>());
		
		//inv significa Invocation (a chamada do método).
		//Ele contém informações sobre a execução, como:
		//quais argumentos foram passados;
		//qual método foi chamado; etc
		when(usuarioRepository.save(any(Usuario.class)))
				 .thenAnswer(inv -> inv.getArgument(0)); //retorna apenas um argumento [0]
	
		//act
		
		UsuarioResponseDTO resultado = usuarioService.createUsuario(novo);
		
		//assert
		
		assertEquals(Role.USER, novo.getRole()); //verifica se a role eh USER
		assertNull(novo.getId()); //a funcao deve NULL o id
		assertEquals("novo@teste.com", resultado.getEmail());
		//verifica se a funcao foi chamada
		verify(usuarioRepository).save(novo);
	}
	
	
	//UsuarioResponseDTO getUsuarioById(Long id, HttpServletRequest request)
	
	@Test
	void admin_retorna_qualquer_user() {
		//arrange
		
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
		//pois o reporitory retorna Optional<Usuario>
 
		//act
		UsuarioResponseDTO resultado = usuarioService.getUsuarioById(2L, request);
 
		//assert
		assertEquals(userComum.getId(), resultado.getId());
	}
	
	@Test
	void user_retorna_si_mesmo() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
 
		//act
		UsuarioResponseDTO resultado = usuarioService.getUsuarioById(2L, request);
		
		//assert
		assertEquals(userComum.getId(), resultado.getId());
	}
	
	@Test
	void user_nao_retorna_outro_usuario() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
		when(usuarioRepository.findById(1L)).thenReturn(Optional.of(admin));
		//1L eh o admin
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> usuarioService.getUsuarioById(1L, request));
	}
	
	@Test
	void throw_exception_usuario_nao_encontrado() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(usuarioRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> usuarioService.getUsuarioById(999L, request));
	}
	
	// Usuario getUsuarioByEmailAndSenha(String email, String senha)
	
	@Test
	void retorna_usuario(){
		//arrangr
		when(usuarioRepository.findByEmail("admin@teste.com")).thenReturn(Optional.of(admin));
		 
		//act
		Usuario resultado = usuarioService.getUsuarioByEmailAndSenha("admin@teste.com", "1234");
 
		//assert
		assertNotNull(resultado);
		assertEquals(admin.getId(), resultado.getId());
	}
	
	@Test
	void senha_incorreta_retorna_null() {
		when(usuarioRepository.findByEmail("admin@teste.com")).thenReturn(Optional.of(admin));
		 
		Usuario resultado = usuarioService.getUsuarioByEmailAndSenha("admin@teste.com", "senhaErrada");
 
		assertNull(resultado);
	}
	
	@Test
	void email_inexistente_retorna_null() {
		when(usuarioRepository.findByEmail("naoexiste@teste.com")).thenReturn(Optional.empty());
 
		Usuario resultado = usuarioService.getUsuarioByEmailAndSenha("naoexiste@teste.com", "1234");
 
		assertNull(resultado);
	}
	
	
	//UsuarioResponseDTO updateUsuario(Usuario usuario, HttpServletRequest request)
	
	@Test
	void senha_vazia_mantem_senha_atual() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
 
		// update userComum
		Usuario alteracao = new Usuario();
		alteracao.setId(2L);
		alteracao.setEmail("user_editado@teste.com");
		alteracao.setSenha(""); // senha em branco
		alteracao.setRole(Role.USER);
		alteracao.setEmpresas(new HashSet<Empresa>());
		
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
		when(usuarioRepository.save(any(Usuario.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		usuarioService.updateUsuario(alteracao, request);
 
		//assert
		assertEquals(userComum.getSenha(), alteracao.getSenha());
		verify(usuarioRepository).save(alteracao);
	}
	
	@Test
	void user_nao_update_outro_usuario() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
 
		Usuario alteracao = new Usuario();
		alteracao.setId(1L); // tentando editar o admin
		alteracao.setSenha("qualquer");
 
		//act & assert
		
		assertThrows(RuntimeException.class,
				() -> usuarioService.updateUsuario(alteracao, request));
 
		verify(usuarioRepository, never()).save(any());
	}
	
	
	//void deleteUsuario(Long id, HttpServletRequest request)
	
	@Test
	void admin_deleta_qualquer_usuario() {
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(usuarioRepository.findById(2L)).thenReturn(Optional.of(userComum));
 
		usuarioService.deleteUsuario(2L, request);
 
		verify(petService).deletePetsUsuario(userComum);
		verify(usuarioRepository).delete(userComum);
	}
	
	@Test
	void user_nao_deleta_outro_usuario() {
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
		when(usuarioRepository.findById(1L)).thenReturn(Optional.of(admin));
 
		assertThrows(RuntimeException.class,
				() -> usuarioService.deleteUsuario(1L, request));
 
		verify(petService, never()).deletePetsUsuario(any());
		verify(usuarioRepository, never()).delete(any());
	}
}