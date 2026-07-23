package service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import dto.PetResponseDTO;
import jakarta.servlet.http.HttpServletRequest;
import model.Especie;
import model.Pet;
import model.Role;
import model.Usuario;
import repository.PetRepository;

@ExtendWith(MockitoExtension.class)
public class PetServiceTest {

	@Mock
	private PetRepository petRepository;
 
	@InjectMocks
	private PetService petService;
 
	@Mock
	private HttpServletRequest request;
	
	private Usuario admin;
	private Usuario userComum;
	private Usuario outroUser;
	private Pet petDoUserComum;
	
	@BeforeEach //funcao eh chamada antes de cada uma
	void setUp() {
		// cria ADMIN
		admin = new Usuario();
		admin.setId(1L);
		admin.setEmail("admin@teste.com");
		admin.setRole(Role.ADMIN);
 
		// cria USER comum, dono do pet usado nos testes
		userComum = new Usuario();
		userComum.setId(2L);
		userComum.setEmail("user@teste.com");
		userComum.setRole(Role.USER);
 
		// outro USER, sem nenhum pet
		outroUser = new Usuario();
		outroUser.setId(3L);
		outroUser.setEmail("outro@teste.com");
		outroUser.setRole(Role.USER);
 
		// pet base usado nos testes, pertence ao userComum
		petDoUserComum = new Pet();
		petDoUserComum.setId(100L);
		petDoUserComum.setNome("Rex");
		petDoUserComum.setEspecie(Especie.CACHORRO);
		petDoUserComum.setDono(userComum);
	}
	
	// List<PetResponseDTO> getAllPets(HttpServletRequest request)
	 
	@Test
	void admin_retorna_todos_os_pets() {
		//arrange
		Pet outroPet = new Pet();
		outroPet.setId(101L);
		outroPet.setNome("Mingau");
		outroPet.setEspecie(Especie.GATO);
		outroPet.setDono(outroUser);
 
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(petRepository.findAll()).thenReturn(Arrays.asList(petDoUserComum, outroPet));
 
		//act
		List<PetResponseDTO> resultado = petService.getAllPets(request);
 
		//assert
		assertEquals(2, resultado.size());
		//verifica se os dois pets da lista estao corretos
		assertEquals(petDoUserComum.getId(), resultado.get(0).getId());
		assertEquals(outroPet.getId(), resultado.get(1).getId());
		verify(petRepository).findAll();
	}
	
	@Test
	void user_retorna_apenas_seus_proprios_pets() {
		//arrange
		Pet outroPet = new Pet();
		outroPet.setId(101L);
		outroPet.setNome("Mingau");
		outroPet.setEspecie(Especie.GATO);
		outroPet.setDono(outroUser);
		
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
		when(petRepository.findByDono_Id(userComum.getId())).thenReturn(Arrays.asList(petDoUserComum));
 
		//act
		List<PetResponseDTO> resultado = petService.getAllPets(request);
 
		//assert
		assertEquals(1, resultado.size());
		assertEquals(petDoUserComum.getId(), resultado.get(0).getId());
		verify(petRepository, never()).findAll();
	}
	
	
	// PetResponseDTO createPet(Pet pet, HttpServletRequest request)
	 
	@Test
	void define_logado_como_dono_e_zera_id() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
 
		Pet novoPet = new Pet();
		novoPet.setId(999L); // deve ser ignorado / zerado pelo service
		novoPet.setNome("Bidu");
		novoPet.setEspecie(Especie.CACHORRO);
 
		when(petRepository.save(any(Pet.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		PetResponseDTO resultado = petService.createPet(novoPet, request);
 
		//assert
		assertEquals(userComum, novoPet.getDono());
		assertNull(novoPet.getId());
		assertEquals("Bidu", resultado.getNome());
		verify(petRepository).save(novoPet);
	}
	
	
	// PetResponseDTO getPetById(Long id, HttpServletRequest request)
	 
	@Test
	void admin_busca_qualquer_pet_por_id() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
 
		//act
		PetResponseDTO resultado = petService.getPetById(100L, request);
 
		//assert
		assertEquals(petDoUserComum.getId(), resultado.getId());
	}
	
	@Test
	void dono_busca_proprio_pet() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
 
		//act
		PetResponseDTO resultado = petService.getPetById(100L, request);
 
		//assert
		assertEquals(petDoUserComum.getId(), resultado.getId());
	}
	
	@Test
	void user_nao_busca_pet_de_outro_usuario() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(request.getAttribute("usuarioLogado")).thenReturn(outroUser);
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> petService.getPetById(100L, request));
	}
	
	@Test
	void throw_exception_id_inexistente() {
		//arrange
		when(petRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> petService.getPetById(999L, request));
	}
	
	// PetResponseDTO updatePet(Pet pet, HttpServletRequest request)
	 
	@Test
	void dono_atualiza_proprio_pet() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
		when(petRepository.save(any(Pet.class))).thenAnswer(inv -> inv.getArgument(0));
 
		petDoUserComum.setNome("Rex Editado");
 
		//act
		PetResponseDTO resultado = petService.updatePet(petDoUserComum, request);
 
		//assert
		assertEquals("Rex Editado", resultado.getNome());
		verify(petRepository).save(petDoUserComum);
	}
	
	@Test
	void admin_atualiza_qualquer_pet() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
		when(petRepository.save(any(Pet.class))).thenAnswer(inv -> inv.getArgument(0));
 
		//act
		PetResponseDTO resultado = petService.updatePet(petDoUserComum, request);
 
		//assert
		assertEquals(petDoUserComum.getId(), resultado.getId());
		verify(petRepository).save(petDoUserComum);
	}
	
	@Test
	void user_nao_atualiza_pet_de_outro_usuario() {
		//arrange
		when(request.getAttribute("usuarioLogado")).thenReturn(outroUser);
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> petService.updatePet(petDoUserComum, request));
 
		verify(petRepository, never()).save(any());
	}
	
	// void deletePet(Long id, HttpServletRequest request)
	 
	@Test
	void dono_deleta_seu_proprio_pet() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(request.getAttribute("usuarioLogado")).thenReturn(userComum);
 
		//act
		petService.deletePet(100L, request);
 
		//assert
		verify(petRepository).delete(petDoUserComum);
	}
	
	@Test
	void admin_deleta_qualquer_pet() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(request.getAttribute("usuarioLogado")).thenReturn(admin);
 
		//act
		petService.deletePet(100L, request);
 
		//assert
		verify(petRepository).delete(petDoUserComum);
	}
	
	@Test
	void user_nao_deleta_pet_de_outro_usuario() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(request.getAttribute("usuarioLogado")).thenReturn(outroUser);
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> petService.deletePet(100L, request));
 
		verify(petRepository, never()).delete(any());
	}
	
	@Test
	void throw_exception_deleta_id_inexistente() {
		//arrange
		when(petRepository.findById(999L)).thenReturn(Optional.empty());
 
		//act & assert
		assertThrows(RuntimeException.class,
				() -> petService.deletePet(999L, request));
 
		verify(petRepository, never()).delete(any());
	}
	
	
	// void deletePetsUsuario(Usuario usuario)
	 
	@Test
	void deleta_todos_os_pets_do_usuario() {
		//arrange
		Pet outroPetDoMesmoDono = new Pet();
		outroPetDoMesmoDono.setId(102L);
		outroPetDoMesmoDono.setNome("Nemo");
		outroPetDoMesmoDono.setEspecie(Especie.PEIXE);
		outroPetDoMesmoDono.setDono(userComum);
 
		when(petRepository.findByDono_Id(userComum.getId()))
				.thenReturn(Arrays.asList(petDoUserComum, outroPetDoMesmoDono));
 
		//act
		petService.deletePetsUsuario(userComum);
 
		//assert
		verify(petRepository).delete(petDoUserComum);
		verify(petRepository).delete(outroPetDoMesmoDono);
	}
	
	@Test
	void nao_chama_quando_usuario_nao_tem_pets() {
		//arrange
		when(petRepository.findByDono_Id(outroUser.getId())).thenReturn(Arrays.asList());
 
		//act
		petService.deletePetsUsuario(outroUser);
 
		//assert
		verify(petRepository, never()).delete(any());
	}
	
}
