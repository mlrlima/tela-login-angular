package service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Optional;

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

import dto.PetResponseDTO;
import exception.GlobalExceptionHandler;
import model.Especie;
import model.Pet;
import model.Role;
import model.Usuario;
import repository.PetRepository;

// OBS: as anotacoes de cache (@Cacheable, @CacheEvict) so funcionam atraves
// do proxy do Spring, entao nao entram nesses testes unitarios

@ExtendWith(MockitoExtension.class)
class PetServiceTest {

	@Mock
	private PetRepository petRepository;

	@InjectMocks
	private PetService petService;

	@Mock
	private SecurityContext securityContext;

	@Mock
	private Authentication authentication;

	private MockedStatic<SecurityContextHolder> securityContextHolderMock;

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


	// boolean isAdminLogado()==============================================================

	@Test
	void isAdminLogado_retorna_true_para_admin() {
		//arrange
		logarComo(admin);

		//act & assert
		assertTrue(petService.isAdminLogado());
	}

	@Test
	void isAdminLogado_retorna_false_para_user_comum() {
		//arrange
		logarComo(userComum);

		//act & assert
		assertFalse(petService.isAdminLogado());
	}


	// Page<PetResponseDTO> getAllPets(Pageable pageable)====================================

	@Test
	void admin_retorna_todos_os_pets_paginado() {
		//arrange
		Pet outroPet = new Pet();
		outroPet.setId(101L);
		outroPet.setNome("Mingau");
		outroPet.setEspecie(Especie.GATO);
		outroPet.setDono(outroUser);

		logarComo(admin);
		Pageable pageable = PageRequest.of(0, 10);
		when(petRepository.findAll(pageable))
				.thenReturn(new PageImpl<>(Arrays.asList(petDoUserComum, outroPet), pageable, 2));

		//act
		Page<PetResponseDTO> resultado = petService.getAllPets(pageable);

		//assert
		assertEquals(2, resultado.getContent().size());
		verify(petRepository).findAll(pageable);
	}

	@Test
	void user_retorna_apenas_seus_proprios_pets_paginado() {
		//arrange
		logarComo(userComum);
		Pageable pageable = PageRequest.of(0, 10);
		when(petRepository.findByDono_Id(pageable, userComum.getId()))
				.thenReturn(new PageImpl<>(Arrays.asList(petDoUserComum), pageable, 1));

		//act
		Page<PetResponseDTO> resultado = petService.getAllPets(pageable);

		//assert
		assertEquals(1, resultado.getContent().size());
		assertEquals(petDoUserComum.getId(), resultado.getContent().get(0).getId());
		verify(petRepository, never()).findAll(any(Pageable.class));
	}

	// PetResponseDTO createPet(Pet pet) ====================================================

	@Test
	void createPet_define_dono_como_logado_e_zera_id() {
		//arrange
		logarComo(userComum);

		Pet novoPet = new Pet();
		novoPet.setId(999L); // deve ser ignorado / zerado pelo service
		novoPet.setNome("Bidu");
		novoPet.setEspecie(Especie.CACHORRO);

		when(petRepository.save(any(Pet.class))).thenAnswer(inv -> inv.getArgument(0));

		//act
		PetResponseDTO resultado = petService.createPet(novoPet);

		//assert
		assertEquals(userComum, novoPet.getDono());
		assertNull(novoPet.getId());
		assertEquals("Bidu", resultado.getNome());
		verify(petRepository).save(novoPet);
	}

	// PetResponseDTO getPetById(Long id)====================================================

	@Test
	void admin_busca_qualquer_pet_por_id() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		logarComo(admin);

		//act
		PetResponseDTO resultado = petService.getPetById(100L);

		//assert
		assertEquals(petDoUserComum.getId(), resultado.getId());
	}

	@Test
	void dono_busca_seu_proprio_pet() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		logarComo(userComum);

		//act
		PetResponseDTO resultado = petService.getPetById(100L);

		//assert
		assertEquals(petDoUserComum.getId(), resultado.getId());
	}

	@Test
	void user_nao_busca_pet_de_outro_usuario() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		logarComo(outroUser);

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> petService.getPetById(100L));
	}

	@Test
	void getPetById_lanca_excecao_quando_nao_encontrado() {
		//arrange
		when(petRepository.findById(999L)).thenReturn(Optional.empty());

		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> petService.getPetById(999L));
	}

	// PetResponseDTO updatePet(Pet pet)=====================================================

	@Test
	void dono_atualiza_seu_proprio_pet() {
		//arrange
		logarComo(userComum);
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(petRepository.save(any(Pet.class))).thenAnswer(inv -> inv.getArgument(0));

		Pet alteracao = new Pet();
		alteracao.setId(100L);
		alteracao.setNome("Rex Editado");
		alteracao.setEspecie(Especie.CACHORRO);
		alteracao.setDono(userComum); // mesmo dono do original

		//act
		PetResponseDTO resultado = petService.updatePet(alteracao);

		//assert
		assertEquals("Rex Editado", resultado.getNome());
		verify(petRepository).save(alteracao);
	}

	@Test
	void admin_atualiza_pet_de_qualquer_usuario() {
		//arrange
		logarComo(admin);
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(petRepository.save(any(Pet.class))).thenAnswer(inv -> inv.getArgument(0));

		Pet alteracao = new Pet();
		alteracao.setId(100L);
		alteracao.setNome("Rex Editado Pelo Admin");
		alteracao.setEspecie(Especie.CACHORRO);
		alteracao.setDono(userComum); // admin nao muda o dono

		//act
		PetResponseDTO resultado = petService.updatePet(alteracao);

		//assert
		assertEquals("Rex Editado Pelo Admin", resultado.getNome());
		verify(petRepository).save(alteracao);
	}

	@Test
	void user_nao_atualiza_pet_de_outro_usuario() {
		//arrange
		logarComo(outroUser);

		Pet alteracao = new Pet();
		alteracao.setId(100L);
		alteracao.setDono(userComum); // outroUser nao eh dono nem admin

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> petService.updatePet(alteracao));

		verify(petRepository, never()).save(any());
	}

	@Test
	void nao_permite_trocar_o_dono_do_pet() {
		//arrange
		logarComo(userComum);
 
		// dono de verdade no banco eh outroUser
		Pet originalNoBanco = new Pet();
		originalNoBanco.setId(100L);
		originalNoBanco.setNome("Rex");
		originalNoBanco.setDono(outroUser);
		when(petRepository.findById(100L)).thenReturn(Optional.of(originalNoBanco));
 
		// mas o payload alega que o dono eh o proprio userComum,
		// so pra passar na checagem inicial de ehDonoOuAdmin
		Pet alteracao = new Pet();
		alteracao.setId(100L);
		alteracao.setNome("Rex Hackeado");
		alteracao.setDono(userComum);
 
		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> petService.updatePet(alteracao));
 
		verify(petRepository, never()).save(any());
	}

	// void deletePet(Long id) ==============================================================

	@Test
	void dono_deleta_seu_proprio_pet() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		logarComo(userComum);

		//act
		petService.deletePet(100L);

		//assert
		verify(petRepository).delete(petDoUserComum);
	}

	@Test
	void admin_deleta_pet_de_qualquer_usuario() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		logarComo(admin);

		//act
		petService.deletePet(100L);

		//assert
		verify(petRepository).delete(petDoUserComum);
	}

	@Test
	void user_nao_deleta_pet_de_outro_usuario() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		logarComo(outroUser);

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> petService.deletePet(100L));

		verify(petRepository, never()).delete(any());
	}

	@Test
	void deletePet_lanca_excecao_quando_nao_encontrado() {
		//arrange
		when(petRepository.findById(999L)).thenReturn(Optional.empty());

		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> petService.deletePet(999L));

		verify(petRepository, never()).delete(any());
	}

	// void deletePetsUsuario(Usuario usuario)===============================================

	@Test
	void deletePetsUsuario_deleta_todos_os_pets_do_usuario() {
		//arrange
		Pet outroPetDoMesmoDono = new Pet();
		outroPetDoMesmoDono.setId(102L);
		outroPetDoMesmoDono.setNome("Nemo");
		outroPetDoMesmoDono.setEspecie(Especie.PEIXE);
		outroPetDoMesmoDono.setDono(userComum);

		when(petRepository.findAllByDono_Id(userComum.getId()))
				.thenReturn(Arrays.asList(petDoUserComum, outroPetDoMesmoDono));

		//act
		petService.deletePetsUsuario(userComum);

		//assert
		verify(petRepository).delete(petDoUserComum);
		verify(petRepository).delete(outroPetDoMesmoDono);
	}

	@Test
	void deletePetsUsuario_nao_deleta_nada_quando_usuario_nao_tem_pets() {
		//arrange
		when(petRepository.findAllByDono_Id(outroUser.getId())).thenReturn(Arrays.asList());

		//act
		petService.deletePetsUsuario(outroUser);

		//assert
		verify(petRepository, never()).delete(any());
	}

	// PetResponseDTO atualizarLocalizacao(Long id, Double latitude, Double longitude)=======

	@Test
	void dono_atualiza_localizacao_do_seu_pet() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		when(petRepository.save(any(Pet.class))).thenAnswer(inv -> inv.getArgument(0));
		logarComo(userComum);

		//act
		petService.atualizarLocalizacao(100L, -8.05, -34.9);

		//assert
		assertEquals(-8.05, petDoUserComum.getLatitude());
		assertEquals(-34.9, petDoUserComum.getLongitude());
		verify(petRepository).save(petDoUserComum);
	}

	@Test
	void user_nao_atualiza_localizacao_de_pet_de_outro_usuario() {
		//arrange
		when(petRepository.findById(100L)).thenReturn(Optional.of(petDoUserComum));
		logarComo(outroUser);

		//act & assert
		assertThrows(GlobalExceptionHandler.UnauthorizedException.class,
				() -> petService.atualizarLocalizacao(100L, -8.05, -34.9));

		verify(petRepository, never()).save(any());
	}

	@Test
	void atualizarLocalizacao_lanca_excecao_quando_pet_nao_encontrado() {
		//arrange
		when(petRepository.findById(999L)).thenReturn(Optional.empty());

		//act & assert
		assertThrows(GlobalExceptionHandler.ResourceNotFoundException.class,
				() -> petService.atualizarLocalizacao(999L, -8.05, -34.9));
	}
}