package service;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dto.DonoDTO;
import dto.PetResponseDTO;
import exception.GlobalExceptionHandler;
import model.Pet;
import model.Role;
import model.Usuario;
import repository.PetRepository;

//CLASSE: PetService
//DESCRICAO: Camada de servico para gerenciamento de pets
//FUNCAO: Regras de negocio, validacoes e controle de permissao
@Service
public class PetService implements Serializable {
	private static final long serialVersionUID=1L;

	@Autowired
	private PetRepository petRepository;

    // METODO: logado()
    // FUNCAO: Obtem o usuario logado a partir do request
    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }

    // METODO: getAllPets()
    // FUNCAO: Lista pets baseado na role do usuario logado
    // REGRA: ADMIN ve todos | USER ve apenas seus proprios pets
	public List<PetResponseDTO> getAllPets(HttpServletRequest request){
		Usuario usuarioLogado = logado(request);
		
		List<Pet> pets;
		
		//se for ADMIN, retorna todos os pets
		if(usuarioLogado.getRole() == Role.ADMIN) {
			pets = petRepository.findAll();
		} else { //se for user, retorna apenas os dele
			pets = petRepository.findByDono_Id(usuarioLogado.getId());
		}
		
		return pets.stream()
				.map(this::toDTO)
				.collect(Collectors.toList());
	}

    // METODO: createPet()
    // FUNCAO: Cria um novo pet associado ao usuario logado
    // REGRA: O dono do pet eh sempre o usuario logado
	@Transactional
	public PetResponseDTO createPet(Pet pet, HttpServletRequest request) {
		Usuario usuarioLogado = logado(request);
        pet.setDono(usuarioLogado);
		
		pet.setId(null);
		Pet salvo = petRepository.save(pet);
		return toDTO(salvo);
	}
	
    // METODO: ehDonoOuAdmin()
    // FUNCAO: Verifica se o usuario eh dono do pet ou ADMIN
    // RETORNO: true se tiver permissao, false caso contrario
    private boolean ehDonoOuAdmin(Usuario logado, Pet pet) {
        return logado.getRole() == Role.ADMIN ||
               pet.getDono().getId().equals(logado.getId());
    }

    // METODO: getPetById()
    // FUNCAO: Busca pet por ID com verificacao de permissao
    // REGRA: ADMIN ou dono do pet podem acessar
	public PetResponseDTO getPetById(Long id, HttpServletRequest request) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado(request);
		if(ehDonoOuAdmin(usuarioLogado, alvo)) return toDTO(alvo);
		
		throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}

    // METODO: updatePet()
    // FUNCAO: Atualiza um pet existente
    // REGRA: ADMIN ou dono do pet podem atualizar
	@Transactional
	public PetResponseDTO updatePet(Pet pet, HttpServletRequest request) {
		Usuario usuarioLogado = logado(request);
		if(!ehDonoOuAdmin(usuarioLogado, pet)) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		
		Pet salvo = petRepository.save(pet);
		return toDTO(salvo);
	}
	
    // METODO: deletePet()
    // FUNCAO: Remove um pet pelo ID
    // REGRA: ADMIN ou dono do pet podem deletar
	@Transactional
	public void deletePet(Long id, HttpServletRequest request) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado(request);
		if(!ehDonoOuAdmin(usuarioLogado, alvo)) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");

		petRepository.delete(alvo);
	}
	
    // METODO: deletePetsUsuario()
    // FUNCAO: Remove TODOS os pets de um usuario (usado ao deletar usuario)
    // REGRA: Chamado internamente pelo UsuarioService
	@Transactional
	public void deletePetsUsuario(Usuario usuario) {
		List<Pet> lista=petRepository.findByDono_Id(usuario.getId());
		
		// Deleta cada pet individualmente
		for (Pet pet:lista) {
			petRepository.delete(pet);
		}
	}
	
	// Converte a entidade em DTO, sem loop e sem expor o Usuario completo
	private PetResponseDTO toDTO(Pet pet) {
		DonoDTO dono = new DonoDTO(
				pet.getDono().getId(),
				pet.getDono().getEmail()
		);
		
		return new PetResponseDTO(
				pet.getId(),
				pet.getNome(),
				pet.getEspecie(),
				dono
		);
	}
}
	