package service;

import java.io.Serializable;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;

import dto.DonoDTO;
import dto.PetResponseDTO;
import exception.GlobalExceptionHandler;
import model.Pet;
import model.Role;
import model.Usuario;
import repository.PetRepository;

@Service
public class PetService implements Serializable {
	private static final long serialVersionUID=1L;

	@Autowired
	private PetRepository petRepository;
	
	private Usuario logado() {
	    var auth = SecurityContextHolder.getContext().getAuthentication();
	    if (auth == null || !auth.isAuthenticated()) return null;
	    
	    return (Usuario) auth.getPrincipal();
	}
	
	
	public boolean isAdminLogado() {
	    Usuario usuarioLogado = logado();
	    return usuarioLogado != null && usuarioLogado.getRole() == Role.ADMIN;
	}

	// retorna a quantidade de tempo em segundos que o script vai mover o pet pelo mapa
	public int getIntervaloMover(@PathVariable Long id) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado();
		if(ehDonoOuAdmin(usuarioLogado, alvo)) return alvo.getIntervaloMover();
		
		throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}
	
	@Cacheable(value = "pets", key = "#pageable.pageNumber + '-' + #pageable.pageSize",
			condition = "#root.target.isAdminLogado()") //cache por pagina
	public Page<PetResponseDTO> getAllPets(Pageable pageable){
		Usuario usuarioLogado = logado();
		
		Page<Pet> pets;
		
		//se for ADMIN, retorna todos os pets
		if(usuarioLogado.getRole() == Role.ADMIN) {
			pets = petRepository.findAll(pageable);
		} else { //se for user, retorna apenas os dele
			pets = petRepository.findByDono_Id(pageable, usuarioLogado.getId());
		}
		
		return pets.map(this::toDTO);
	}

	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public PetResponseDTO createPet(Pet pet) {
		Usuario usuarioLogado = logado();
        pet.setDono(usuarioLogado); // o dono do pet sempre será automaticamente o usuario que criou ele
        // nao é possivel alterar o dono de um pet
		
		pet.setId(null);
		
		Pet salvo = petRepository.save(pet);
		return toDTO(salvo);
	}
	
    private boolean ehDonoOuAdmin(Usuario logado, Pet pet) {
        return logado.getRole() == Role.ADMIN ||
               pet.getDono().getId().equals(logado.getId());
    }

	public PetResponseDTO getPetById(Long id) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado();
		if(ehDonoOuAdmin(usuarioLogado, alvo)) return toDTO(alvo);
		
		throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}

	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public PetResponseDTO updatePet(Pet pet) {
		Usuario usuarioLogado = logado();
		if(!ehDonoOuAdmin(usuarioLogado, pet)) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		
		Pet original=petRepository.findById(pet.getId())
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		if(!original.getDono().equals(pet.getDono())) {
			throw new GlobalExceptionHandler.UnauthorizedException("Não é permitido alterar o dono de um pet.");
		}
		
		Pet salvo = petRepository.save(pet);
		return toDTO(salvo);
	}
	
	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public void deletePet(Long id) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado();
		if(!ehDonoOuAdmin(usuarioLogado, alvo)) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");

		petRepository.delete(alvo);
	}
	
    // FUNCAO: Remove TODOS os pets de um usuario (usado ao deletar usuario)
    // Chamado internamente pelo UsuarioService
	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public void deletePetsUsuario(Usuario usuario) {
		List<Pet> lista=petRepository.findAllByDono_Id(usuario.getId());
		
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
				dono,
		        pet.getLatitude(), pet.getLongitude(),
		        pet.getIntervaloMover(),
		        pet.getDataNascimento(),
		        pet.getPeso()
		);
	}
	
	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public PetResponseDTO atualizarLocalizacao(Long id, Double latitude, Double longitude) {
	    Pet alvo = petRepository.findById(id)
	            .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));

	    Usuario usuarioLogado = logado();
	    if (!ehDonoOuAdmin(usuarioLogado, alvo)) {
	        throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	    }

	    alvo.setLatitude(latitude);
	    alvo.setLongitude(longitude);
	    
	    Pet salvo = petRepository.save(alvo);
	    return toDTO(salvo);
	}
}
	