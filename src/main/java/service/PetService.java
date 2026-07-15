package service;

import java.io.Serializable;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import model.Pet;
import model.Role;
import model.Usuario;
import repository.PetRepository;

@Service
public class PetService implements Serializable {
	private static final long serialVersionUID=1L;

	@Autowired
	private PetRepository petRepository;

    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }

	public List<Pet> getAllPets(HttpServletRequest request){
		Usuario usuarioLogado = logado(request);
		
		if(usuarioLogado.getRole() == Role.ADMIN) {
			return petRepository.findAll();
		}
		
		return petRepository.findByDono_Id(usuarioLogado.getId());
	}

	@Transactional
	public Pet createPet(Pet pet, HttpServletRequest request) {
		
		Usuario usuarioLogado = logado(request);
        pet.setDono(usuarioLogado);
		
		pet.setId(null);
		return petRepository.save(pet);
	}
	
    private boolean ehDonoOuAdmin(Usuario logado, Pet pet) {
        return logado.getRole() == Role.ADMIN ||
               pet.getDono().getId().equals(logado.getId());
    }

	public Pet getPetById(Long id, HttpServletRequest request) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado(request);
		if(ehDonoOuAdmin(usuarioLogado, alvo)) return alvo;
		
		throw new RuntimeException("Sem permissão");
	}

	@Transactional
	public Pet updatePet(Pet pet,HttpServletRequest request) {
		Usuario usuarioLogado = logado(request);
		if(!ehDonoOuAdmin(usuarioLogado, pet)) throw new RuntimeException("Sem permissão");
		
		return petRepository.save(pet);
	}
	

	@Transactional
	public void deletePet(Long id, HttpServletRequest request) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado(request);
		if(!ehDonoOuAdmin(usuarioLogado, alvo)) throw new RuntimeException("Sem permissão");

		petRepository.delete(alvo);
	}
	
	@Transactional
	public void deletePetsUsuario(Usuario usuario) {
		List<Pet> lista=petRepository.findByDono_Id(usuario.getId());
		
		for (Pet pet:lista) {
			petRepository.delete(pet);
		}
	}
}
	