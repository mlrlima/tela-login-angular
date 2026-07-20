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
	public List<Pet> getAllPets(HttpServletRequest request){
		Usuario usuarioLogado = logado(request);
		
		if(usuarioLogado.getRole() == Role.ADMIN) {
			return petRepository.findAll();
		}
		
		return petRepository.findByDono_Id(usuarioLogado.getId());
	}

    // METODO: createPet()
    // FUNCAO: Cria um novo pet associado ao usuario logado
    // REGRA: O dono do pet eh sempre o usuario logado
	@Transactional
	public Pet createPet(Pet pet, HttpServletRequest request) {
		
		Usuario usuarioLogado = logado(request);
        pet.setDono(usuarioLogado); // Forca o dono ser o usuario logado
		
		pet.setId(null); // Garante que o ID seja gerado pelo banco
		return petRepository.save(pet);
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
	public Pet getPetById(Long id, HttpServletRequest request) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado(request);
		if(ehDonoOuAdmin(usuarioLogado, alvo)) return alvo;
		
		throw new RuntimeException("Sem permissão");
	}

    // METODO: updatePet()
    // FUNCAO: Atualiza um pet existente
    // REGRA: ADMIN ou dono do pet podem atualizar
	@Transactional
	public Pet updatePet(Pet pet,HttpServletRequest request) {
		Usuario usuarioLogado = logado(request);
		if(!ehDonoOuAdmin(usuarioLogado, pet)) throw new RuntimeException("Sem permissão");
		
		return petRepository.save(pet);
	}
	
    // METODO: deletePet()
    // FUNCAO: Remove um pet pelo ID
    // REGRA: ADMIN ou dono do pet podem deletar
	@Transactional
	public void deletePet(Long id, HttpServletRequest request) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado(request);
		if(!ehDonoOuAdmin(usuarioLogado, alvo)) throw new RuntimeException("Sem permissão");

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
}
	