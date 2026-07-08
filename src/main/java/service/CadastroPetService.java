package service;

import java.io.Serializable;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import model.Pet;
import repository.Pets;

@Service
public class CadastroPetService implements Serializable {
	private static final long serialVersionUID=1L;
	
	private final Pets pets;
	
    public CadastroPetService(Pets pets) {
        this.pets = pets;
    }
	
	@Transactional
	public void salvar(Pet pet) {
		pets.guardar(pet);
	}
	
	@Transactional
	public void excluir(Pet pet) {
		pets.remover(pet);
	}
}
