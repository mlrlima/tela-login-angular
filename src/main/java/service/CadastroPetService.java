package service;

import java.io.Serializable;

import javax.inject.Inject;

import model.Pet;
import repository.Pets;
import util.Transacional;

public class CadastroPetService implements Serializable {
	private final long seriaVersionUID=1L;
	
	@Inject
	private Pets pets;
	
	@Transacional
	public void salvar(Pet pet) {
		pets.guardar(pet);
	}
	
	@Transacional
	public void excluir(Pet pet) {
		pets.remover(pet);
	}
}
