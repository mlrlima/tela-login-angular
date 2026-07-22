package controller;

import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import dto.PetResponseDTO;
import model.Pet;
import security.Secured;
import service.PetService;

//CLASSE: PetController
//DESCRICAO: Endpoints para gerenciamento de pets (CRUD)
//ROTA BASE: /pet
//SEGURANCA: TODOS os endpoints requerem autenticacao (@Secured na classe)

@RestController
@RequestMapping("/pet")// Define a rota base para todos os endpoints
@Secured // Aplica autenticacao a TODOS os endpoints desta classe
public class PetController {
	
	@Autowired //injeta
	private PetService service;

    // ENDPOINT: GET /pet/all
    // FUNCAO: Lista todos os pets do usuario logado
    // RETORNO: HTTP 200 com lista de pets
	@GetMapping("/all")
	public ResponseEntity<List<PetResponseDTO>> getAll(HttpServletRequest request){
		return ResponseEntity.ok(service.getAllPets(request));
	}
	
	// ENDPOINT: POST /pet
    // FUNCAO: Cria um novo pet (associado ao usuario logado)
    // RETORNO: HTTP 200 com o pet criado
	@PostMapping
	public ResponseEntity<PetResponseDTO> create(@RequestBody Pet pet, HttpServletRequest request) {
		return ResponseEntity.ok(service.createPet(pet, request));
	}
	
    // ENDPOINT: GET /pet/{id}
    // FUNCAO: Busca um pet pelo ID (verifica se pertence ao usuario)
    // RETORNO: HTTP 200 com os dados do pet
	@GetMapping("/{id}")
	public ResponseEntity<PetResponseDTO> getById(@PathVariable Long id, HttpServletRequest request) {
		return ResponseEntity.ok(service.getPetById(id, request));
	}

    // ENDPOINT: PUT /pet
    // FUNCAO: Atualiza um pet existente
    // RETORNO: HTTP 200 com o pet atualizado
	@PutMapping
	public ResponseEntity<PetResponseDTO> update(@RequestBody Pet pet,HttpServletRequest request) {
		return ResponseEntity.ok(service.updatePet(pet, request));
	}
	
    // ENDPOINT: DELETE /pet/{id}
    // FUNCAO: Remove um pet pelo ID
    // RETORNO: HTTP 204 (No Content)
	@DeleteMapping("/{id}")
	public ResponseEntity<Void> deletePet(@PathVariable Long id, HttpServletRequest request) {
		service.deletePet(id, request);
        return ResponseEntity.noContent().build();
	}
}