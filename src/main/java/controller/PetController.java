package controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.*;

import dto.LocalizacaoPetDTO;
import dto.PetResponseDTO;
import jakarta.validation.Valid;
import model.Pet;
import service.PetService;

//CLASSE: PetController
//DESCRICAO: Endpoints para gerenciamento de pets (CRUD)
//ROTA BASE: /pet
//SEGURANCA: TODOS os endpoints requerem autenticacao (@Secured na classe)

@RestController
@RequestMapping("/pet")// Define a rota base para todos os endpoints
public class PetController {
	
	@Autowired //injeta
	private PetService service;
	
	@Autowired
	private SimpMessagingTemplate messagingTemplate;

	
	// ENDPOINT: PUT /pet/{id}/localizacao
	// FUNCAO: Atualiza a posicao do pet e notifica quem esta com o mapa aberto
	@PutMapping("/{id}/localizacao")
	public ResponseEntity<PetResponseDTO> atualizarLocalizacao(@PathVariable Long id,
	        			@RequestBody @Valid LocalizacaoPetDTO body) {

	    PetResponseDTO atualizado = service.atualizarLocalizacao(id, body.getLatitude(), body.getLongitude());
	    
	    messagingTemplate.convertAndSend("/topic/pet-location", atualizado);
	    return ResponseEntity.ok(atualizado);
	}

   
	@GetMapping("/all")
	public ResponseEntity<Page<PetResponseDTO>> getAll(
			 						@RequestParam(defaultValue = "0") int page,
			 						@RequestParam(defaultValue = "10") int size){
		
		Pageable pageable = PageRequest.of(page, size);
	    Page<PetResponseDTO> pets = service.getAllPets(pageable);
	    return ResponseEntity.ok(pets);
	}
	
	// ENDPOINT: POST /pet
    // FUNCAO: Cria um novo pet (associado ao usuario logado)
    // RETORNO: HTTP 200 com o pet criado
	@PostMapping
	public ResponseEntity<PetResponseDTO> create(@RequestBody Pet pet) {
		return ResponseEntity.ok(service.createPet(pet));
	}
	
    // ENDPOINT: GET /pet/{id}
    // FUNCAO: Busca um pet pelo ID (verifica se pertence ao usuario)
    // RETORNO: HTTP 200 com os dados do pet
	@GetMapping("/{id}")
	public ResponseEntity<PetResponseDTO> getById(@PathVariable Long id) {
		return ResponseEntity.ok(service.getPetById(id));
	}

    // ENDPOINT: PUT /pet
    // FUNCAO: Atualiza um pet existente
    // RETORNO: HTTP 200 com o pet atualizado
	@PutMapping
	public ResponseEntity<PetResponseDTO> update(@RequestBody Pet pet) {
		return ResponseEntity.ok(service.updatePet(pet));
	}
	
    // ENDPOINT: DELETE /pet/{id}
    // FUNCAO: Remove um pet pelo ID
    // RETORNO: HTTP 204 (No Content)
	@DeleteMapping("/{id}")
	public ResponseEntity<Void> deletePet(@PathVariable Long id) {
		service.deletePet(id);
        return ResponseEntity.noContent().build();
	}
}