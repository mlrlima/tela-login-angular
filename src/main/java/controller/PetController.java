package controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import dto.LocalizacaoPetDTO;
import dto.PetResponseDTO;
import model.Pet;
import service.PetService;
import websocket.service.PetLocationBuffer;


@RestController
@RequestMapping("/pet")// Define a rota base para todos os endpoints
public class PetController {
	
	@Autowired //injeta
	private PetService service;

	@Autowired
	private PetLocationBuffer locationBuffer;
	
	// Atualiza a posicao do pet e notifica quem esta com o mapa aberto
	@PutMapping("/{id}/localizacao")
	public ResponseEntity<PetResponseDTO> atualizarLocalizacao(
	        @PathVariable Long id,
	        @RequestBody @jakarta.validation.Valid LocalizacaoPetDTO body) {

	    PetResponseDTO atualizado = service.atualizarLocalizacao(id, body.getLatitude(), body.getLongitude());
	    locationBuffer.adicionar(atualizado);   // so acumula; quem manda de fato eh o @Scheduled
	    return ResponseEntity.ok(atualizado);
	}
	
	@GetMapping("/{id}/intervalo")
	public ResponseEntity<Integer> getIntervaloMover(@PathVariable Long id){
		return ResponseEntity.ok(service.getIntervaloMover(id));
	}

   
	@GetMapping("/all")
	public ResponseEntity<Page<PetResponseDTO>> getAll(
			 						@RequestParam(defaultValue = "0") int page,
			 						@RequestParam(defaultValue = "10") int size){
		
		Pageable pageable = PageRequest.of(page, size);
	    Page<PetResponseDTO> pets = service.getAllPets(pageable);
	    return ResponseEntity.ok(pets);
	}
	
	@PostMapping
	public ResponseEntity<PetResponseDTO> create(@RequestBody Pet pet) {
		return ResponseEntity.ok(service.createPet(pet)); //HTTP 200 com o pet criado
	}
	
	@GetMapping("/{id}")
	public ResponseEntity<PetResponseDTO> getById(@PathVariable Long id) {
		return ResponseEntity.ok(service.getPetById(id)); // HTTP 200 com os dados do pet
	}

	@PutMapping
	public ResponseEntity<PetResponseDTO> update(@RequestBody Pet pet) {
		return ResponseEntity.ok(service.updatePet(pet)); //HTTP 200 com o pet atualizado
	}
	
	@DeleteMapping("/{id}")
	public ResponseEntity<Void> deletePet(@PathVariable Long id) {
		service.deletePet(id);
        return ResponseEntity.noContent().build(); //HTTP 204 (No Content)
	}
}