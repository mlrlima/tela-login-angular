package controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import model.Pet;
import security.Secured;
import service.PetService;

@RestController
@RequestMapping("/pet")
@Secured
public class PetController {
	
	@Autowired
	private PetService service;

	@GetMapping("/all")
	public ResponseEntity<List<Pet>> getAll(HttpServletRequest request){
		return ResponseEntity.ok(service.getAllPets(request));
	}
	
	@PostMapping
	public ResponseEntity<Pet> create(@RequestBody Pet pet, HttpServletRequest request) {
		return ResponseEntity.ok(service.createPet(pet, request));
	}
	
	@GetMapping("/{id}")
	public ResponseEntity<Pet> getById(@PathVariable Long id, HttpServletRequest request) {
		return ResponseEntity.ok(service.getPetById(id, request));
	}

    
	@PutMapping
	public ResponseEntity<Pet> update(@RequestBody Pet pet,HttpServletRequest request) {
		return ResponseEntity.ok(service.updatePet(pet, request));
	}
	
	@DeleteMapping("/{id}")
	public ResponseEntity<Void> deletePet(@PathVariable Long id, HttpServletRequest request) {
		service.deletePet(id, request);
        return ResponseEntity.noContent().build();
	}
}