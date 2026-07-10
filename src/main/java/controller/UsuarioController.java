package controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import model.Role;
import model.Usuario;
import security.Secured;
import service.UsuarioService;

@RestController
@RequestMapping("/usuario")
public class UsuarioController {

	@Autowired
	private UsuarioService service;

    @Secured
	@GetMapping("/all")
	public ResponseEntity<List<Usuario>> getAll(HttpServletRequest request){
		return ResponseEntity.ok(service.getAllUsuarios(request));
	}
	
	@PostMapping
	public ResponseEntity<Usuario> create(@RequestBody Usuario usuario) {
		return ResponseEntity.ok(service.createUsuario(usuario));
	}
	
    @Secured
	@GetMapping("/{id}")
	public ResponseEntity<Usuario> getById(@PathVariable Long id, HttpServletRequest request) {
    	return ResponseEntity.ok(service.getUsuarioById(id, request));
	}
    
    @Secured
	@GetMapping
	public ResponseEntity<Usuario> getByEmail(@RequestParam String email, HttpServletRequest request) {
    	return ResponseEntity.ok(service.getUsuarioByEmail(email, request));  
	}
	
    @Secured
	@PutMapping
	public ResponseEntity<Usuario> update(@RequestBody Usuario usuario,
									HttpServletRequest request) {
    	return ResponseEntity.ok(service.updateUsuario(usuario, request));  
	}
	
    @Secured
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id, HttpServletRequest request) {
    	service.deleteUsuario(id, request);
        return ResponseEntity.noContent().build();
    }
	
}