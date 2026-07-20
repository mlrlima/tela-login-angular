package controller;

import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import model.Usuario;
import security.Secured;
import service.UsuarioService;

//CLASSE: UsuarioController
//DESCRICAO: Endpoints para gerenciamento de usuarios (CRUD)
//ROTA BASE: /usuario

@RestController
@RequestMapping("/usuario") // Define a rota base para todos os endpoints
public class UsuarioController {

	@Autowired // Injeta
	private UsuarioService service;

    // ENDPOINT: GET /usuario/all
    // FUNCAO: Lista todos os usuarios (com base no papel/role)
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 200 com lista de usuarios
    @Secured
	@GetMapping("/all")
	public ResponseEntity<List<Usuario>> getAll(HttpServletRequest request){
		return ResponseEntity.ok(service.getAllUsuarios(request));
	}
	
    // ENDPOINT: POST /usuario
    // FUNCAO: Cria um novo usuario (cadastro)
    // RETORNO: HTTP 200 com o usuario criado
	@PostMapping
	public ResponseEntity<Usuario> create(@RequestBody Usuario usuario) { // Recebe JSON
		return ResponseEntity.ok(service.createUsuario(usuario));
	}
	
	
    // ENDPOINT: GET /usuario/{id}
    // FUNCAO: Busca um usuario pelo ID
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 200 com os dados do usuario
    @Secured
	@GetMapping("/{id}")
	public ResponseEntity<Usuario> getById(@PathVariable Long id, HttpServletRequest request) {
    	return ResponseEntity.ok(service.getUsuarioById(id, request));
	}
    
    
    // ENDPOINT: GET /usuario?email=xxx
    // FUNCAO: Busca um usuario pelo email
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 200 com os dados do usuario
    @Secured
	@GetMapping
	public ResponseEntity<Usuario> getByEmail(@RequestParam String email, HttpServletRequest request) {
    	return ResponseEntity.ok(service.getUsuarioByEmail(email, request));  
	}
	
    
    // ENDPOINT: PUT /usuario
    // FUNCAO: Atualiza um usuario existente
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 200 com o usuario atualizado
    @Secured
	@PutMapping
	public ResponseEntity<Usuario> update(@RequestBody Usuario usuario,
									HttpServletRequest request) {
    	return ResponseEntity.ok(service.updateUsuario(usuario, request));  
	}
	
    // ENDPOINT: DELETE /usuario/{id}
    // FUNCAO: Remove um usuario pelo ID
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 204 (No Content)
    @Secured
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id, HttpServletRequest request) {
    	service.deleteUsuario(id, request);
        return ResponseEntity.noContent().build();
    }
	
}