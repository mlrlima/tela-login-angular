package controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import dto.UsuarioRelacionadoDTO;
import dto.UsuarioResponseDTO;
import model.Usuario;
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
	@GetMapping("/all")
	public ResponseEntity<List<UsuarioResponseDTO>> getAll(){
		return ResponseEntity.ok(service.getAllUsuarios());
	}
	
    // ENDPOINT: GET /usuario/{id}
    // FUNCAO: Busca um usuario pelo ID
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 200 com os dados do usuario
	@GetMapping("/{id}")
	public ResponseEntity<UsuarioResponseDTO> getById(@PathVariable Long id) {
    	return ResponseEntity.ok(service.getUsuarioById(id));
	}
    
    @GetMapping
    public UsuarioRelacionadoDTO porEmail(@RequestParam String email) {
        return service.getUsuarioByEmail(email);
    }
	
    
    // ENDPOINT: PUT /usuario
    // FUNCAO: Atualiza um usuario existente
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 200 com o usuario atualizado
	@PutMapping
	public ResponseEntity<UsuarioResponseDTO> update(@RequestBody Usuario usuario) {
    	return ResponseEntity.ok(service.updateUsuario(usuario));  
	}
	
    // ENDPOINT: DELETE /usuario/{id}
    // FUNCAO: Remove um usuario pelo ID
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 204 (No Content)
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
    	service.deleteUsuario(id);
        return ResponseEntity.noContent().build();
    }
	
}