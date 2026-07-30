package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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

	
	@GetMapping("/all")
	public ResponseEntity<Page<UsuarioResponseDTO>> getAllUsuarios( //com paginacao
					        @RequestParam(defaultValue = "0") int page,
					        @RequestParam(defaultValue = "10") int size) {

	    Pageable pageable = PageRequest.of(page, size);
	    Page<UsuarioResponseDTO> usuarios = service.getAllUsuarios(pageable);
	    return ResponseEntity.ok(usuarios);
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