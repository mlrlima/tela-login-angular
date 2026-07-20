package controller;

import java.util.HashMap;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import model.Usuario;
import security.GeradorToken;
import security.Secured;
import service.UsuarioService;

//CLASSE: AuthController
//DESCRICAO: Endpoints de autenticacao (login, logout, perfil)
//ROTA BASE: /auth

@RestController // Indica que esta classe eh um controller REST (retorna JSON)
@RequestMapping("/auth") // Define a rota base para todos os endpoints
public class AuthController {

	private final UsuarioService usuarioService;

    public AuthController(UsuarioService usuarioService) {
        this.usuarioService = usuarioService;
        //System.out.println(">>> AuthController carregado!");
    }

    // CLASSE INTERNA: LoginRequest
    // DESCRICAO: DTO (Data Transfer Object) para receber credenciais
	public static class LoginRequest{
		public String email;
		public String senha;
	}
	
	
    // ENDPOINT: POST /auth/login
    // FUNCAO: Autentica o usuario e retorna um token
    // RETORNO: HTTP 200 com token + dados do usuario
    //          HTTP 401 se credenciais sao invalidas
	@PostMapping("/login")
	public ResponseEntity<?> login(@RequestBody LoginRequest req){ // Recebe JSON no corpo da requisicao
		Usuario usuario=usuarioService.getUsuarioByEmailAndSenha(req.email, req.senha);
		
		if(usuario==null) { // Credenciais invalidas
			return ResponseEntity.status(401).body(Map.of("erro", "Email ou senha invalidos"));
		}
		
		// Gera o token para o usuario autenticado
		String token=GeradorToken.gerar(usuario);
		
		// Monta a resposta com token e dados do usuario
		Map<String, Object> resposta = new HashMap<>();
        resposta.put("token", token);
        resposta.put("id", usuario.getId());
        resposta.put("nome", usuario.getNome());
        resposta.put("email", usuario.getEmail());
        resposta.put("role", usuario.getRole());
        
        return ResponseEntity.ok(resposta); // HTTP 200
	}
	
	
    // ENDPOINT: POST /auth/logout
    // FUNCAO: Invalida o token (adiciona a blacklist)
    // RETORNO: HTTP 200
	@PostMapping("/logout")
	public ResponseEntity<Void> logout(@RequestHeader(value = "Authorization", required = false) String auth){
		// Verifica se o cabecalho Authorization existe e eh Bearer token
		if(auth!=null && auth.startsWith("Bearer ")) {
			GeradorToken.invalidar(auth.substring(7)); // Remove "Bearer " e invalida o token
		}
		
		return ResponseEntity.ok().build(); // HTTP 200 sem corpo
	}
	
	
    // ENDPOINT: GET /auth/me
    // FUNCAO: Retorna os dados do usuario logado (obtido do request)
    // SEGURANCA: Requer autenticacao (@Secured)
    // RETORNO: HTTP 200 com os dados do usuario
	@Secured // Anotacao personalizada
	@GetMapping("/me")
	public ResponseEntity<Usuario> me(HttpServletRequest request){
		// O usuario logado foi adicionado ao request pelo AuthInterceptor
		return ResponseEntity.ok((Usuario) request.getAttribute("usuarioLogado"));
	}

}
