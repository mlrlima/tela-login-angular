package security.controller;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.UsuarioRepository;
import security.DTO.AuthDTO;
import security.DTO.LoginResponseDTO;
import security.DTO.NovoUsuarioDTO;
import security.service.TokenService;

@RestController // Indica que esta classe eh um controller REST (retorna JSON)
@RequestMapping("/auth")
public class AuthController {
	
    @Autowired
    private AuthenticationManager authenticationManager;
    
    @Autowired
    private UsuarioRepository repository;
    
    @Autowired
    private TokenService tokenService;
    
    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletResponse response){
    	
        ResponseCookie cookie= ResponseCookie.from("token", "")
        		.httpOnly(true)
        		.secure(true) 
        		.path("/") 
        		.maxAge(0) 
        		.sameSite("Strict")
        		.build();
		
        response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());
		return ResponseEntity.ok().build(); // HTTP 200 sem corpo
    }

	@PostMapping("/login")
	public ResponseEntity login(@RequestBody @Valid AuthDTO dados, HttpServletResponse response) {
		var emailSenha =new UsernamePasswordAuthenticationToken(dados.email(), dados.senha());
		var auth =this.authenticationManager.authenticate(emailSenha);
		
		Usuario usuario = (Usuario) auth.getPrincipal();
        var token = tokenService.gerarToken(usuario);

        ResponseCookie cookie= ResponseCookie.from("token", token)
        		.httpOnly(true) // JS nao acessa, previne contra XSS
        		.secure(true) 
        		.path("/") // acessivel atraves de todo o dominio
        		.maxAge(Duration.ofMinutes(30)) //dura por 30 min
        		.sameSite("Strict")
        		.build();
        
        response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());
        
        return ResponseEntity.ok(
        		new LoginResponseDTO(
        				usuario.getId(),
        				usuario.getNome(),
        				usuario.getEmail(),
        				usuario.getRole()
        		));
	}
	
    @PostMapping("/usuario")
    public ResponseEntity novoUsuario(@RequestBody @Valid NovoUsuarioDTO dados){
    	// se esse email ja eh cadastrado
        if(this.repository.findByEmail(dados.email()) != null) return ResponseEntity.badRequest().build();
        if(dados.senha().length() < 4) {
        	return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        			.body(Map.of("erro", "A senha deve ter no mínimo 4 caracteres"));
        }
						
        
        //encriptar a senha
        String encryptedPassword = new BCryptPasswordEncoder().encode(dados.senha());
        
        //criar novo usuario
        Usuario novo = new Usuario();
        novo.setId(null);
        novo.setNome(dados.nome());
        novo.setEmail(dados.email());
        novo.setSenha(encryptedPassword);
        novo.setRole(Role.USER);
        novo.setEmpresas(new HashSet<Empresa>());

        this.repository.save(novo);

        return ResponseEntity.ok().build();
    }
}
