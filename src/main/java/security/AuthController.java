package security;

import java.util.HashSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.UsuarioRepository;

@RestController // Indica que esta classe eh um controller REST (retorna JSON)
@RequestMapping("/auth")
public class AuthController {
	
    @Autowired
    private AuthenticationManager authenticationManager;
    
    @Autowired
    private UsuarioRepository repository;
    
    @Autowired
    private TokenService tokenService;

	@PostMapping("/login")
	public ResponseEntity(@RequestBody @Valid AuthDTO dados) {
		var emailSenha =new UsernamePasswordAuthenticationToken(dados.email(), dados.senha());
		var auth =this.authenticationManager.authenticate(emailSenha);
		
        var token = tokenService.generateToken((User) auth.getPrincipal());

        return ResponseEntity.ok(new LoginResponseDTO(token));
	}
	
    @PostMapping("/usuario")
    public ResponseEntity novoUsuario(@RequestBody @Valid NovoUsuarioDTO dados){
    	// se esse email ja eh cadastrado
        if(this.repository.findByEmail(dados.email()) != null) return ResponseEntity.badRequest().build();

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
