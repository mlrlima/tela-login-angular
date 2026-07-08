package controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import model.Role;
import model.Usuario;
import repository.Usuarios;
import security.Secured;
import service.CadastroUsuarioService;

@RestController
@RequestMapping("/usuario")
public class UsuarioController {

    private final Usuarios usuarios;
    private final CadastroUsuarioService cadastroUsuarioService;

    public UsuarioController(Usuarios usuarios,
                             CadastroUsuarioService cadastroUsuarioService) {
        this.usuarios = usuarios;
        this.cadastroUsuarioService = cadastroUsuarioService;
    }

    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }

    @PostMapping
    public ResponseEntity<Usuario> guardarUsuario(@RequestBody Usuario usuario) {

        usuario.setId(null);
        usuario.setRole(Role.USER);

        cadastroUsuarioService.salvar(usuario);

        return ResponseEntity.ok(usuario);
    }

    @Secured
    @GetMapping("/{id}")
    public ResponseEntity<?> porId(@PathVariable Long id,
                                   HttpServletRequest request) {

        Usuario usuarioLogado = logado(request);
        Usuario alvo = usuarios.porId(id);

        if (alvo == null) {
            return ResponseEntity.notFound().build();
        }

        if (usuarioLogado.getRole() != Role.ADMIN &&
            !usuarioLogado.getId().equals(alvo.getId())) {

            return ResponseEntity.status(403)
                    .body(Map.of("erro", "Sem permissao"));
        }

        return ResponseEntity.ok(alvo);
    }

    @Secured
    @GetMapping
    public ResponseEntity<?> porEmail(@RequestParam String email,
                                      HttpServletRequest request) {

        Usuario usuarioLogado = logado(request);
        Usuario alvo = usuarios.porEmail(email);

        if (alvo == null) {
            return ResponseEntity.notFound().build();
        }

        if (usuarioLogado.getRole() != Role.ADMIN &&
            !usuarioLogado.getId().equals(alvo.getId())) {

            return ResponseEntity.status(403)
                    .body(Map.of("erro", "Sem permissao"));
        }

        return ResponseEntity.ok(alvo);
    }

    @Secured(Role.ADMIN)
    @GetMapping("/all")
    public ResponseEntity<List<Usuario>> todos() {
        return ResponseEntity.ok(usuarios.todos());
    }

    @Secured
    @DeleteMapping("/{id}")
    public ResponseEntity<?> remover(@PathVariable Long id,
                                     HttpServletRequest request) {

        Usuario usuarioLogado = logado(request);

        if (usuarioLogado.getRole() != Role.ADMIN &&
            !usuarioLogado.getId().equals(id)) {

            return ResponseEntity.status(403)
                    .body(Map.of("erro", "Sem permissao"));
        }

        Usuario usuario = usuarios.porId(id);

        if (usuario == null) {
            return ResponseEntity.notFound().build();
        }

        cadastroUsuarioService.excluir(usuario);

        return ResponseEntity.ok().build();
    }

    @Secured
    @PutMapping
    public ResponseEntity<?> atualizar(@RequestBody Usuario usuario,
                                       HttpServletRequest request) {

        Usuario usuarioLogado = logado(request);

        if (usuarioLogado.getRole() != Role.ADMIN &&
            !usuarioLogado.getId().equals(usuario.getId())) {

            return ResponseEntity.status(403)
                    .body(Map.of("erro", "Sem permissao"));
        }

        if (usuarioLogado.getRole() != Role.ADMIN) {
            usuario.setRole(Role.USER);
        }

        if (usuario.getSenha() == null || usuario.getSenha().isBlank()) {
            Usuario existente = usuarios.porId(usuario.getId());

            if (existente == null) {
                return ResponseEntity.notFound().build();
            }

            usuario.setSenha(existente.getSenha());
        }

        cadastroUsuarioService.salvar(usuario);

        return ResponseEntity.ok(usuario);
    }

}