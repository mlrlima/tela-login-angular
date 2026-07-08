package controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import model.Pet;
import model.Role;
import model.Usuario;
import repository.Pets;
import security.Secured;
import service.CadastroPetService;

@RestController
@RequestMapping("/pet")
@Secured
public class PetController {

    private final Pets pets;
    private final CadastroPetService cadastroPetService;

    public PetController(Pets pets, CadastroPetService cadastroPetService) {
        this.pets = pets;
        this.cadastroPetService = cadastroPetService;
    }

    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }

    private boolean donoOuAdmin(Usuario logado, Pet pet) {
        return logado.getRole() == Role.ADMIN ||
               pet.getDono().getId().equals(logado.getId());
    }

    @GetMapping
    public ResponseEntity<List<Pet>> todos(HttpServletRequest request) {

        Usuario usuarioLogado = logado(request);

        List<Pet> lista = pets.todos();

        if (usuarioLogado.getRole() != Role.ADMIN) {
            lista = pets.filtrarPetsDoUsuario(lista, usuarioLogado);
        }

        return ResponseEntity.ok(lista);
    }

    @PostMapping
    public ResponseEntity<?> guardarPet(@RequestBody Pet pet,
                                        HttpServletRequest request) {

        Usuario usuarioLogado = logado(request);

        if (usuarioLogado.getRole() != Role.ADMIN) {
            pet.setDono(usuarioLogado);
        }

        pet.setId(null);

        cadastroPetService.salvar(pet);

        return ResponseEntity.ok(pet);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> porId(@PathVariable Long id,
                                   HttpServletRequest request) {

        Pet pet = pets.porId(id);

        if (pet == null) {
            return ResponseEntity.notFound().build();
        }

        if (!donoOuAdmin(logado(request), pet)) {
            return ResponseEntity.status(403)
                    .body(Map.of("erro", "Sem permissao"));
        }

        return ResponseEntity.ok(pet);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remover(@PathVariable Long id,
                                     HttpServletRequest request) {

        Pet pet = pets.porId(id);

        if (pet == null) {
            return ResponseEntity.notFound().build();
        }

        if (!donoOuAdmin(logado(request), pet)) {
            return ResponseEntity.status(403)
                    .body(Map.of("erro", "Sem permissao"));
        }

        cadastroPetService.excluir(pet);

        return ResponseEntity.ok().build();
    }

    @PutMapping
    public ResponseEntity<?> atualizar(@RequestBody Pet pet,
                                       HttpServletRequest request) {

        Pet existente = pets.porId(pet.getId());

        if (existente == null) {
            return ResponseEntity.notFound().build();
        }

        Usuario usuarioLogado = logado(request);

        if (!donoOuAdmin(usuarioLogado, existente)) {
            return ResponseEntity.status(403)
                    .body(Map.of("erro", "Sem permissao"));
        }

        if (usuarioLogado.getRole() != Role.ADMIN) {
            pet.setDono(existente.getDono());
        }

        cadastroPetService.salvar(pet);

        return ResponseEntity.ok(pet);
    }
}