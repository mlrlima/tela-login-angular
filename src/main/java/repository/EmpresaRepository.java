package repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import model.Empresa;
import model.Usuario;

public interface EmpresaRepository extends JpaRepository<Empresa, Long> {

	// Busca pelo ID do usuario
    List<Empresa> findByUsuariosId(Long usuarioId);
    
    Optional<Empresa> findByNome(String nome);
}
