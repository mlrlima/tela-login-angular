package repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import model.Empresa;

public interface EmpresaRepository extends JpaRepository<Empresa, Long> {

	// Busca pelo ID do usuario
    List<Empresa> findByUsuariosId(Long usuarioId);
}
