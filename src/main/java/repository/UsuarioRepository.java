package repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import model.Usuario;

//INTERFACE: UsuarioRepository
//DESCRICAO: Camada de acesso a dados (DAO) para a entidade Usuario
//FUNCAO: Fornece metodos CRUD e consultas personalizadas

public interface UsuarioRepository extends JpaRepository<Usuario, Long> {
	// Estende JpaRepository - ja fornece metodos prontos:
	// - save(), findById(), findAll(), deleteById(), etc.
	// - Paginacao e ordenacao (Pageable)
	// - Count, exists, flush, etc.
	
	// GENERICS: <TIPO_DA_ENTIDADE, TIPO_DO_ID>
    // Usuario = entidade gerenciada
    // Long = tipo da chave primaria (@Id)

    // METODO: findByEmail()
    // FUNCAO: Busca um usuario pelo email (campo UNIQUE)
    // RETORNO: Optional<Usuario> - pode conter o usuario ou estar vazio
    // ============================================================
    // O Spring Data JPA interpreta o nome do metodo e gera a query automaticamente:
    // SELECT * FROM usuario WHERE email = ?
    // 
    // CONVENCAO DE NOMES:
    // - findBy + NomeDoCampo (email) + Parametro (String email)
    // - Retorna Optional para evitar NullPointerException
	Optional<Usuario> findByEmail(String email);
}
