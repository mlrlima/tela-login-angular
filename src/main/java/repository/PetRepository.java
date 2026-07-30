package repository;

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import model.Pet;

public interface PetRepository extends JpaRepository<Pet, Long> {
	
	//para paginacao , usado no get all
	Page<Pet> findByDono_Id(Pageable pageable, Long donoId);
	
	// Para operações que precisam de TODOS os registros (ex: deletar)
    List<Pet> findAllByDono_Id(Long donoId);
}
