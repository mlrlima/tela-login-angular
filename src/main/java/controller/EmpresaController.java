package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import dto.EmpresaResponseDTO;
import model.Empresa;
import service.EmpresaService;

@RestController
@RequestMapping("/empresa")
public class EmpresaController {
	
	@Autowired
	private EmpresaService service;
	
	@GetMapping("/all")
	public ResponseEntity<Page<EmpresaResponseDTO>> getAll(
									@RequestParam(defaultValue = "0") int page,
	        						@RequestParam(defaultValue = "10") int size){
		
		 Pageable pageable = PageRequest.of(page, size);
		 Page<EmpresaResponseDTO> empresas = service.getAllEmpresas(pageable);
		 return ResponseEntity.ok(empresas);
	}
    
    @PostMapping
    public EmpresaResponseDTO create(@RequestBody Empresa empresa) {
        return service.createEmpresa(empresa);
    }

	@GetMapping("/{id}")
	public EmpresaResponseDTO getById(@PathVariable Long id) {
    	return service.getEmpresaById(id);
	}

	@GetMapping
	public EmpresaResponseDTO getByNome(@RequestParam String nome) {
    	return service.getEmpresaByNome(nome);  
	}

    @PutMapping
    public EmpresaResponseDTO update(@RequestBody Empresa empresa) {
        return service.updateEmpresa(empresa);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
    	service.deleteEmpresa(id);
        return ResponseEntity.noContent().build();
    }

}
