package controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
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
import jakarta.servlet.http.HttpServletRequest;
import model.Empresa;
import security.Secured;
import service.EmpresaService;

@RestController
@RequestMapping("/empresa")
public class EmpresaController {
	
	@Autowired
	private EmpresaService service;
	
    @Secured
	@GetMapping("/all")
	public ResponseEntity<List<EmpresaResponseDTO>> getAll(HttpServletRequest request){
		return ResponseEntity.ok(service.getAllEmpresas(request));
	}
    
    @PostMapping
    public EmpresaResponseDTO create(@RequestBody Empresa empresa, HttpServletRequest request) {
        return service.createEmpresa(empresa, request);
    }
	
    @Secured
	@GetMapping("/{id}")
	public EmpresaResponseDTO getById(@PathVariable Long id, HttpServletRequest request) {
    	return service.getEmpresaById(id, request);
	}
    
    @Secured
	@GetMapping
	public EmpresaResponseDTO getByNome(@RequestParam String nome, HttpServletRequest request) {
    	return service.getEmpresaByNome(nome, request);  
	}
	
    
    @Secured
    @PutMapping
    public EmpresaResponseDTO update(@RequestBody Empresa empresa, HttpServletRequest request) {
        return service.updateEmpresa(empresa, request);
    }
    
    @Secured
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id, HttpServletRequest request) {
    	service.deleteEmpresa(id, request);
        return ResponseEntity.noContent().build();
    }

}
