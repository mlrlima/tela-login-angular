package service;

import java.io.Serializable;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.transaction.Transactional;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;

@Service
public class EmpresaService implements Serializable {
	private static final long serialVersionUID=1L;
	
	@Autowired
	private EmpresaRepository empresaRepository;
	
    // METODO: logado()
    // FUNCAO: Obtem o usuario logado a partir do request
    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }
	
    
	public List<Empresa> getAllEmpresas(HttpServletRequest request){
		Usuario usuarioLogado =logado(request);
		
		if(usuarioLogado.getRole()==Role.ADMIN){ //se for adm, ve todas as empresas
			return empresaRepository.findAll();
		}
		
		//se for user, so ve as empresas q ele pertence
		return empresaRepository.findByUsuariosId(usuarioLogado.getId());
	}
	
	
	@Transactional
	public Empresa createEmpresa(Empresa empresa, HttpServletRequest request){
		empresa.setId(null); // Garante que o ID seja gerado pelo banco
		return empresaRepository.save(empresa);
	}
	
	
	//apenas admin pode acessar
	public Empresa getEmpresaById(Long id, HttpServletRequest request) {
		Empresa alvo=empresaRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Empresa não encontrada"));
		
		Usuario usuarioLogado =logado(request);
		if(usuarioLogado.getRole()==Role.ADMIN) return alvo;
		
		throw new RuntimeException("Sem permissão");
	}
	
	//apenas admin pode acessar
	public Empresa getEmpresaByNome(String nome, HttpServletRequest request) {
		Empresa alvo=empresaRepository.findByNome(nome)
				.orElseThrow(() -> new RuntimeException("Empresa não encontrada"));
		
		Usuario usuarioLogado =logado(request);
		if(usuarioLogado.getRole()==Role.ADMIN) return alvo;
		
		throw new RuntimeException("Sem permissão");
	}
	
	//apenas admin pode alterar empresas
	@Transactional
	public Empresa updateEmpresa(Empresa empresa, HttpServletRequest request) {
		
		Usuario usuarioLogado = logado(request);
		if(usuarioLogado.getRole()==Role.ADMIN) return empresaRepository.save(empresa);
		
		throw new RuntimeException("Sem permissão");
	}
	
	//apenas admin pode deletar empresas
	@Transactional
	public void deleteEmpresa(Long id, HttpServletRequest request) {
		Empresa alvo=empresaRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Empresa não encontrada"));
		
		Usuario usuarioLogado = logado(request);
		if(usuarioLogado.getRole()!=Role.ADMIN) throw new RuntimeException("Sem permissão");
		
		//desvinculando essa empresa dos usuarios
	    for (Usuario usuario : alvo.getUsuarios()) {
	        usuario.getEmpresas().remove(alvo);
	    }
	    
		empresaRepository.delete(alvo);
			
		
	}

}
