package service;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import dto.EmpresaResponseDTO;
import dto.UsuarioRelacionadoDTO;
import exception.GlobalExceptionHandler;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.transaction.Transactional;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;
import repository.UsuarioRepository;

@Service
public class EmpresaService implements Serializable {
	private static final long serialVersionUID=1L;
	
	@Autowired
	private EmpresaRepository empresaRepository;
	
	@Autowired
	private UsuarioRepository usuarioRepository;
	
    // METODO: logado()
    // FUNCAO: Obtem o usuario logado a partir do request
    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }
	
    private EmpresaResponseDTO toDTO(Empresa empresa) {
        List<UsuarioRelacionadoDTO> usuarios = empresa.getUsuarios().stream()
            .map(u -> new UsuarioRelacionadoDTO(u.getId(), u.getEmail()))
            .collect(Collectors.toList()); //passa todo os usuarios relacionados pelo DTO

        return new EmpresaResponseDTO(empresa.getId(), empresa.getNome(), usuarios);
    }
    
	public List<EmpresaResponseDTO> getAllEmpresas(HttpServletRequest request){
		return empresaRepository.findAll()
				.stream().map(this::toDTO).collect(Collectors.toList());
	}
	
	
	@Transactional
	public EmpresaResponseDTO createEmpresa(Empresa empresa, HttpServletRequest request){
	    empresa.setId(null);
	    vincularUsuariosExistentes(empresa);
	    Empresa salva = empresaRepository.save(empresa);
	    return toDTO(salva);
	}
	
	
	//apenas admin pode acessar
	public EmpresaResponseDTO getEmpresaById(Long id, HttpServletRequest request) {
	    Empresa alvo=empresaRepository.findById(id)
	            .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Empresa não encontrada"));

	    Usuario usuarioLogado =logado(request);
	    if(usuarioLogado.getRole()==Role.ADMIN) return toDTO(alvo);

	    throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}
	
	//apenas admin pode acessar
	public EmpresaResponseDTO getEmpresaByNome(String nome, HttpServletRequest request) {
	    Empresa alvo=empresaRepository.findByNome(nome)
	            .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Empresa não encontrada"));

	    Usuario usuarioLogado =logado(request);
	    if(usuarioLogado.getRole()==Role.ADMIN) return toDTO(alvo);

	    throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}
	
	//apenas admin pode alterar empresas
	@Transactional
	public EmpresaResponseDTO updateEmpresa(Empresa empresa, HttpServletRequest request) {

	    Usuario usuarioLogado = logado(request);
	    if(usuarioLogado.getRole()!=Role.ADMIN)
	    	throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");

	    vincularUsuariosExistentes(empresa);
	    Empresa salva = empresaRepository.save(empresa);
	    return toDTO(salva);
	}
	
	//apenas admin pode deletar empresas
	@Transactional
	public void deleteEmpresa(Long id, HttpServletRequest request) {
		Empresa alvo=empresaRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Empresa não encontrada"));
		
		Usuario usuarioLogado = logado(request);
		if(usuarioLogado.getRole()!=Role.ADMIN)
			throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		
		//desvinculando essa empresa dos usuarios
	    for (Usuario usuario : alvo.getUsuarios()) {
	        usuario.getEmpresas().remove(alvo);
	    }
	    
		empresaRepository.delete(alvo);
	}
	
	// Substitui os Usuarios vindos do JSON, só com id/email
	// pelas entidades gerenciadas de verdade, buscadas do banco por id.
	private void vincularUsuariosExistentes(Empresa empresa) {
		//caso a lista do frontend venha vazia
		if (empresa.getUsuarios() == null || empresa.getUsuarios().isEmpty()) {
			empresa.setUsuarios(new HashSet<>());
			return;
		}

		Set<Long> ids = empresa.getUsuarios().stream()
				.map(Usuario::getId)
				.collect(Collectors.toSet());

		List<Usuario> usuariosGerenciados = usuarioRepository.findAllById(ids);

		if (usuariosGerenciados.size() != ids.size()) {
			throw new GlobalExceptionHandler.ResourceNotFoundException("Um ou mais usuários informados não foram encontrados");
		}

		empresa.setUsuarios(new HashSet<>(usuariosGerenciados));
	}

}
