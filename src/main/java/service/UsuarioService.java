
package service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dto.EmpresaRelacionadaDTO;
import dto.UsuarioRelacionadoDTO;
import dto.UsuarioResponseDTO;
import exception.GlobalExceptionHandler;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;
import repository.UsuarioRepository;

//CLASSE: UsuarioService
//DESCRICAO: Camada de servico para gerenciamento de usuarios
//FUNCAO: Regras de negocio, validacoes e controle de permissao

@Service //servico gerenciado pelo Spring
public class UsuarioService implements Serializable {
	private static final long serialVersionUID=1L;
	
	@Autowired
	private UsuarioRepository usuarioRepository;
	
	@Autowired
	private EmpresaRepository empresaRepository;
	
	@Autowired
	private PetService petService;
	
	
	private Usuario logado() {
	    var auth = SecurityContextHolder.getContext().getAuthentication();
	    if (auth == null || !auth.isAuthenticated()) return null;
	    return (Usuario) auth.getPrincipal();
	}
	
    // METODO: getAllUsuarios()
    // FUNCAO: Lista usuarios baseado na role do usuario logado
    // REGRA: ADMIN ve todos | USER ve apenas a si mesmo
	public List<UsuarioResponseDTO> getAllUsuarios(){
		Usuario usuarioLogado = logado();
		
		List<Usuario> usuarios;
		
		if(usuarioLogado.getRole() == Role.ADMIN) {
			usuarios = usuarioRepository.findAll();
		} else { 
			usuarios = new ArrayList<>();
			usuarios.add(usuarioRepository.findById(usuarioLogado.getId())
					.orElseThrow(()-> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado")));
		}
		
		return usuarios.stream().map(this::toDTO)
				.collect(Collectors.toList());
	}
	
    // METODO: createUsuario()
    // FUNCAO: Cria um novo usuario (cadastro)
    // REGRA: Sempre define role como USER (nao permite criar ADMIN)
	@Transactional // Garante atomicidade (commit ou rollback)
	public UsuarioResponseDTO createUsuario(Usuario usuario) {
		usuario.setId(null);
		usuario.setRole(Role.USER);
		vincularEmpresasExistentes(usuario);
		Usuario salvo = usuarioRepository.save(usuario);
		return toDTO(salvo);
	}
	
    // METODO: getUsuarioById()
    // FUNCAO: Busca usuario por ID com verificacao de permissao
    // REGRA: ADMIN pode ver qualquer um | USER so pode ver a si mesmo
	public UsuarioResponseDTO getUsuarioById(Long id) {
    	Usuario usuarioLogado = logado();
        Usuario alvo = usuarioRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
        
        //verifica se eh ADMIN ou o proprio usuario
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(alvo.getId())) {
                throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
        }
        
        return toDTO(alvo);
	}
    
	
	public UsuarioRelacionadoDTO getUsuarioByEmail(String email) {
	    Usuario alvo = usuarioRepository.findByEmail(email);
	    
	    if(alvo==null)
	    	throw new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado");

	    return new UsuarioRelacionadoDTO(alvo.getId(), alvo.getEmail());
	}
	
	// METODO: getUsuarioByEmailAndSenha()
    // FUNCAO: Autenticacao - busca usuario por email e senha
	public Usuario getUsuarioByEmailAndSenha(String email, String senha){
		Usuario usuario=usuarioRepository.findByEmail(email);
		
		if(usuario==null || !usuario.getSenha().equals(senha)) return null;
		
		return usuario;
	}
	
    // METODO: updateUsuario()
    // FUNCAO: Atualiza um usuario existente
    // REGRA: ADMIN pode atualizar qualquer um | USER so pode atualizar a si mesmo
    //       Se senha vier em branco, mantem a senha atual
	@Transactional
	public UsuarioResponseDTO updateUsuario(Usuario usuario) {
		Usuario usuarioLogado = logado();
		
		//verifica se eh ADMIN ou o proprio usuario
		if(usuarioLogado.getRole() != Role.ADMIN &&
		!usuarioLogado.getId().equals(usuario.getId())) {
			throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		}
		
		//deixar a senha antiga
		if (usuario.getSenha() == null || usuario.getSenha().isBlank()) {
			
			Usuario existente = usuarioRepository.findById(usuario.getId())
			.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
			
			usuario.setSenha(existente.getSenha());
		}
		
		vincularEmpresasExistentes(usuario);
		Usuario salvo = usuarioRepository.save(usuario);
		return toDTO(salvo);
	}
	
    // METODO: deleteUsuario()
    // FUNCAO: Remove um usuario (e seus pets)
    // REGRA: ADMIN pode deletar qualquer um | USER so pode deletar a si mesmo
	@Transactional
    public void deleteUsuario(Long id) {
		 Usuario alvo = usuarioRepository.findById(id)
	        		.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
		
		Usuario usuarioLogado = logado();

        if (usuarioLogado.getRole() != Role.ADMIN &&
            !usuarioLogado.getId().equals(id)) {

        	throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
        }

        //deletar pets desse usuario
        petService.deletePetsUsuario(alvo);
        
		//desvinculando esse usuario das empresas
	    for (Empresa empresa : alvo.getEmpresas()) {
	        empresa.getUsuarios().remove(alvo);
	    }
        
        usuarioRepository.delete(alvo);
    }
	
	// Converte a entidade em DTO, sem senha e sem risco de loop
	private UsuarioResponseDTO toDTO(Usuario usuario) {
		List<EmpresaRelacionadaDTO> empresas = usuario.getEmpresas().stream()
				.map(e -> new EmpresaRelacionadaDTO(e.getId(), e.getNome()))
				.collect(Collectors.toList());
		
		return new UsuarioResponseDTO(
				usuario.getId(),
				usuario.getEmail(),
				usuario.getNome(),
				usuario.getRole(),
				empresas
		);
	}
	
	// Substitui as Empresas vindos do JSON
	// pelas entidades gerenciadas de verdade, buscadas do banco por id.
	private void vincularEmpresasExistentes(Usuario usuario) {
		//caso a lista do frontend venha vazia
		if (usuario.getEmpresas() == null || usuario.getEmpresas().isEmpty()) {
			usuario.setEmpresas(new HashSet<>());
			return;
		}

		Set<Long> ids = usuario.getEmpresas().stream()
				.map(Empresa::getId)
				.collect(Collectors.toSet());

		List<Empresa> empresasGerenciadas = empresaRepository.findAllById(ids);

		usuario.setEmpresas(new HashSet<>(empresasGerenciadas));
	}

}
