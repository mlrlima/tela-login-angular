
package service;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
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
	
	@Cacheable(value = "usuarios", key = "#pageable.pageNumber + '-' + #pageable.pageSize") //cache por pagina
	public Page<UsuarioResponseDTO> getAllUsuarios(Pageable pageable){ //implementa paginacao
	    Usuario usuarioLogado = logado();

	    Page<Usuario> usuarios;

	    if(usuarioLogado.getRole() == Role.ADMIN) {
	        usuarios = usuarioRepository.findAll(pageable);
	    } else {
	    	//se o usuario nao for admin, retorna apenas ele mesmo
	        Usuario usuarioComum = usuarioRepository.findById(usuarioLogado.getId())
	                .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
	        usuarios = new PageImpl<>(List.of(usuarioComum), pageable, 1);
	    }

	    return usuarios.map(this::toDTO);
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
	@Cacheable(value = "usuarioPorId", key = "#id")
	public Usuario buscarUsuarioNoCache(Long id) {
	    return usuarioRepository.findById(id)
	        .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
	}
	public UsuarioResponseDTO getUsuarioById(Long id) {
    	Usuario usuarioLogado = logado();
    	Usuario alvo = buscarUsuarioNoCache(id);
        
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
	    
	    Usuario usuarioLogado = logado();
        //verifica se eh ADMIN ou o proprio usuario
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(alvo.getId())) {
                throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
        }

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
	@CacheEvict(value = "usuarioPorId", key = "#usuario.id") //update o cache
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
		
		}else if(usuario.getSenha().length() < 4) {
			throw new GlobalExceptionHandler.UnauthorizedException("A senha dever ter no mínimo 4 caracteres.");
        }else{
			//encriptar nova senha
			String encryptedPassword = new BCryptPasswordEncoder().encode(usuario.getSenha());
	        usuario.setSenha(encryptedPassword);
		}
		
		//nao salva empresas pois isso ja eh feito em empresaRepository
		Usuario salvo = usuarioRepository.save(usuario);
		return toDTO(salvo);
	}
	
    // METODO: deleteUsuario()
    // FUNCAO: Remove um usuario (e seus pets)
    // REGRA: ADMIN pode deletar qualquer um | USER so pode deletar a si mesmo
	@Transactional
	@CacheEvict(value = "usuarioPorId", key = "#id") //update cache
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
