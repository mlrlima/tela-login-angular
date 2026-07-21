package service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import model.Empresa;
import model.Role;
import model.Usuario;
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
	private PetService petService;
	
    // METODO: logado()
    // FUNCAO: Obtem o usuario logado a partir do request
    // RETORNO: Usuario (ou null se nao estiver autenticado)
    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }
	
    // METODO: getAllUsuarios()
    // FUNCAO: Lista usuarios baseado na role do usuario logado
    // REGRA: ADMIN ve todos | USER ve apenas a si mesmo
	public List<Usuario> getAllUsuarios(HttpServletRequest request){
		Usuario usuarioLogado = logado(request);
		
		if(usuarioLogado.getRole() == Role.ADMIN) {
			return usuarioRepository.findAll();
		}
		
		//se for user, so pode ver ele mesmo
		List<Usuario> lista =new ArrayList<>();
		lista.add(usuarioLogado);
		return lista;
	
	}
	
    // METODO: createUsuario()
    // FUNCAO: Cria um novo usuario (cadastro)
    // REGRA: Sempre define role como USER (nao permite criar ADMIN)
	@Transactional // Garante atomicidade (commit ou rollback)
	public Usuario createUsuario(Usuario usuario) {
		usuario.setId(null); // Garante que o ID seja gerado pelo banco
		usuario.setRole(Role.USER); // Forca role USER (seguranca)
		return usuarioRepository.save(usuario);
	}
	
    // METODO: getUsuarioById()
    // FUNCAO: Busca usuario por ID com verificacao de permissao
    // REGRA: ADMIN pode ver qualquer um | USER so pode ver a si mesmo
	public Usuario getUsuarioById(Long id, HttpServletRequest request) {
    	Usuario usuarioLogado = logado(request);
        Usuario alvo = usuarioRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Usuário não encontrado"));
        
     // Verifica permissao: ADMIN ou o proprio usuario
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(alvo.getId())) {

                throw new RuntimeException("Sem permissão");
        }
        
        return alvo;  
	}
    
	
    // METODO: getUsuarioByEmail()
    // FUNCAO: Busca usuario por email com verificacao de permissao
    // REGRA: ADMIN pode ver qualquer um | USER so pode ver a si mesmo
	public Usuario getUsuarioByEmail(String email, HttpServletRequest request) {
    	Usuario usuarioLogado = logado(request);
        Usuario alvo = usuarioRepository.findByEmail(email)
				.orElseThrow(() -> new RuntimeException("Usuário não encontrado"));
        
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(alvo.getId())) {
        	
                throw new RuntimeException("Sem permissão");
        }
        
        return alvo;  
	}
	
	// METODO: getUsuarioByEmailAndSenha()
    // FUNCAO: Autenticacao - busca usuario por email e senha
	public Usuario getUsuarioByEmailAndSenha(String email, String senha){
		Usuario usuario=usuarioRepository.findByEmail(email)
				.orElse(null); // Retorna null se nao encontrar
		
		if(usuario==null || !usuario.getSenha().equals(senha)) return null;
		
		return usuario;
	}
	
    // METODO: updateUsuario()
    // FUNCAO: Atualiza um usuario existente
    // REGRA: ADMIN pode atualizar qualquer um | USER so pode atualizar a si mesmo
    //       Se senha vier em branco, mantem a senha atual
	@Transactional
	public Usuario updateUsuario(Usuario usuario,
									HttpServletRequest request) {
    	
    	Usuario usuarioLogado = logado(request);
    	
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(usuario.getId())) {
        	
                throw new RuntimeException("Sem permissão");
        }
        
        //caso a senha venha em branco (nao alterar senha)
        if (usuario.getSenha() == null || usuario.getSenha().isBlank()) {
            Usuario existente = usuarioRepository.findById(usuario.getId())
            		.orElseThrow(() -> new RuntimeException("Usuário não encontrado"));;

            usuario.setSenha(existente.getSenha());
        }
        
        return usuarioRepository.save(usuario);
	
	}
	
    // METODO: deleteUsuario()
    // FUNCAO: Remove um usuario (e seus pets)
    // REGRA: ADMIN pode deletar qualquer um | USER so pode deletar a si mesmo
	@Transactional
    public void deleteUsuario(Long id, HttpServletRequest request) {
		 Usuario alvo = usuarioRepository.findById(id)
	        		.orElseThrow(() -> new RuntimeException("Usuário não encontrado"));
		
		Usuario usuarioLogado = logado(request);

        if (usuarioLogado.getRole() != Role.ADMIN &&
            !usuarioLogado.getId().equals(id)) {

        	throw new RuntimeException("Sem permissão");
        }

        //deletar pets desse usuario
        petService.deletePetsUsuario(alvo);
        
		//desvinculando esse usuario das empresas
	    for (Empresa empresa : alvo.getEmpresas()) {
	        empresa.getUsuarios().remove(alvo);
	    }
        
        usuarioRepository.delete(alvo);
    }
}
