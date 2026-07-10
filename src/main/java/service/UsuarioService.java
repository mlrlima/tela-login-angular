package service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import model.Role;
import model.Usuario;
import repository.UsuarioRepository;

@Service
public class UsuarioService implements Serializable {
	private static final long serialVersionUID=1L;
	
	@Autowired
	private UsuarioRepository usuarioRepository;
	
	@Autowired
	private PetService petService;
	
    private Usuario logado(HttpServletRequest request) {
        return (Usuario) request.getAttribute("usuarioLogado");
    }
	
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
	
	@Transactional
	public Usuario createUsuario(Usuario usuario) {
		usuario.setId(null);
		usuario.setRole(Role.USER);
		return usuarioRepository.save(usuario);
	}
	
	public Usuario getUsuarioById(Long id, HttpServletRequest request) {
    	Usuario usuarioLogado = logado(request);
        Usuario alvo = usuarioRepository.findById(id)
				.orElseThrow(() -> new RuntimeException("Usuário não encontrado"));
        
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(alvo.getId())) {

                throw new RuntimeException("Sem permissão");
        }
        
        return alvo;  
	}
    
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
	
	public Usuario getUsuarioByEmailAndSenha(String email, String senha){
		Usuario usuario=usuarioRepository.findByEmail(email)
				.orElse(null);
		
		if(usuario==null || !usuario.getSenha().equals(senha)) return null;
		
		return usuario;
	}
	
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
        
        usuarioRepository.delete(alvo);
    }
}
