package security.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import model.Usuario;
import repository.UsuarioRepository;

@Service
public class AuthService implements UserDetailsService{
	
	@Autowired
	UsuarioRepository usuarioRepository;

	@Override
	public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
		Usuario usuario = usuarioRepository.findByEmail(username);
		
	    if (usuario == null)
	        throw new UsernameNotFoundException("Usuário não encontrado: " + username);

	    return usuario;
	}

}
