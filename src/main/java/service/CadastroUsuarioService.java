package service;

import java.io.Serializable;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import model.Usuario;
import repository.Usuarios;

@Service
public class CadastroUsuarioService implements Serializable {
	private static final long serialVersionUID=1L;
	
    private final Usuarios usuarios;

    public CadastroUsuarioService(Usuarios usuarios) {
        this.usuarios = usuarios;
    }
	
	@Transactional
	public void salvar(Usuario usuario) {
		usuarios.guardar(usuario);
	}
	
	@Transactional
	public void excluir(Usuario usuario) {
		usuarios.remover(usuario);
	}

}
