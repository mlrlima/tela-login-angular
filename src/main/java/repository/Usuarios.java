package repository;

import java.io.Serializable;
import java.util.List;

import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import model.Usuario;

@Repository
public class Usuarios implements Serializable {
	private static final long serialVersionUID = 1L;
	
	@Autowired
	private EntityManager manager;
	
	public Usuarios() {
	}
	
	public Usuarios(EntityManager manager) {
		this.manager = manager;
	}
	
	
	//ok
	public Usuario porId(Long id) {
		return manager.find(Usuario.class, id);
	}

	//ok
	//pesquisar usuario por nome
	public List<Usuario> pesquisar(String nome) {
		String jpql = "from Usuario where nome like :nome";
		
		TypedQuery<Usuario> query = manager.createQuery(jpql, Usuario.class);
		query.setParameter("nome", nome + "%");
		return query.getResultList();
	}
	
	//ok
	public Usuario porEmail(String email){
		try {
			String jpql="from Usuario where email = :email";
			
			TypedQuery<Usuario> query=manager.createQuery(jpql, Usuario.class);
			query.setParameter("email", email);
			return query.getSingleResult(); //espero q isso retorne um Usuario
		}catch(Exception e) {
			return null;
		}
		
	}
	

	public Usuario porEmailESenha(String email, String senha){
		try {
			String jpql="from Usuario where email = :email and senha = :senha";
			
			TypedQuery<Usuario> query=manager.createQuery(jpql, Usuario.class);
			query.setParameter("email", email);
			query.setParameter("senha", senha);
			return query.getSingleResult();
		}catch(Exception e){
			return null;
		}
	}
	
	//retorna lista com todos os usuarios
	public List<Usuario> todos() {
		TypedQuery<Usuario> query = manager.createQuery("from Usuario", Usuario.class);
		return query.getResultList();
	}

	//create && update
	public Usuario guardar(Usuario usuario) {
		//atualizar - update ou insert
		return manager.merge(usuario);
	}

	//delete
	public void remover(Usuario usuario) {
		usuario = porId(usuario.getId());
		
		Pets pets=new Pets(manager);
		pets.removerPetsUsuario(usuario);
		
		manager.remove(usuario);
	}
	
}
