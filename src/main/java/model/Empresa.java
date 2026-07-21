package model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotBlank;

@Entity
@Table(name="empresa")
public class Empresa implements Serializable {
	private static final long serialVersionUID=1L;
	
	// CAMPOS DA ENTIDADE
	
	@Id //Chave primaria PK
	@GeneratedValue(strategy=GenerationType.IDENTITY) //gera um valor auto-incremental
	private Long id;
	
	@NotBlank
	@Column(nullable = false, length = 100, unique = true)
	private String nome;
	
    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE}) //para apagar registros caso uma empresa ou usuario for apagado
    @JoinTable(
        name = "empresa_usuario", // Nome da tabela associativa
        joinColumns = @JoinColumn(name = "empresa_id"), // Chave estrangeira desta classe
        inverseJoinColumns = @JoinColumn(name = "usuario_id") // Chave estrangeira da outra classe (usuario)
    )
    private Set<Usuario> usuarios = new HashSet<>();

    
    // GETTERS E SETTERS
    
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}

	public String getNome() {
		return nome;
	}
	public void setNome(String nome) {
		this.nome = nome;
	}

	public Set<Usuario> getUsuarios() {
		return usuarios;
	}
	public void setUsuarios(Set<Usuario> usuarios) {
		this.usuarios = usuarios;
	}
	
    // METODOS UTILITARIOS
	
	@Override 
	public String toString() {
		return "Empresa [id=" + id + "]";
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(id);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Empresa other = (Empresa) obj;
		return Objects.equals(id, other.id);
	}
}
