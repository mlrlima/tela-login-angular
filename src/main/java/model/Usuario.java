package model;

import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@Entity // Indica que esta classe eh uma entidade JPA (mapeada para o banco)
@Table(name="usuario") // Nome da tabela no banco de dados
public class Usuario implements Serializable{
	//transforma a informacao em streams de bytes
	//garante compatibilidade na desserializacao
	private static final long serialVersionUID=1L;
	
	// CAMPOS DA ENTIDADE
	
	@Id //Chave primaria PK
	@GeneratedValue(strategy=GenerationType.IDENTITY) //gera um valor auto-incremental
	private Long id;
	
	@NotBlank // Nao pode ser nulo, vazio ou conter apenas espacos
	@Email // Deve ser um email valido (ex: usuario@email.com)
	@Column(nullable = false, length = 50, unique = true)
	private String email;

	@NotBlank
	@Size(min = 4, message = "A senha deve ter no minimo 4 caracteres")
	@Column(nullable = false, length = 50)
	private String senha;

	@NotBlank
	@Column(nullable = false, length = 100)
	private String nome;
	
	@NotNull 
	@Enumerated(EnumType.STRING) // Salva o nome do enum (ex: "ADMIN") no banco
	@Column(nullable=false)
	private Role role; //ADMIN, USER

	
    // GETTERS E SETTERS
	
	public Role getRole() {
		return role;
	}
	public void setRole(Role role) {
		this.role=role;
	}
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}

	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}

	public String getSenha() {
		return senha;
	}
	public void setSenha(String senha) {
		this.senha = senha;
	}

	public String getNome() {
		return nome;
	}
	public void setNome(String nome){
		this.nome = nome;
	}
	
	
    // METODOS UTILITARIOS
	
	@Override //pra substituir por essa funcao a que ja existe
	public String toString() {
		return "Usuario [id=" + id + "]";
	}
	
	
	@Override
	public int hashCode() {
		return Objects.hash(id); // Baseado apenas no ID
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Usuario other = (Usuario) obj;
		return Objects.equals(id, other.id); // Compara pelo ID
	}
}
