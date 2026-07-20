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
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;


@Entity // Indica que esta classe eh uma entidade JPA
@Table(name="pet") // Nome da tabela no banco de dados
public class Pet implements Serializable {
	//transforma a informacao em streams de bytes
	//garante compatibilidade na desserializacao
	private static final long serialVersionUID=1L;
	
    // CAMPOS DA ENTIDADE
	
	@Id //Chave primaria PK
	@GeneratedValue(strategy=GenerationType.IDENTITY) //gera um valor auto-incremental
	private Long id;
	
	@NotBlank // Nao pode ser nulo, vazio ou conter apenas espacos
	@Column(nullable=false, length=100)
	private String nome;
	
	@NotNull
	@ManyToOne //pet N:1 dono ; RELACIONAMENTO: Muitos pets para um usuario
	@JoinColumn(name="user_id", nullable=false) // Chave estrangeira FK
	private Usuario dono;
	
	@NotNull
	@Enumerated(EnumType.STRING) // Salva o nome do enum (ex: "CACHORRO") no banco
	@Column(nullable=false)
	private Especie especie;

	
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

	public Usuario getDono() {
		return dono;
	}
	public void setDono(Usuario dono) {
		this.dono = dono;
	}
	
	public Especie getEspecie() {
		return especie;
	}
	public void setEspecie(Especie especie) {
		this.especie = especie;
	}
	
    // METODOS UTILITARIOS
	
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
		Pet other = (Pet) obj;
		return Objects.equals(id, other.id); // Compara pelo ID
	}
	@Override
	public String toString() {
		return "Pet [id=" + id + "]";
	}
	
}
