package dto;

import model.Especie;

public class PetResponseDTO {
    private Long id;
    private String nome;
    private Especie especie;
    private DonoDTO dono;

    public PetResponseDTO(Long id, String nome, Especie especie, DonoDTO dono) {
        this.id = id;
        this.nome = nome;
        this.especie = especie;
        this.dono = dono;
    }

    public Long getId() { return id; }
    public String getNome() { return nome; }
    public Especie getEspecie() { return especie; }
    public DonoDTO getDono() { return dono; }
}