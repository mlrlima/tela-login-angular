package dto;

import model.Especie;

public class PetResponseDTO {
    private Long id;
    private String nome;
    private Especie especie;
    private DonoDTO dono;
    
    private Double latitude;
    private Double longitude;

    public PetResponseDTO(Long id, String nome, Especie especie, DonoDTO dono,
    					 	Double latitude, Double longitude) {
        this.id = id;
        this.nome = nome;
        this.especie = especie;
        this.dono = dono;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Long getId() { return id; }
    public String getNome() { return nome; }
    public Especie getEspecie() { return especie; }
    public DonoDTO getDono() { return dono; }
    
    public Double getLatitude() { return latitude; }
    public Double getLongitude() { return longitude; }
}