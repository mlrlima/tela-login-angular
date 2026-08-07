package dto;

import java.time.LocalDate;

import model.Especie;

public class PetResponseDTO {
    private Long id;
    private String nome;
    private Especie especie;
    private DonoDTO dono;
    
    private Double latitude;
    private Double longitude;
    private int intervaloMover; //em segundos
    
    private LocalDate dataNascimento;
    private int peso; //em gramas

    public PetResponseDTO(Long id, String nome, Especie especie, DonoDTO dono,
    					 	Double latitude, Double longitude, int intervaloMover,
    					 	LocalDate dataNascimento, int peso) {
        this.id = id;
        this.nome = nome;
        this.especie = especie;
        this.dono = dono;
        
        this.latitude = latitude;
        this.longitude = longitude;
        this.intervaloMover=intervaloMover;
        
        this.dataNascimento=dataNascimento;
        this.peso=peso;
    }

    public Long getId() { return id; }
    public String getNome() { return nome; }
    public Especie getEspecie() { return especie; }
    public DonoDTO getDono() { return dono; }
    
    public Double getLatitude() { return latitude; }
    public Double getLongitude() { return longitude; }
    public int getIntervaloMover() {return intervaloMover;}
    
    public LocalDate getDataNascimento() { return dataNascimento; }
    public int getPeso(){return peso;}
}