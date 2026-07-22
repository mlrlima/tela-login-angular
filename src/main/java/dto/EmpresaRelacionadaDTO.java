package dto;

public class EmpresaRelacionadaDTO {
    private Long id;
    private String nome;

    public EmpresaRelacionadaDTO(Long id, String nome) {
        this.id = id;
        this.nome = nome;
    }

    public Long getId() { return id; }
    public String getNome() { return nome; }
}