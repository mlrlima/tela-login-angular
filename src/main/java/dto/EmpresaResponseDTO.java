package dto;

import java.util.List;

public class EmpresaResponseDTO {
    private Long id;
    private String nome;
    private List<UsuarioRelacionadoDTO> usuarios;

    public EmpresaResponseDTO(Long id, String nome, List<UsuarioRelacionadoDTO> usuarios) {
        this.id = id;
        this.nome = nome;
        this.usuarios = usuarios;
    }

    public Long getId() { return id; }
    public String getNome() { return nome; }
    public List<UsuarioRelacionadoDTO> getUsuarios() { return usuarios; }
}