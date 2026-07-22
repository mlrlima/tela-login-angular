package dto;

import java.util.List;
import model.Role;

public class UsuarioResponseDTO {
    private Long id;
    private String email;
    private String nome;
    private Role role;
    private List<EmpresaRelacionadaDTO> empresas;

    public UsuarioResponseDTO(Long id, String email, String nome, Role role, List<EmpresaRelacionadaDTO> empresas) {
        this.id = id;
        this.email = email;
        this.nome = nome;
        this.role = role;
        this.empresas = empresas;
    }

    public Long getId() { return id; }
    public String getEmail() { return email; }
    public String getNome() { return nome; }
    public Role getRole() { return role; }
    public List<EmpresaRelacionadaDTO> getEmpresas() { return empresas; }
}