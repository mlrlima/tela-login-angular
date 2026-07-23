package dto;

public class DonoDTO {
    private Long id;
    private String email;

    public DonoDTO(Long id, String email) {
        this.id = id;
        this.email = email;
    }

    public Long getId() { return id; }
    public String getEmail() { return email; }
}