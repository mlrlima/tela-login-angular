package security.DTO;

import model.Role;

public record LoginResponseDTO(
		Long id,
        String nome,
        String email,
        Role role
        ) {

}
