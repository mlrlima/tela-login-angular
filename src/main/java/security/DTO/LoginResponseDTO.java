package security.DTO;

import model.Role;

public record LoginResponseDTO(
		String token,
        Long id,
        String nome,
        String email,
        Role role
        ) {

}
