package websocket.service;

import dto.PetResponseDTO;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// DESCRICAO: Buffer thread-safe das localizacoes atualizadas
// FUNCAO: Acumula updates de pets ate o proximo envio em lote
@Component
public class PetLocationBuffer {

    // Chave = id do pet -> so a ULTIMA posicao de cada pet fica no lote,
    // mesmo que ele tenha mandado varios updates no mesmo intervalo
    private final Map<Long, PetResponseDTO> pendentes = new ConcurrentHashMap<>();

    public void adicionar(PetResponseDTO pet) {
        pendentes.put(pet.getId(), pet);
    }


    // Retorna tudo que esta pendente e limpa o buffer, de forma atomica
    public Map<Long, PetResponseDTO> drenar() {
        Map<Long, PetResponseDTO> copia = new ConcurrentHashMap<>(pendentes);
        pendentes.clear();
        return copia;
    }
}