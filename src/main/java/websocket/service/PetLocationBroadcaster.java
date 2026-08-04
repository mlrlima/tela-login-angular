package websocket.service;

import dto.PetResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.Collection;
import java.util.Map;

// DESCRICAO: Envia as localizacoes acumuladas periodicamente
// FUNCAO: Evita 1 mensagem WS por pet, agrupando em lote
@Component
public class PetLocationBroadcaster {

    @Autowired
    private PetLocationBuffer buffer;

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Scheduled(fixedRate = 500) // a cada 500ms
    public void enviarLote() {
        Map<Long, PetResponseDTO> pendentes = buffer.drenar();
        if (pendentes.isEmpty()) return; // se nada mudou, nao envia

        Collection<PetResponseDTO> lote = pendentes.values();
        messagingTemplate.convertAndSend("/topic/pet-location-lote", lote);
    }
}