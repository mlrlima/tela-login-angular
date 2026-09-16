package service;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;

import com.lowagie.text.Document;
import com.lowagie.text.DocumentException;
import com.lowagie.text.Font;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Phrase;
import com.lowagie.text.pdf.PdfPCell;
import com.lowagie.text.pdf.PdfPTable;
import com.lowagie.text.pdf.PdfWriter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;

import dto.DonoDTO;
import dto.PetResponseDTO;
import exception.GlobalExceptionHandler;
import model.Pet;
import model.Role;
import model.Usuario;
import repository.PetRepository;

@Service
public class PetService implements Serializable {
	private static final long serialVersionUID=1L;

	@Autowired
	private PetRepository petRepository;
	
	private Usuario logado() {
	    var auth = SecurityContextHolder.getContext().getAuthentication();
	    if (auth == null || !auth.isAuthenticated()) return null;
	    
	    return (Usuario) auth.getPrincipal();
	}
	
	
	public boolean isAdminLogado() {
	    Usuario usuarioLogado = logado();
	    return usuarioLogado != null && usuarioLogado.getRole() == Role.ADMIN;
	}

	// retorna a quantidade de tempo em segundos que o script vai mover o pet pelo mapa
	public int getIntervaloMover(@PathVariable Long id) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado();
		if(ehDonoOuAdmin(usuarioLogado, alvo)) return alvo.getIntervaloMover();
		
		throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}
	
	@Cacheable(value = "pets", key = "#pageable.pageNumber + '-' + #pageable.pageSize",
			condition = "#root.target.isAdminLogado()") //cache por pagina
	public Page<PetResponseDTO> getAllPets(Pageable pageable){
		Usuario usuarioLogado = logado();
		
		Page<Pet> pets;
		
		//se for ADMIN, retorna todos os pets
		if(usuarioLogado.getRole() == Role.ADMIN) {
			pets = petRepository.findAll(pageable);
		} else { //se for user, retorna apenas os dele
			pets = petRepository.findByDono_Id(pageable, usuarioLogado.getId());
		}
		
		return pets.map(this::toDTO);
	}

	//retorna o PDF como uma sequencia de bytes
	//manda esses bytes diretamente para o navegador
	public byte[] gerarPdfPets() {
		Usuario usuarioLogado = logado();
		if (usuarioLogado == null) {
			throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		}

		//se for ADMIN lista todos, se for USER lista apenas os dele
		List<Pet> pets = usuarioLogado.getRole() == Role.ADMIN
				? petRepository.findAll()
				: petRepository.findAllByDono_Id(usuarioLogado.getId());

		Document document = new Document();
		//lugar na memoria onde os bytes do PDF serao armazenados
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			//Liga o PDF ao OutputStream
			//document -> PdfWriter -> output -> bytes do PDF
			PdfWriter.getInstance(document, output);
			document.open();
			
			document.add(new Paragraph("Relatório de Pets", new Font(Font.HELVETICA, 16, Font.BOLD)));
			
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
			document.add(new Paragraph("PDF gerado em: " + LocalDateTime.now().format(formatter)));
			
			document.add(new Paragraph(" "));

			// cria a tabela, indicando o tamanho relativo de cada coluna
			PdfPTable tabela = new PdfPTable(new float[] { 1, 3, 3, 3, 3, 2, 3, 2 });
			//Ocupa 100% da largura disponivel
			tabela.setWidthPercentage(100);
			adicionarCabecalhoPet(tabela, "ID");
			adicionarCabecalhoPet(tabela, "Nome");
			adicionarCabecalhoPet(tabela, "Espécie");
			adicionarCabecalhoPet(tabela, "Dono");
			adicionarCabecalhoPet(tabela, "Nascimento");
			adicionarCabecalhoPet(tabela, "Peso em gramas");
			adicionarCabecalhoPet(tabela, "Localização");
			adicionarCabecalhoPet(tabela, "Intervalo em segundos");

			// .stream() permite fazer operaçoes funcionais sobre a lista. processa um por um
			pets.stream()
					// oedena pelo nome do pet
					.sorted(Comparator.comparing(Pet::getNome, String.CASE_INSENSITIVE_ORDER))
					.forEach(pet -> {
						tabela.addCell(String.valueOf(pet.getId()));
						tabela.addCell(pet.getNome());
						tabela.addCell(pet.getEspecie().name());
						tabela.addCell(pet.getDono().getEmail());
						tabela.addCell(pet.getDataNascimento() == null ? "-" : pet.getDataNascimento().toString());
						tabela.addCell(String.valueOf(pet.getPeso()));
						tabela.addCell(pet.getLatitude() == null || pet.getLongitude() == null
								? "-"
								: pet.getLatitude() + ", " + pet.getLongitude());
						tabela.addCell(String.valueOf(pet.getIntervaloMover()));
					});

			document.add(tabela);
		} catch (DocumentException exception) {
			throw new IllegalStateException("Não foi possível gerar o PDF de pets", exception);
		} finally { //executa sempre, independente se foi try ou catch
			document.close();
		}
		return output.toByteArray();
	}

	private void adicionarCabecalhoPet(PdfPTable tabela, String texto) {
		PdfPCell celula = new PdfPCell(new Phrase(texto, new Font(Font.HELVETICA, 10, Font.BOLD)));
		celula.setBackgroundColor(new java.awt.Color(230, 230, 230));
		tabela.addCell(celula);
	}

	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public PetResponseDTO createPet(Pet pet) {
		Usuario usuarioLogado = logado();
        pet.setDono(usuarioLogado); // o dono do pet sempre será automaticamente o usuario que criou ele
        // nao é possivel alterar o dono de um pet
		
		pet.setId(null);
		
		Pet salvo = petRepository.save(pet);
		return toDTO(salvo);
	}
	
    private boolean ehDonoOuAdmin(Usuario logado, Pet pet) {
        return logado.getRole() == Role.ADMIN ||
               pet.getDono().getId().equals(logado.getId());
    }

	public PetResponseDTO getPetById(Long id) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado();
		if(ehDonoOuAdmin(usuarioLogado, alvo)) return toDTO(alvo);
		
		throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}

	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public PetResponseDTO updatePet(Pet pet) {
		Usuario usuarioLogado = logado();
		if(!ehDonoOuAdmin(usuarioLogado, pet)) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		
		Pet original=petRepository.findById(pet.getId())
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		if(!original.getDono().equals(pet.getDono())) {
			throw new GlobalExceptionHandler.UnauthorizedException("Não é permitido alterar o dono de um pet.");
		}
		
		Pet salvo = petRepository.save(pet);
		return toDTO(salvo);
	}
	
	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public void deletePet(Long id) {
		Pet alvo=petRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));
		
		Usuario usuarioLogado = logado();
		if(!ehDonoOuAdmin(usuarioLogado, alvo)) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");

		petRepository.delete(alvo);
	}
	
    // FUNCAO: Remove TODOS os pets de um usuario (usado ao deletar usuario)
    // Chamado internamente pelo UsuarioService
	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public void deletePetsUsuario(Usuario usuario) {
		List<Pet> lista=petRepository.findAllByDono_Id(usuario.getId());
		
		// Deleta cada pet individualmente
		for (Pet pet:lista) {
			petRepository.delete(pet);
		}
	}
	
	// Converte a entidade em DTO, sem loop e sem expor o Usuario completo
	private PetResponseDTO toDTO(Pet pet) {
		DonoDTO dono = new DonoDTO(
				pet.getDono().getId(),
				pet.getDono().getEmail()
		);
		
		return new PetResponseDTO(
				pet.getId(),
				pet.getNome(),
				pet.getEspecie(),
				dono,
		        pet.getLatitude(), pet.getLongitude(),
		        pet.getIntervaloMover(),
		        pet.getDataNascimento(),
		        pet.getPeso()
		);
	}
	
	@Transactional
	@CacheEvict(value = "pets", allEntries = true) //update o cache
	public PetResponseDTO atualizarLocalizacao(Long id, Double latitude, Double longitude) {
	    Pet alvo = petRepository.findById(id)
	            .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Pet não encontrado"));

	    Usuario usuarioLogado = logado();
	    if (!ehDonoOuAdmin(usuarioLogado, alvo)) {
	        throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	    }

	    alvo.setLatitude(latitude);
	    alvo.setLongitude(longitude);
	    
	    Pet salvo = petRepository.save(alvo);
	    return toDTO(salvo);
	}
}
	