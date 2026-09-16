package service;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.ByteArrayOutputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.springframework.cache.annotation.Caching;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import dto.EmpresaResponseDTO;
import dto.UsuarioRelacionadoDTO;
import exception.GlobalExceptionHandler;
import jakarta.transaction.Transactional;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;
import repository.UsuarioRepository;

@Service
public class EmpresaService implements Serializable {
	private static final long serialVersionUID=1L;
	
	@Autowired
	private EmpresaRepository empresaRepository;
	
	@Autowired
	private UsuarioRepository usuarioRepository;
	
	private Usuario logado() {
	    var auth = SecurityContextHolder.getContext().getAuthentication();
	    if (auth == null || !auth.isAuthenticated()) return null;
	    return (Usuario) auth.getPrincipal();
	}
	
    private EmpresaResponseDTO toDTO(Empresa empresa) {
        List<UsuarioRelacionadoDTO> usuarios = empresa.getUsuarios().stream()
            .map(u -> new UsuarioRelacionadoDTO(u.getId(), u.getEmail()))
            .collect(Collectors.toList()); //passa todo os usuarios relacionados pelo DTO

        return new EmpresaResponseDTO(empresa.getId(), empresa.getNome(), usuarios);
    }
    
    @Cacheable(value = "empresas", key = "#pageable.pageNumber + '-' + #pageable.pageSize") //cache por pagina
	public Page<EmpresaResponseDTO> getAllEmpresas(Pageable pageable){
    	//apenas ADMIN pode listar todas as empresas
    	Usuario usuarioLogado =logado();
	    if(usuarioLogado.getRole()!=Role.ADMIN) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
    	
		return empresaRepository.findAll(pageable).map(this::toDTO);
	}

	public byte[] gerarPdfEmpresas() {
		validarAdmin();

		Document document = new Document();
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			PdfWriter.getInstance(document, output);
			document.open();
			document.add(new Paragraph("Relatório de Empresas", new Font(Font.HELVETICA, 16, Font.BOLD)));
			
			//data e hora
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
			String dataHora = LocalDateTime.now().format(formatter);
			document.add(new Paragraph("PDF gerado em: " + dataHora));
			
			document.add(new Paragraph(" "));

			PdfPTable tabela = new PdfPTable(new float[] { 1, 3, 5 });
			tabela.setWidthPercentage(100);
			adicionarCabecalho(tabela, "ID");
			adicionarCabecalho(tabela, "Empresa");
			adicionarCabecalho(tabela, "Usuários relacionados");

			empresaRepository.findAll().stream()
				.sorted(Comparator.comparing(Empresa::getNome, String.CASE_INSENSITIVE_ORDER))
				.forEach(empresa -> {
					tabela.addCell(String.valueOf(empresa.getId()));
					tabela.addCell(empresa.getNome());
					String usuarios = empresa.getUsuarios().stream()
							.map(Usuario::getEmail)
							.sorted(String.CASE_INSENSITIVE_ORDER)
							.collect(Collectors.joining(", "));
					tabela.addCell(usuarios.isEmpty() ? "Nenhum usuário vinculado" : usuarios);
				});

			document.add(tabela);
		} catch (DocumentException exception) {
			throw new IllegalStateException("Nao foi possivel gerar o PDF de empresas", exception);
		} finally {
			document.close();
		}
		return output.toByteArray();
	}

	private void adicionarCabecalho(PdfPTable tabela, String texto) {
		PdfPCell celula = new PdfPCell(new Phrase(texto, new Font(Font.HELVETICA, 10, Font.BOLD)));
		celula.setBackgroundColor(new java.awt.Color(230, 230, 230));
		tabela.addCell(celula);
	}

	private void validarAdmin() {
		Usuario usuarioLogado = logado();
		if (usuarioLogado == null || usuarioLogado.getRole() != Role.ADMIN) {
			throw new GlobalExceptionHandler.UnauthorizedException("Sem permissao");
		}
	}
	
	
	@Transactional
	@Caching(evict = { //update os caches
		    @CacheEvict(value = "empresas", allEntries = true),
		    @CacheEvict(value = "usuarios", allEntries = true)
		})
	public EmpresaResponseDTO createEmpresa(Empresa empresa){
		//apenas ADMIN pode criar empresas
    	Usuario usuarioLogado =logado();
	    if(usuarioLogado.getRole()!=Role.ADMIN) throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		
	    empresa.setId(null);
	    vincularUsuariosExistentes(empresa);
	    Empresa salva = empresaRepository.save(empresa);
	    return toDTO(salva);
	}
	
	
	//apenas admin pode acessar
	public EmpresaResponseDTO getEmpresaById(Long id) {
	    Empresa alvo=empresaRepository.findById(id)
	            .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Empresa não encontrada"));

	    Usuario usuarioLogado =logado();
	    if(usuarioLogado.getRole()==Role.ADMIN) return toDTO(alvo);

	    throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}
	
	//apenas admin pode acessar
	public EmpresaResponseDTO getEmpresaByNome(String nome) {
	    Empresa alvo=empresaRepository.findByNome(nome)
	            .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Empresa não encontrada"));

	    Usuario usuarioLogado =logado();
	    if(usuarioLogado.getRole()==Role.ADMIN) return toDTO(alvo);

	    throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
	}
	
	//apenas admin pode alterar empresas
	@Transactional
	@Caching(evict = { //update os caches
		    @CacheEvict(value = "empresas", allEntries = true),
		    @CacheEvict(value = "usuarios", allEntries = true)
		})
	public EmpresaResponseDTO updateEmpresa(Empresa empresa) {

	    Usuario usuarioLogado = logado();
	    if(usuarioLogado.getRole()!=Role.ADMIN)
	    	throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");

	    vincularUsuariosExistentes(empresa);
	    Empresa salva = empresaRepository.save(empresa);
	    return toDTO(salva);
	}
	
	//apenas admin pode deletar empresas
	@Transactional
	@Caching(evict = { //update os caches
		    @CacheEvict(value = "empresas", allEntries = true),
		    @CacheEvict(value = "usuarios", allEntries = true)
		})
	public void deleteEmpresa(Long id) {
		Empresa alvo=empresaRepository.findById(id)
				.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Empresa não encontrada"));
		
		Usuario usuarioLogado = logado();
		if(usuarioLogado.getRole()!=Role.ADMIN)
			throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		
		//desvinculando essa empresa dos usuarios
	    for (Usuario usuario : alvo.getUsuarios()) {
	        usuario.getEmpresas().remove(alvo);
	    }
	    
		empresaRepository.delete(alvo);
	}
	
	// Substitui os Usuarios vindos do JSON, só com id/email
	// pelas entidades gerenciadas de verdade, buscadas do banco por id.
	private void vincularUsuariosExistentes(Empresa empresa) {
		//caso a lista do frontend venha vazia
		if (empresa.getUsuarios() == null || empresa.getUsuarios().isEmpty()) {
			empresa.setUsuarios(new HashSet<>());
			return;
		}

		Set<Long> ids = empresa.getUsuarios().stream()
				.map(Usuario::getId)
				.collect(Collectors.toSet());

		//verificar se os usuarios existem
		List<Usuario> usuariosGerenciados = usuarioRepository.findAllById(ids);
		if (usuariosGerenciados.size() != ids.size()) {
			throw new GlobalExceptionHandler.ResourceNotFoundException("Um ou mais usuários informados não foram encontrados");
		}

		empresa.setUsuarios(new HashSet<>(usuariosGerenciados));
	}

}
