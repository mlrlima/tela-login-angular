
package service;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dto.EmpresaRelacionadaDTO;
import dto.UsuarioRelacionadoDTO;
import dto.UsuarioResponseDTO;
import exception.GlobalExceptionHandler;
import model.Empresa;
import model.Role;
import model.Usuario;
import repository.EmpresaRepository;
import repository.UsuarioRepository;


@Service //servico gerenciado pelo Spring
public class UsuarioService implements Serializable {
	private static final long serialVersionUID=1L;
	
	@Autowired
	private UsuarioRepository usuarioRepository;
	
	@Autowired
	private EmpresaRepository empresaRepository;
	
	@Autowired
	private PetService petService;
	
	
	private Usuario logado() { // verifica se o usuario logado tem autorizacao e retorna ele
	    var auth = SecurityContextHolder.getContext().getAuthentication();
	    if (auth == null || !auth.isAuthenticated()) return null;
	    return (Usuario) auth.getPrincipal();
	}
	
	
	public boolean isAdminLogado() {
	    Usuario usuarioLogado = logado();
	    return usuarioLogado != null && usuarioLogado.getRole() == Role.ADMIN;
	}
	
	@Cacheable(value = "usuarios", key = "#pageable.pageNumber + '-' + #pageable.pageSize",
				condition = "#root.target.isAdminLogado()") //cache por pagina
	public Page<UsuarioResponseDTO> getAllUsuarios(Pageable pageable){ //implementa paginacao
	    Usuario usuarioLogado = logado();

	    Page<Usuario> usuarios;

	    if(usuarioLogado.getRole() == Role.ADMIN) {
	        usuarios = usuarioRepository.findAll(pageable);
	    } else {
	    	//se o usuario nao for admin, retorna apenas ele mesmo
	        Usuario usuarioComum = usuarioRepository.findById(usuarioLogado.getId())
	                .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
	        usuarios = new PageImpl<>(List.of(usuarioComum), pageable, 1);
	    }

	    return usuarios.map(this::toDTO);
	}

	public byte[] gerarPdfUsuarios() {
		Usuario usuarioLogado = logado();
		if (usuarioLogado == null) {
			throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		}

		List<Usuario> usuarios = usuarioLogado.getRole() == Role.ADMIN
				? usuarioRepository.findAll()
				: List.of(usuarioRepository.findById(usuarioLogado.getId())
						.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado")));

		Document document = new Document();
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			PdfWriter.getInstance(document, output);
			document.open();
			document.add(new Paragraph("Relatório de Usuários", new Font(Font.HELVETICA, 16, Font.BOLD)));
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
			document.add(new Paragraph("PDF gerado em: " + LocalDateTime.now().format(formatter)));
			document.add(new Paragraph(" "));

			PdfPTable tabela = new PdfPTable(new float[] { 1, 3, 4, 2, 5 });
			tabela.setWidthPercentage(100);
			adicionarCabecalhoUsuario(tabela, "ID");
			adicionarCabecalhoUsuario(tabela, "Nome");
			adicionarCabecalhoUsuario(tabela, "E-mail");
			adicionarCabecalhoUsuario(tabela, "Perfil");
			adicionarCabecalhoUsuario(tabela, "Empresas relacionadas");

			usuarios.stream()
					.sorted(Comparator.comparing(Usuario::getNome, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)))
					.forEach(usuario -> {
						tabela.addCell(String.valueOf(usuario.getId()));
						tabela.addCell(usuario.getNome());
						tabela.addCell(usuario.getEmail());
						tabela.addCell(usuario.getRole().name());
						String empresas = usuario.getEmpresas().stream()
								.map(Empresa::getNome)
								.sorted(String.CASE_INSENSITIVE_ORDER)
								.collect(Collectors.joining(", "));
						tabela.addCell(empresas.isEmpty() ? "Nenhuma empresa vinculada" : empresas);
					});

			document.add(tabela);
		} catch (DocumentException exception) {
			throw new IllegalStateException("Não foi possível gerar o PDF de usuários", exception);
		} finally {
			document.close();
		}
		return output.toByteArray();
	}

	private void adicionarCabecalhoUsuario(PdfPTable tabela, String texto) {
		PdfPCell celula = new PdfPCell(new Phrase(texto, new Font(Font.HELVETICA, 10, Font.BOLD)));
		celula.setBackgroundColor(new java.awt.Color(230, 230, 230));
		tabela.addCell(celula);
	}
	
	@Transactional
	@CacheEvict(value = "usuarios", allEntries = true) //update o cache
	public UsuarioResponseDTO createUsuario(Usuario usuario) {
		usuario.setId(null);
		usuario.setRole(Role.USER); //sempre cria com a role USER
		vincularEmpresasExistentes(usuario);
		Usuario salvo = usuarioRepository.save(usuario);
		return toDTO(salvo);
	}
	

	//@Cacheable(value = "usuarioPorId", )
	public Usuario buscarUsuarioNoCache(Long id) {
	    return usuarioRepository.findById(id)
	        .orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
	}
	public UsuarioResponseDTO getUsuarioById(Long id) {
    	Usuario usuarioLogado = logado();
    	Usuario alvo = buscarUsuarioNoCache(id);
        
        //verifica se eh ADMIN ou o proprio usuario
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(alvo.getId())) {
                throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
        }
        
        return toDTO(alvo);
	}
    
	
	public UsuarioRelacionadoDTO getUsuarioByEmail(String email) {
	    Usuario alvo = usuarioRepository.findByEmail(email);
	    
	    if(alvo==null)
	    	throw new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado");
	    
	    Usuario usuarioLogado = logado();
        //verifica se eh ADMIN ou o proprio usuario
        if(usuarioLogado.getRole() != Role.ADMIN &&
                !usuarioLogado.getId().equals(alvo.getId())) {
                throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
        }

	    return new UsuarioRelacionadoDTO(alvo.getId(), alvo.getEmail());
	}
	
	public Usuario getUsuarioByEmailAndSenha(String email, String senha){
		Usuario usuario=usuarioRepository.findByEmail(email);
		
		if(usuario==null || !usuario.getSenha().equals(senha)) return null;
		
		return usuario;
	}

	@Transactional
	@Caching(evict = { //update os caches
		    @CacheEvict(value = "empresas", allEntries = true),
		    @CacheEvict(value = "usuarios", allEntries = true) //update o cache da empresa caso o usuario tenha mudado o email
		})
	public UsuarioResponseDTO updateUsuario(Usuario usuario) {
		Usuario usuarioLogado = logado();
		
		//verifica se eh ADMIN ou o proprio usuario
		if(usuarioLogado.getRole() != Role.ADMIN &&
		!usuarioLogado.getId().equals(usuario.getId())) {
			throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
		}
		
		//deixar a senha antiga
		if (usuario.getSenha() == null || usuario.getSenha().isBlank()) {
			
			Usuario existente = usuarioRepository.findById(usuario.getId())
			.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
			
			usuario.setSenha(existente.getSenha());
		
		}else if(usuario.getSenha().length() < 4) {
			throw new GlobalExceptionHandler.UnauthorizedException("A senha dever ter no mínimo 4 caracteres.");
        }else{
			//encriptar nova senha
			String encryptedPassword = new BCryptPasswordEncoder().encode(usuario.getSenha());
	        usuario.setSenha(encryptedPassword);
		}
		
		vincularEmpresasExistentes(usuario);
		
		Usuario salvo = usuarioRepository.save(usuario);
		return toDTO(salvo);
	}
	
	@Transactional
	@Caching(evict = { //update os caches
		    @CacheEvict(value = "empresas", allEntries = true),
		    @CacheEvict(value = "usuarios", allEntries = true)
		})
    public void deleteUsuario(Long id) {
		 Usuario alvo = usuarioRepository.findById(id)
	        		.orElseThrow(() -> new GlobalExceptionHandler.ResourceNotFoundException("Usuário não encontrado"));
		
		Usuario usuarioLogado = logado();

        if (usuarioLogado.getRole() != Role.ADMIN &&
            !usuarioLogado.getId().equals(id)) {

        	throw new GlobalExceptionHandler.UnauthorizedException("Sem permissão");
        }

        //deletar pets desse usuario
        petService.deletePetsUsuario(alvo);
        
		//desvinculando esse usuario das empresas
	    for (Empresa empresa : alvo.getEmpresas()) {
	        empresa.getUsuarios().remove(alvo);
	    }
        
        usuarioRepository.delete(alvo);
    }
	
	// Converte a entidade em DTO, sem senha e sem risco de loop
	private UsuarioResponseDTO toDTO(Usuario usuario) {
		List<EmpresaRelacionadaDTO> empresas = usuario.getEmpresas().stream()
				.map(e -> new EmpresaRelacionadaDTO(e.getId(), e.getNome()))
				.collect(Collectors.toList());
		
		return new UsuarioResponseDTO(
				usuario.getId(),
				usuario.getEmail(),
				usuario.getNome(),
				usuario.getRole(),
				empresas
		);
	}
	
	// Substitui as Empresas vindos do JSON
	// pelas entidades gerenciadas de verdade, buscadas do banco por id.
	private void vincularEmpresasExistentes(Usuario usuario) {
		//caso a lista do frontend venha vazia
		if (usuario.getEmpresas() == null || usuario.getEmpresas().isEmpty()) {
			usuario.setEmpresas(new HashSet<>());
			return;
		}

		Set<Long> ids = usuario.getEmpresas().stream()
				.map(Empresa::getId)
				.collect(Collectors.toSet());

		List<Empresa> empresasGerenciadas = empresaRepository.findAllById(ids);

		usuario.setEmpresas(new HashSet<>(empresasGerenciadas));
	}

}
