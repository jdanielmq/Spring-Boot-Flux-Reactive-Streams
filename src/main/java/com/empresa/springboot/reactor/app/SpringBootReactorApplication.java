package com.empresa.springboot.reactor.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.empresa.springboot.reactor.app.models.Usuario;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SpringBootReactorApplication.class);
	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		Flux<Usuario> nombres =  Flux.just("andres maturana","daniel sanchez","francisco vera","juan daniel","loco pedro", "nacho zuñiga","maria muñoz","Bruce Lee", "Bruce Willis")
				.map(nombreApellido -> {
					String[] arrayNombreApellido = nombreApellido.split(" ");
					return new Usuario(arrayNombreApellido[0], arrayNombreApellido[1]);
				})
				.filter(usuario -> usuario.getNombre().toLowerCase().equals("bruce"))
				.doOnNext(usuario -> {
					if(usuario.getNombre().isEmpty()) {
						throw new RuntimeException("nombres no debe ser vacio.");
					}
					System.out.println(usuario.toString());
				
				})
				.map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});



		nombres.subscribe(
				e -> log.info(e.getNombre()),
				error -> log.error(error.getMessage()),
				new Runnable() {
					@Override
					public void run() {
						log.info("Ha finalizado la ejecución del observable con éxito");
					}
				}
			);
		
	}

}
