package org.axonframework.kafka.example.sender;

import java.time.LocalDate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.ResponseEntity;

import com.google.common.base.Predicates;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableSwagger2
@Import({ MessagingConfiguration.class, AxonConfiguration.class })
public class AxonKafkaSenderApplication {

  public static void main(String[] args) {
    SpringApplication.run(AxonKafkaSenderApplication.class, args);
  }

  @Bean
  public Docket newsApi() {
    // @formatter:off
      return new Docket(DocumentationType.SWAGGER_2)
              .select()
                .apis(RequestHandlerSelectors.any())
                .paths(Predicates.not(PathSelectors.regex("/error.*")))
                .build()
              .pathMapping("/")
              .apiInfo(new ApiInfoBuilder().title("Kafka Sender API").build())
              .directModelSubstitute(LocalDate.class,
                  String.class)
              .genericModelSubstitutes(ResponseEntity.class)
              .enableUrlTemplating(true) 
              ;
      // @formatter:on
  }
}
