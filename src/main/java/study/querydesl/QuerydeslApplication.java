package study.querydesl;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import javax.persistence.EntityManager;

@SpringBootApplication
public class QuerydeslApplication {

	public static void main(String[] args) {
		SpringApplication.run(QuerydeslApplication.class, args);
	}

	// 이렇게 스프링 빈으로 등록해도 됨
//	@Bean
//	JPAQueryFactory jpaQueryFactory(EntityManager em) {
//		return new JPAQueryFactory(em);
//	}

}
