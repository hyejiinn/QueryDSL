package study.querydesl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;
import study.querydesl.dto.MemberDto;
import study.querydesl.dto.QMemberDto;
import study.querydesl.dto.UserDto;
import study.querydesl.entity.Member;
import study.querydesl.entity.QMember;
import study.querydesl.entity.QTeam;
import study.querydesl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.assertj.core.api.Assertions.*;
import static study.querydesl.entity.QMember.*;
import static study.querydesl.entity.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    @BeforeEach
    public void before() {
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);


        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        // member1을 찾아라.
        Member findByJPQL = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findByJPQL.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        JPAQueryFactory factory = new JPAQueryFactory(em);
//        QMember m = QMember.member;

        Member findMember = factory.
                select(member)
                .from(member)
                .where(member.username.eq("member1")) // 파라미터 바인딩 처리
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");

    }

    // 검색 조건 쿼리
    @Test
    public void search() {
        JPAQueryFactory factory = new JPAQueryFactory(em);
        Member findMember = factory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }


    @Test
    public void searchAndParam() {
        JPAQueryFactory factory = new JPAQueryFactory(em);
        Member findMember = factory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),  // ,는 and 사용하는 거랑 동일하게 동작한다.
                        member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    /**
     * 결과 조회
     * fetch() : 리스트 조회
     * fetchOne() : 단 건 조회
     * fetchFirst() : limit(1).fetchOne()
     * fetchResults() : 페이징 정보 포함 + total count 쿼리 추가 실행
     * fetchCount() : count 쿼리로 변경해서 count 수 조회
     */
    @Test
    public void resultFetch() {
        JPAQueryFactory factory = new JPAQueryFactory(em);

//        List<Member> fetch = factory
//                .selectFrom(member)
//                .fetch();
//
//        Member fetchOne = factory
//                .selectFrom(member)
//                .where(member.username.eq("member1"))
//                .fetchOne();
//
//        Member fetchFirst = factory
//                .selectFrom(member)
//                .limit(1)
//                .fetchOne();

        // 페이징에서 사용
        QueryResults<Member> results = factory
                .selectFrom(member)
                .fetchResults();

        results.getTotal(); // count 쿼리
        List<Member> content = results.getResults();

        long count = factory
                .selectFrom(member)
                .fetchCount();

    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순 desc
     * 2. 회원 이름 올림차순 asc
     * 단 2에서 회원 이름이 없으면 마지막에 출력 nulls last
     */
    @Test
    public void sort() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();

    }

    /**
     * 페이징
     */
    @Test
    public void paging1() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    /**
     * 페이징
     * 실무에서 페이징 쿼리를 작성할 때, 데이터를 조회하는 쿼리는 여러 테이블을 조인해야 하지만,
     * count 쿼리는 조인이 필요 없는 경우가 있다.
     * count 쿼리에 조인이 필요 없는 성능 최적화가 필요하다면 count 전용 쿼리를 별도로 작성해야 한다.
     */
    @Test
    public void paging2() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        QueryResults<Member> queryResults = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);
    }

    /**
     * 집합
     */
    @Test
    public void aggregation() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

    }

    /**
     * 집합
     * 팀의 이름과 각 팀의 평균 연령을 구해라.
     */
    @Test
    public void group() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * 조인 - 기본 조인
     * :팀 A에 소속된 모든 회원
     */
    @Test
    public void join() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team) // leftJoin, rightJoin 모두 가능
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result).extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void theta_join() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * 조인 - on절
     * 1. 조인 대상 필터링
     * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조인
     * JPQL: select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void join_on_filtering() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        // select 가 여러가지 타입으로 나오면 Tuple로 결과를 뽑게 됨
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

    }

    /**
     * 조인 - on절
     * 2. 연관관계 없는 엔티티 외부 조인
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     */
    @Test
    public void join_on_no_relation() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team) // 기존 조인 방법은 leftJoin(member.team, team) 이런식으로 사용,
                // 이건 team의 id로 매칭해주는데 관계 없는 테이블끼리 조인할 때는 leftJoin(team)으로 해서 사용
                .on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

    }

    @PersistenceUnit
    EntityManagerFactory emf;

    /**
     * 조인 - 페치 조인
     * 페치 조인 사용 안한 버전
     */
    @Test
    public void fetchJoinNo() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();
    }

    /**
     * 조인 - 페치 조인
     * 페치 조인 사용 버전
     */
    @Test
    public void fetchJoinUse() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isTrue();
    }

    /**
     * 서브 쿼리
     * : 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age").containsExactly(40);
    }

    /**
     * 서브 쿼리
     * : 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryGoe() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age").containsExactly(30, 40);
    }

    /**
     * 서브 쿼리
     * : in 절
     */
    @Test
    public void subQueryIn() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10)) // 20, 30, 40
                ))
                .fetch();

        assertThat(result).extracting("age").containsExactly(20, 30, 40);
    }

    /**
     * 서브 쿼리
     * : select sub query
     */
    @Test
    public void selectSubQuery() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub)
                )
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * Case문
     */
    @Test
    public void basicCase() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타")
                )
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * Case문 : 복잡한 조건
     * CaseBuilder 사용
     * -> 근데 이렇게 DB에서 조회할 때 filter를 하는 것은 별로 좋지 않다고 강사님은 말함.
     * 이런건 그냥 10 , 20 이렇게 가져와서 애플리케이션에서 처리하는게 좋음
     */
    @Test
    public void complexCase() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타")
                ).from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * 상수 더하기
     * Expressions.constant()
     */
    @Test
    public void constant() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 문자 더하기 concat
     */
    @Test
    public void concat() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        // {username}_{age}
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * 프로젝션 결과 반환 - 기본
     * 프로젝션 대상이 하나면 타입을 명확하게 지정할 수 있음.
     */
    @Test
    public void simpleProjection() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * 프로젝션 결과 반환 - 기본
     * 프로젝션 대상이 둘 이상이면 튜플이나 DTO 조회
     * 참고로 Tuple은 리포지토리 안에서만 필요할 때 쓰고, 서비스 계층으로 넘어가는건 좋지 않다고 한다.
     * -> Tuple은 querydls에 종속적이기 때문에!
     */
    @Test
    public void tupleProjection() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();


        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);

            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }


    /**
     * 프로젝션 결과 반환 - DTO 조회
     * 순수 JPA에서 DTO 조회
     */
    @Test
    public void findDtoJPQL() {
        List<MemberDto> result = em.createQuery("select new study.querydesl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * 프로젝션 결과 반환 - 프로퍼티 접근 Setter
     */
    @Test
    public void findDtoBySetter() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * 프로젝션 결과 반환 - 필드 직접 접근
     * -> getter, setter 없어도 됨
     */
    @Test
    public void findDtoByField() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * 프로젝션 결과 반환 - 생성자 사용
     */
    @Test
    public void findDtoByConstructor() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * 프로젝션 결과 반환 - 별칭이 다를 때
     */
    @Test
    public void findUserDto() {
        QMember memberSub = new QMember("memberSub");
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(JPAExpressions // 서브쿼리 역시 별칭을 줘야 할 때~~
                                .select(memberSub.age.max())
                                .from(memberSub), "age")
                ))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }

    }

    /**
     * 프로젝션 결과 반환 - DTO 생성자에 @QueryProjection
     */
    @Test
    public void findDtoByQueryProjection() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<MemberDto> result = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * 동적 쿼리 - BooleanBuilder 사용
     */
    @Test
    public void dynamicQuery_BooleanBuilder() {
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = searchMember1(usernameParam, ageParam);

        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        BooleanBuilder builder = new BooleanBuilder();
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }


    /**
     * 동적 쿼리 - Where 다중 파라미터 사용
     * -> 강사님이 추천하는 방식
     * where 안의 null은 무시된다.
     * 재활용이 가능하다.
     * 가독성이 좋다.
     */
    @Test
    public void dynamicQuery_whereParam() {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember2(usernameParam, ageParam);

        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        return queryFactory
                .selectFrom(member)
                .where(usernameEq(usernameCond), ageEq(ageCond))
                .fetch();

    }

    private BooleanExpression usernameEq(String usernameCond) {
        return usernameCond == null ? null : member.username.eq(usernameCond);
    }
    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    // 광고 상태 isValid, 날짜가 In : isServiceable
    private BooleanExpression isServiceable(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    /**
     * 수정 벌크 연산
     * 벌크 연산은 영속성 컨텍스트를 무시하고 DB에 바로 전달함
     * -> DB의 상태와 영속성 컨텍스트의 상태가 달라짐
     */
    @Test
    public void bulkUpdate() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        // count에는 영향을 받은 row 수 반환
        // member1 = 10 -> 비회원
        // member2 = 20 -> 비회원
        // member3 = 30 -> 유지
        // member4 = 40 -> 유지
        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        em.flush(); // 영속성 컨텍스트 초기화(DB와 영속성 컨텍스트가 다르기 때문에..)
        em.clear();

        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member member1 : result) {
            System.out.println("member = " + member1);
        }

    }

    @Test
    public void bulkAdd() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        long count = queryFactory
                .update(member)
                .set(member.age, member.age.add(1)) // 기존 숫자에 1 더하기
                .execute();
    }

    /**
     * 삭제 벌크 연산
     */
    @Test
    public void bulkDelete() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        long count = queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }




}
