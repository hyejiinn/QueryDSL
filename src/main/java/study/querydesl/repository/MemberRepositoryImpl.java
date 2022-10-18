package study.querydesl.repository;

import com.querydsl.core.QueryResults;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.JPQLQuery;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.data.support.PageableExecutionUtils;
import study.querydesl.dto.MemberSearchCondition;
import study.querydesl.dto.MemberTeamDto;
import study.querydesl.dto.QMemberTeamDto;
import study.querydesl.entity.Member;

import javax.persistence.EntityManager;
import java.util.List;

import static org.springframework.util.StringUtils.hasText;
import static study.querydesl.entity.QMember.member;
import static study.querydesl.entity.QTeam.team;

public class MemberRepositoryImpl extends QuerydslRepositorySupport implements MemberRepositoryCustom {


    private final JPAQueryFactory queryFactory;

    public MemberRepositoryImpl(EntityManager em) {
        super(Member.class);
        this.queryFactory = new JPAQueryFactory(em);

    }
//
//    public MemberRepositoryImpl(EntityManager em) {
//        this.queryFactory = new JPAQueryFactory(em);
//    }


    @Override
    public List<MemberTeamDto> search(MemberSearchCondition condition) {

        List<MemberTeamDto> result = from(member)
                .leftJoin(member.team, team)
                .where(
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                )
                .select(
                        new QMemberTeamDto(
                                member.id,
                                member.username,
                                member.age,
                                team.id,
                                team.name
                        )
                ).fetch();
        return result;
//        return queryFactory
//                .select(new QMemberTeamDto(
//                        member.id,
//                        member.username,
//                        member.age,
//                        team.id,
//                        team.name
//                )).from(member)
//                .leftJoin(member.team, team)
//                .where(
//                        usernameEq(condition.getUsername()),
//                        teamNameEq(condition.getTeamName()),
//                        ageGoe(condition.getAgeGoe()),
//                        ageLoe(condition.getAgeLoe())
//                )
//                .fetch();
    }

//    @Override
//    public Page<MemberTeamDto> searchPageSimple(MemberSearchCondition condition, Pageable pageable) {
//        QueryResults<MemberTeamDto> result = queryFactory
//                .select(
//                        new QMemberTeamDto(
//                                member.id,
//                                member.username,
//                                member.age,
//                                team.id,
//                                team.name
//                        )
//                )
//                .from(member)
//                .leftJoin(member.team, team)
//                .where(
//                        usernameEq(condition.getUsername()),
//                        teamNameEq(condition.getTeamName()),
//                        ageGoe(condition.getAgeGoe()),
//                        ageLoe(condition.getAgeLoe())
//                )
//                .offset(pageable.getOffset()) // 몇번째부터 시작할거야?
//                .limit(pageable.getPageSize()) // 한 페이지에 몇 개 가져올 것인가?
//                .fetchResults();
//
//        List<MemberTeamDto> content = result.getResults();
//        long total = result.getTotal();
//        return new PageImpl<>(content, pageable, total);
//    }

    @Override
    public Page<MemberTeamDto> searchPageSimple(MemberSearchCondition condition, Pageable pageable) {
        JPQLQuery<MemberTeamDto> jpaQuery = from(member)
                .leftJoin(member.team, team)
                .where(
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                ).select(
                        new QMemberTeamDto(
                                member.id,
                                member.username,
                                member.age,
                                team.id,
                                team.name
                        )
                );

        JPQLQuery<MemberTeamDto> query = getQuerydsl().applyPagination(pageable, jpaQuery);

        QueryResults<MemberTeamDto> result = query.fetchResults();

        List<MemberTeamDto> content = result.getResults();
        long total = result.getTotal();
        return new PageImpl<>(content, pageable, total);
    }

    // 데이터 내용과 전체 카운트를 별도로 조회하는 방법
    @Override
    public Page<MemberTeamDto> searchPageComplex(MemberSearchCondition condition, Pageable pageable) {
        List<MemberTeamDto> content = queryFactory
                .select(
                        new QMemberTeamDto(
                                member.id,
                                member.username,
                                member.age,
                                team.id,
                                team.name
                        )
                ).from(member)
                .leftJoin(member.team, team)
                .where(
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                )
                .offset(pageable.getOffset())
                .limit(pageable.getPageSize())
                .fetch();

        // 별도로 조회하면 count 쿼리 좀 더 최적화 가능
        JPAQuery<Member> countQuery = queryFactory
                .select(member)
                .from(member)
                .leftJoin(member.team, team)
                .where(
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                );
//                .fetchCount();

        // Count 쿼리 최적화
        // count 쿼리가 생략 가능한 경우에 생략해서 처리해준다 스프링 데이터 라이브러리가 처리!
        // 페이지 시작이면서 컨텐츠 사이즈가 페이지 사이즈보다 작을 때
        // 마지막 페이지일 때 (offset + 컨텐츠 사이즈를 더해서 전체 사이즈 구함)
//        return PageableExecutionUtils.getPage(content, pageable, () -> countQuery.fetchCount());
        return PageableExecutionUtils.getPage(content, pageable, countQuery::fetchCount);
//        return new PageImpl<>(content, pageable, total);
    }

    private BooleanExpression usernameEq(String username) {
        return hasText(username) ? member.username.eq(username) : null;
    }

    private BooleanExpression teamNameEq(String teamName) {
        return hasText(teamName) ? team.name.eq(teamName) : null;
    }

    private BooleanExpression ageGoe(Integer ageGoe) {
        return ageGoe != null ? member.age.goe(ageGoe) : null;
    }

    private BooleanExpression ageLoe(Integer ageLoe) {
        return ageLoe != null ? member.age.loe(ageLoe) : null;
    }
}
