package study.querydesl.repository;

import study.querydesl.dto.MemberSearchCondition;
import study.querydesl.dto.MemberTeamDto;

import java.util.List;

public interface MemberRepositoryCustom {
    List<MemberTeamDto> search(MemberSearchCondition condition);
}
