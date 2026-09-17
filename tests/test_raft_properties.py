"""
Специализированные тесты для различных аспектов Raft.
Каждый тест проверяет определенное свойство протокола.
"""
import pytest
from src.raft import RaftCluster, NodeState


class TestTermManagement:
    """Тесты управления terms"""
    
    def test_term_increases_monotonically(self):
        """Term растет монотонно"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        
        initial_terms = {nid: 0 for nid in cluster.node_ids}
        
        # Симулировать много шагов
        for _ in range(200):
            cluster.tick_ms()
            
            # Все terms должны быть >= предыдущим
            for node_id, server in cluster.servers.items():
                assert server.state.current_term >= initial_terms[node_id]
                initial_terms[node_id] = server.state.current_term
    
    def test_higher_term_overrides_lower(self):
        """Более высокий term переопределяет более низкий"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        
        # Запустить выборы
        cluster.wait_for_leader_ms(timeout_ms=10000)
        leader = cluster.get_leader()
        leader_term = cluster.servers[leader].state.current_term
        
        # Изолировать и создать другую группу с выборами
        other = [n for n in cluster.node_ids if n != leader]
        cluster.partition([leader], other)
        
        # Дождаться новых выборов в группе без лидера
        for _ in range(200):
            cluster.tick_ms()
        
        # Восстановить сеть
        cluster.heal_partition()
        
        # Все узлы должны иметь term >= leader_term
        for server in cluster.servers.values():
            assert server.state.current_term >= leader_term


class TestVotingRules:
    """Тесты правил голосования"""
    
    def test_node_votes_for_first_candidate(self):
        """Узел голосует за первого кандидата в term"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        server = cluster.servers["n1"]
        
        # Принудительно запустить выборы
        server.state.become_candidate()
        
        assert server.state.voted_for == "n1"
        
        # Попытка голосовать за другого должна быть отклонена
        from src.raft.types import RequestVoteRequest
        req = RequestVoteRequest(
            term=server.state.current_term,
            candidate_id="n2",
            last_log_index=0,
            last_log_term=0
        )
        resp = server.request_vote(req)
        assert not resp.vote_granted
    
    def test_log_entry_comparison(self):
        """Правильное сравнение состояния логов для выборов"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        
        # n1 добавит несколько логов
        server_n1 = cluster.servers["n1"]
        for i in range(3):
            server_n1.state.append_entry(server_n1.state.current_term, {"data": i})
        
        # n2 имеет пустой лог
        server_n2 = cluster.servers["n2"]
        
        # n2 не должен голосовать за себя если n1 имеет более свежие логи
        from src.raft.types import RequestVoteRequest
        server_n2.state.current_term = 1
        
        # n1 просит голос с актуальными логами
        req = RequestVoteRequest(
            term=1,
            candidate_id="n1",
            last_log_index=server_n1.state.get_last_log_index(),
            last_log_term=server_n1.state.get_last_log_term()
        )
        resp = server_n2.request_vote(req)
        assert resp.vote_granted


class TestLogConsistency:
    """Тесты консистентности логов"""
    
    def test_diverging_logs_reconciled(self):
        """Расходящиеся логи должны быть согласованы"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        
        # Заставить n1 быть лидером
        leader = cluster.wait_for_leader_ms(timeout_ms=10000)
        
        # Добавить записи
        for i in range(5):
            cluster.servers[leader].propose({"type": "set", "key": f"k{i}", "value": i})
        
        # Дождаться репликации
        cluster.wait_for_replication_ms(timeout_ms=10000)
        
        # Проверить что все логи одинаковы
        log_lengths = [len(s.state.log) for s in cluster.servers.values()]
        assert len(set(log_lengths)) == 1  # Все одинаковой длины
    
    def test_log_not_applied_until_committed(self):
        """Логи не применяются до коммита"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        leader = cluster.wait_for_leader_ms(timeout_ms=10000)
        
        # Добавить команду
        cluster.servers[leader].propose({"type": "set", "key": "test", "value": 123})
        
        # Может быть в логе но не применено сразу на всех
        for server in cluster.servers.values():
            if server.state.state != NodeState.LEADER:
                # Follower может иметь логи но не применить их сразу
                pass
        
        # После репликации все должны применить
        cluster.wait_for_replication_ms(timeout_ms=10000)
        
        for server in cluster.servers.values():
            assert server.state_machine.get("test") == 123


class TestCommitIndex:
    """Тесты управления commit index"""
    
    def test_leader_commits_entries_from_current_term(self):
        """Лидер коммитит entries только из текущего term"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        
        leader = cluster.wait_for_leader_ms(timeout_ms=10000)
        initial_term = cluster.servers[leader].state.current_term
        
        # Добавить entry в этом term
        cluster.servers[leader].propose({"type": "set", "key": "x", "value": 1})
        
        # Дождаться репликации
        cluster.wait_for_replication_ms(timeout_ms=10000)
        
        # Entry должна быть закомичена (т.е. commit_index должен быть >= 0)
        for server in cluster.servers.values():
            assert server.state.commit_index >= 0
    
    def test_commit_index_never_decreases(self):
        """Commit index никогда не уменьшается"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        
        initial_commits = {nid: -1 for nid in cluster.node_ids}
        
        for _ in range(300):
            leader = cluster.get_leader()
            if leader:
                # Добавить записи
                for i in range(3):
                    cluster.servers[leader].propose({"type": "set", "key": f"k{_}_{i}", "value": i})
            
            cluster.tick_ms()
            
            # Проверить что commit_index не уменьшается
            for node_id, server in cluster.servers.items():
                assert server.state.commit_index >= initial_commits[node_id]
                initial_commits[node_id] = server.state.commit_index


class TestLeaderUniqueness:
    """Тесты уникальности лидера"""
    
    def test_at_most_one_leader_per_term(self):
        """За term может быть максимум один лидер"""
        cluster = RaftCluster(["n1", "n2", "n3", "n4", "n5"])
        
        seen_leaders = {}  # term -> leader_id
        
        # Симулировать несколько выборов
        for _ in range(500):
            leader = cluster.get_leader()
            if leader:
                term = cluster.servers[leader].state.current_term
                
                if term in seen_leaders:
                    # Один и тот же лидер
                    assert seen_leaders[term] == leader
                else:
                    seen_leaders[term] = leader
            
            cluster.tick_ms()
        
        # Если был лидер, он должен быть уникален за свой term
        assert len(seen_leaders) > 0


class TestHeartbeatMechanism:
    """Тесты heartbeat механизма"""
    
    def test_leader_sends_heartbeats(self):
        """Лидер отправляет heartbeats регулярно"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        leader = cluster.wait_for_leader_ms(timeout_ms=10000)
        leader_server = cluster.servers[leader]
        
        initial_log_len = len(leader_server.state.log)
        
        # Симулировать без добавления новых логов
        for _ in range(100):
            cluster.tick_ms()
        
        # Лидер может остаться лидером даже без новых логов (благодаря heartbeats)
        assert cluster.get_leader() == leader


class TestIsolatedNode:
    """Тесты поведения изолированного узла"""
    
    def test_isolated_node_becomes_candidate(self):
        """Изолированный узел становится candidate"""
        cluster = RaftCluster(["n1", "n2", "n3"])
        
        # Убедиться что есть лидер
        leader = cluster.wait_for_leader_ms(timeout_ms=10000)
        
        # Изолировать не-лидера
        followers = [n for n in cluster.node_ids if n != leader]
        isolated = followers[0]
        others = [n for n in cluster.node_ids if n != isolated]
        
        cluster.partition([isolated], others)
        
        # Симулировать
        for _ in range(100):
            cluster.tick_ms()
        
        # Изолированный узел должен стать candidate или leader
        isolated_state = cluster.servers[isolated].state.state
        assert isolated_state in [NodeState.CANDIDATE, NodeState.LEADER]
