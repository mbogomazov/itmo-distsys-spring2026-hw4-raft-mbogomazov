"""
Симуляция кластера Raft в одном процессе.
Используется для тестирования.
"""
import time
from typing import Dict, Optional, List, Any
from .node import RaftNode
from .types import RaftConfig, NodeState
from .types import AppendEntriesRequest, RequestVoteRequest


class RaftCluster:
    """Симуляция кластера Raft в памяти"""
    
    def __init__(self, node_ids: List[str], config_factory=None):
        """
        Args:
            node_ids: список ID узлов
            config_factory: функция для создания конфигурации для каждого узла
        """
        self.node_ids = node_ids
        self.servers: Dict[str, RaftNode] = {}
        self.network_delay = 0  # задержка в секундах для симуляции сети
        self.simulated_time_ms = 0  # симулированное время
        self.partitions: Dict[str, set] = {}  # сетевые разделения
        
        # Создать сервера
        for node_id in node_ids:
            if config_factory:
                config = config_factory(node_id)
            else:
                config = RaftConfig(node_id=node_id)
            
            server = RaftNode(config, node_ids)
            self.servers[node_id] = server
        
        # История RPC вызовов для отладки
        self.rpc_history: List[Dict[str, Any]] = []
    
    def get_leader(self) -> Optional[str]:
        """Получить ID текущего лидера (если есть)"""
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.LEADER:
                return node_id
        return None
    
    def get_leaders(self) -> List[str]:
        """Получить всех лидеров (при сетевом разделении может быть несколько)"""
        leaders = []
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.LEADER:
                leaders.append(node_id)
        return leaders
    
    def tick_ms(self, duration_ms: float = 100) -> None:
        """
        Выполнить шаг симуляции.
        
        Args:
            duration_ms: длительность шага в миллисекундах
        """
        self.simulated_time_ms += duration_ms
        
        for server in self.servers.values():
            server.tick(self.simulated_time_ms)
        
        # Обработать RPC между серверами
        self._process_rpcs()
    
    def _can_reach(self, from_id: str, to_id: str) -> bool:
        """Может ли from_id связаться с to_id"""
        if from_id == to_id:
            return True
        
        # Проверить разделения
        if from_id in self.partitions:
            if to_id in self.partitions[from_id]:
                return False
        
        return True
    
    def _process_rpcs(self) -> None:
        """Обработать RPC вызовы"""
        # Candidate отправляет RequestVote
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.CANDIDATE:
                for peer_id in server.peer_ids:
                    if not self._can_reach(node_id, peer_id):
                        continue
                    
                    peer = self.servers[peer_id]

                    req = RequestVoteRequest(
                        term=server.state.current_term,
                        candidate_id=node_id,
                        last_log_index=server.state.get_last_log_index(),
                        last_log_term=server.state.get_last_log_term(),
                    )

                    resp = peer.request_vote(req)
                    
                    # Обновить информацию candidate
                    if resp.term > server.state.current_term:
                        # Кто-то живет в более новом term - кандидат устарел, выборы прекращаем
                        server.state.set_term(resp.term)
                        server.state.become_follower(resp.term)
                        break
                    elif resp.vote_granted:
                        server.state.votes_received[peer_id] = True
                
                # Проверить, получил ли большинство голосов
                if server.state.state == NodeState.CANDIDATE:  # еще не стал follower
                    votes_count = sum(1 for granted in server.state.votes_received.values() if granted)
                    # Большинство от ВСЕХ узлов кластера (голос за себя тоже учтен)
                    needed = len(self.node_ids) // 2 + 1

                    if votes_count >= needed:
                        server.state.become_leader(server.peer_ids)
                        server._notify_state_change()
        
        # Leader отправляет AppendEntries
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.LEADER:
                for peer_id in server.peer_ids:

                    if not self._can_reach(node_id, peer_id):
                        continue

                    peer = self.servers[peer_id]
                    prev_index = server.state.next_index.get(peer_id, 0) - 1
                    prev_term = server.state.log[prev_index].term if prev_index >= 0 and prev_index < len(server.state.log) else 0
                    
                    entries = server.state.get_entries_from(prev_index + 1)
                    
                    req = AppendEntriesRequest(
                        term=server.state.current_term,
                        leader_id=node_id,
                        prev_log_index=prev_index,
                        prev_log_term=prev_term,
                        entries=entries,
                        leader_commit=server.state.commit_index,
                    )
                    
                    resp = peer.append_entries(req)
                    
                    # Обновить leader state
                    if resp.term > server.state.current_term:
                        # Есть узел с более новым term - значит, мы устаревший лидер
                        server.state.set_term(resp.term)
                        server.state.become_follower(resp.term)
                        break
                    elif resp.success:
                        match = prev_index + len(entries)
                        server.state.match_index[peer_id] = match
                        server.state.next_index[peer_id] = match + 1
                    else:
                        # Проверка prevLogIndex/prevLogTerm не прошла - откатываемся назад
                        server.state.next_index[peer_id] = max(0, resp.last_log_index + 1)
                    
                    # Попробовать продвинуть commit_index
                    self._try_advance_commit_index(server, node_id)
    
    def _try_advance_commit_index(self, leader: RaftNode, leader_id: str) -> None:
        """Попробовать продвинуть commit_index лидера"""
        # Проверить каждую запись, начиная со следующей после commit_index
        for index in range(leader.state.commit_index + 1, len(leader.state.log)):
            # Записи старых term по подсчету реплик не коммитим (Figure 8 из статьи):
            # они закоммитятся неявно вместе с первой записью текущего term
            if leader.state.log[index].term != leader.state.current_term:
                continue

            count = 1  # сам лидер
            for peer_id in leader.peer_ids:
                if leader.state.match_index.get(peer_id, -1) >= index:
                    count += 1

            needed = len(self.node_ids) // 2 + 1

            if count >= needed:
                leader.state.commit_index = index
                leader._apply_committed_entries()
    
    def partition(self, nodes1: List[str], nodes2: List[str]) -> None:
        """
        Создать сетевое разделение между двумя группами узлов.
        
        Args:
            nodes1: первая группа
            nodes2: вторая группа
        """
        # Очистить старые разделения
        self.partitions.clear()
        
        # Добавить новые разделения
        for node_id in nodes1:
            self.partitions[node_id] = set(nodes2)
        for node_id in nodes2:
            self.partitions[node_id] = set(nodes1)
    
    def heal_partition(self) -> None:
        """Исцелить все сетевые разделения"""
        self.partitions.clear()
    
    def wait_for_leader_ms(self, timeout_ms: float = 10000.0) -> Optional[str]:
        """
        Дождаться выборов лидера.
        
        Args:
            timeout_ms: максимальное время ожидания в миллисекундах
            
        Returns:
            ID лидера или None если timeout
        """
        start_time = self.simulated_time_ms
        while self.simulated_time_ms - start_time < timeout_ms:
            leader = self.get_leader()
            if leader:
                return leader
            self.tick_ms()
        return None
    
    def wait_for_replication_ms(self, timeout_ms: float = 10000.0) -> bool:
        """
        Дождаться пока все узлы синхронизируют логи и применят entries.
        
        Returns:
            True если успешно, False если timeout_ms
        """
        start_time = self.simulated_time_ms
        while self.simulated_time_ms - start_time < timeout_ms:
            # Проверить, все ли узлы имеют одинаковые логи И применили entries
            if self._all_logs_match() and self._all_entries_applied():
                return True
            self.tick_ms()
        return False
    
    def _all_entries_applied(self) -> bool:
        """Проверить, применены ли все entries на всех узлах"""
        if not self.servers:
            return True
        
        # Получить максимальный commit_index
        max_commit_index = max(
            (srv.state.commit_index for srv in self.servers.values()),
            default=-1
        )
        
        # Все узлы должны иметь log_len > max_commit_index (т.е. иметь все entries)
        # и last_applied должен быть >= max_commit_index
        for server in self.servers.values():
            if len(server.state.log) <= max_commit_index:
                return False
            if server.state.last_applied < max_commit_index:
                return False
        
        return True
    
    def _all_logs_match(self) -> bool:
        """Проверить, совпадают ли логи всех узлов"""
        if not self.servers:
            return True
        
        first_log = list(self.servers.values())[0].state.log
        for server in self.servers.values():
            if len(server.state.log) != len(first_log):
                return False
            for i, entry in enumerate(server.state.log):
                if first_log[i].term != entry.term or first_log[i].command != entry.command:
                    return False
        return True
    
    def get_cluster_state(self) -> Dict[str, Any]:
        """Получить состояние всего кластера"""
        return {
            "simulated_time": self.simulated_time_ms,
            "leader": self.get_leader(),
            "servers": {
                node_id: server.get_state()
                for node_id, server in self.servers.items()
            }
        }
