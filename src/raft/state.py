"""
Управление состоянием узла Raft.
Содержит персистентное и нестабильное состояние.
"""
from typing import Optional, List, Dict, Any
from .types import NodeState, LogEntry, RaftConfig


class RaftState:
    """Класс для управления состоянием Raft узла"""
    
    def __init__(self, config: RaftConfig):
        self.config = config
        self.node_id = config.node_id
        
        # === Персистентное состояние (должно писаться на диск) ===
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []  # лог все команды, 1-indexed логически
        
        # === Нестабильное состояние ===
        self.state: NodeState = NodeState.FOLLOWER
        self.commit_index: int = -1  # индекс наивысшей закомиченной команды (-1 = нет ничего)
        self.last_applied: int = -1  # индекс последней применной команды (-1 = нет ничего)
        
        # === Нестабильное состояние лидера (забывается при смене лидера) ===
        self.next_index: Dict[str, int] = {}  # для каждого сервера
        self.match_index: Dict[str, int] = {}  # для каждого сервера
        
        # === Состояние для выборов ===
        self.votes_received: Dict[str, bool] = {}
    
    def increment_term(self) -> int:
        """Увеличить текущий term"""
        self.current_term += 1
        self.voted_for = None  # при увеличении term сбрасываем голос
        return self.current_term
    
    def set_term(self, term: int):
        """Установить новый term (если он больше текущего)"""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
    
    def vote_for(self, candidate_id: str):
        """Проголосовать за кандидата"""
        if self.voted_for is None:
            self.voted_for = candidate_id
            return True
        return self.voted_for == candidate_id
    
    def become_follower(self, term: int):
        """Перейти в режим follower"""
        self.set_term(term)
        self.state = NodeState.FOLLOWER
        self.votes_received.clear()
    
    def become_candidate(self) -> int:
        """Перейти в режим candidate и начать выборы"""
        term = self.increment_term()
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id: True}
        return term
    
    def become_leader(self, peer_ids: List[str]):
        """Перейти в режим leader"""
        self.state = NodeState.LEADER
        
        # Инициализировать next_index и match_index
        next_index = len(self.log)
        self.next_index = {peer_id: next_index for peer_id in peer_ids}
        self.match_index = {peer_id: 0 for peer_id in peer_ids}
    
    def append_entry(self, term: int, command: Any) -> LogEntry:
        """Добавить запись в лог"""
        index = len(self.log)
        entry = LogEntry(term=term, index=index, command=command)
        self.log.append(entry)
        return entry
    
    def get_last_log_index(self) -> int:
        """Получить индекс последней записи"""
        return len(self.log) - 1 if self.log else 0
    
    def get_last_log_term(self) -> int:
        """Получить term последней записи"""
        if not self.log:
            return 0
        return self.log[-1].term
    
    def get_log_entry(self, index: int) -> Optional[LogEntry]:
        """Получить запись по индексу (0-indexed)"""
        if 0 <= index < len(self.log):
            return self.log[index]
        return None
    
    def get_entries_from(self, index: int) -> List[LogEntry]:
        """Получить все записи начиная с index (0-indexed)"""
        if index >= len(self.log):
            return []
        return self.log[index:]
    
    def truncate_log(self, index: int):
        """Обрезать лог до index (0-indexed, exclusive)"""
        if 0 <= index < len(self.log):
            self.log = self.log[:index]
    
    def get_entries_range(self, start: int, end: int) -> List[LogEntry]:
        """Получить записи в диапазоне [start, end)"""
        return self.log[start:end]
