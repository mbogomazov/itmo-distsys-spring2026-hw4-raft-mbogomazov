"""
Основные типы данных для Raft алгоритма.
"""
from dataclasses import dataclass, field
from typing import Any, List, Optional
from enum import Enum


class NodeState(Enum):
    """Состояние узла в Raft"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """Запись в логе"""
    term: int
    index: int
    command: Any
    
    def __hash__(self):
        return hash((self.term, self.index))


@dataclass
class AppendEntriesRequest:
    """RPC запрос: AppendEntries"""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResponse:
    """RPC ответ: AppendEntries"""
    term: int
    success: bool
    last_log_index: int = 0


@dataclass
class RequestVoteRequest:
    """RPC запрос: RequestVote"""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    """RPC ответ: RequestVote"""
    term: int
    vote_granted: bool


@dataclass
class RaftConfig:
    """Конфигурация Raft узла"""
    node_id: str
    election_timeout_min_ms: float = 1500
    election_timeout_max_ms: float = 3000
    heartbeat_interval_ms: float = 500
    
    def __post_init__(self):
        if self.election_timeout_min_ms >= self.election_timeout_max_ms:
            raise ValueError("election_timeout_min_ms должен быть < election_timeout_max_ms")
