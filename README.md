# Домашнее задание #4: Реализация консенсуса Raft

## ⚠️ ВАЖНО: Напишите ФИО в свой PR!

## Введение

[Raft](https://raft.github.io/) - это алгоритм консенсуса для распределенных систем.

Вам необходимо дописать в существующей реализации некоторые части кода на Python:
они помечены секциями `TODO:`.

Представленный код является некоторой моделью, которую можно попробовать пощупать
руками с помощью команды `python -m src.simulator`, однако вы можете заметить
некоторые условности и упрощения по сравнению с оригинальным алгоритмом.

Ваша задача – дописать код в места `TODO:` (файлы `cluster.py` / `node.py`) в соответствии с комментариями, чтобы прошли тесты.

### Основные концепции Raft

**Состояния узла:**
- **Follower** (ведомый): взаимодействует с лидером, объявляет себя кандидатом, если не получал ответа от лидера в течение рандомизированного времени, интервал которого задавается конфигурацией (`election_timeout`)
- **Candidate** (кандидат в лидеры): инициирует выборы себя лидером, получая кворум голосов (`RequestVoteRequest`, `RequestVoteResponse`) от других узлов системы
- **Leader** (лидер): координирует репликацию логов (`AppendEntriesRequest`, `AppendEntriesResponse`), реализуя коммит по кворуму, получая подтверждения от большинства узлов

---


## Рекомендации по реализации

### Отладка

Если потребуется, можно воспользоваться CLI симулятором для отладки:

```bash
# Запустить симулятор
python -m src.simulator

# Команды в симуляторе:
raft> status              # Статус кластера
raft> tick 100            # 100 шагов
raft> leader              # Текущий лидер
raft> logs                # Показать логи
raft> propose x 42        # Добавить команду
raft> partition 1 2,3     # Разделить сеть
raft> heal                # Восстановить
```

### Виртуальное окружение

```bash
python -m venv .venv
source .venv/bin/activate  # На Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Запустить тесты
python -m pytest tests/ -v

# Запустить конкретный тест
python -m pytest tests/test_raft_implementation.py::TestLeaderElection::test_single_node_becomes_leader -v
```

---

## Полезные ссылки

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft Paper Talk](https://www.youtube.com/watch?v=YbZ3zDzDnrw)

- [Raft Visualization [Emulator]](https://raft.github.io/raftscope/index.html)
- [Raft Visualization [Tutorial]](https://thesecretlivesofdata.com/raft/)

---
