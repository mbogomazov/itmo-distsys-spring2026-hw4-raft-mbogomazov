"""
CLI симулятор для Raft кластера.
Позволяет интерактивно управлять кластером и видеть его состояние.
"""
import sys
import json
from typing import Optional
from src.raft import RaftCluster, RaftConfig, NodeState


class RaftSimulator:
    """Интерактивный симулятор Raft"""
    
    def __init__(self, num_nodes: int = 3):
        self.num_nodes = num_nodes
        self.node_ids = [f"node{i+1}" for i in range(num_nodes)]
        self.cluster = RaftCluster(self.node_ids)
        self.history = []
    
    def print_header(self):
        """Вывести заголовок"""
        print("\n" + "="*80)
        print("RAFT CONSENSUS SIMULATOR")
        print("="*80)
        print(f"Кластер: {self.num_nodes} узлов")
        print("\nКоманды:")
        print("  status              - показать статус кластера")
        print("  tick [N]            - выполнить N шагов симуляции (по умолчанию 1 tick = 100 ms)")
        print("  leader              - показать текущего лидера")
        print("  logs                - показать логи всех узлов")
        print("  propose KEY VALUE   - предложить команду на добавление")
        print("  partition N1 N2     - разделить сеть на две группы")
        print("                        (например: partition 1 2,3)")
        print("  heal                - исцелить сетевое разделение")
        print("  state_machine       - показать state machine")
        print("  help                - показать эту справку")
        print("  quit                - выход")
        print("="*80 + "\n")
    
    def status(self):
        """Показать статус кластера"""
        cluster_state = self.cluster.get_cluster_state()
        
        print(f"\nВремя симуляции: {cluster_state['simulated_time']:.2f}s")
        print(f"Лидер: {cluster_state['leader']}")
        print(f"\n{'Node':<10} {'State':<12} {'Term':<8} {'Log Len':<10} {'Committed':<10}")
        print("-" * 60)
        
        for node_id, state_info in cluster_state['servers'].items():
            print(f"{node_id:<10} {state_info['state']:<12} {state_info['term']:<8} "
                  f"{state_info['log_length']:<10} {state_info['commit_index']:<10}")
    
    def tick(self, steps: int = 1):
        """Выполнить шаги симуляции"""
        for _ in range(steps):
            self.cluster.tick_ms(100)
        print(f"✓ Выполнено {steps} шагов симуляции")
    
    def show_leader(self):
        """Показать текущего лидера"""
        leader = self.cluster.get_leader()
        if leader:
            print(f"Текущий лидер: {leader}")
            term = self.cluster.servers[leader].state.current_term
            print(f"Term: {term}")
        else:
            print("⚠ Лидера нет (нет большинства)")
    
    def show_logs(self):
        """Показать логи всех узлов"""
        print("\n" + "-"*80)
        for node_id, server in self.cluster.servers.items():
            print(f"\n{node_id} (term={server.state.current_term}, "
                  f"state={server.state.state.value}, "
                  f"commit_index={server.state.commit_index}):")
            
            if not server.state.log:
                print("  [пусто]")
            else:
                for i, entry in enumerate(server.state.log):
                    committed = "✓" if i <= server.state.commit_index else " "
                    applied = "✓" if i < server.state.last_applied else " "
                    print(f"  [{committed}][{applied}] [{i}] term={entry.term}: {entry.command}")
        print("-"*80 + "\n")
    
    def propose(self, key: str, value: str):
        """Предложить команду"""
        leader = self.cluster.get_leader()
        if not leader:
            print("⚠ Нет лидера, команда отклонена")
            return
        
        try:
            # Попытаться конвертировать value в число
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass  # Оставить как строка
            
            command = {"type": "set", "key": key, "value": value}
            success = self.cluster.servers[leader].propose(command)
            
            if success:
                print(f"✓ Команда предложена лидеру {leader}")
                # Симулировать репликацию
                for _ in range(50):
                    self.cluster.tick_ms(0.01)
                print(f"✓ Команда реплицирована и применена")
            else:
                print("⚠ Команда отклонена")
        except Exception as e:
            print(f"✗ Ошибка: {e}")
    
    def partition(self, group1_str: str, group2_str: str):
        """Разделить сеть"""
        try:
            # Парсить группы (например "1,2" -> ["node1", "node2"])
            def parse_group(s):
                nodes = []
                for item in s.split(","):
                    item = item.strip()
                    if item.isdigit():
                        nodes.append(f"node{item}")
                    else:
                        nodes.append(item)
                return nodes
            
            group1 = parse_group(group1_str)
            group2 = parse_group(group2_str)
            
            # Валидировать
            for node in group1 + group2:
                if node not in self.cluster.node_ids:
                    print(f"✗ Неизвестный узел: {node}")
                    return
            
            self.cluster.partition(group1, group2)
            print(f"✓ Сеть разделена:")
            print(f"  Группа 1: {group1}")
            print(f"  Группа 2: {group2}")
            
        except Exception as e:
            print(f"✗ Ошибка: {e}")
    
    def heal(self):
        """Исцелить разделение"""
        self.cluster.heal_partition()
        print("✓ Сетевое разделение исцелено")
    
    def show_state_machine(self):
        """Показать state machine всех узлов"""
        print("\n" + "-"*80)
        for node_id, server in self.cluster.servers.items():
            print(f"{node_id}: {json.dumps(server.state_machine, indent=2)}")
        print("-"*80 + "\n")
    
    def run(self):
        """Запустить интерактивный цикл"""
        self.print_header()
        
        while True:
            try:
                cmd_input = input("raft> ").strip()
                
                if not cmd_input:
                    continue
                
                parts = cmd_input.split()
                cmd = parts[0].lower()
                args = parts[1:] if len(parts) > 1 else []
                
                if cmd == "status":
                    self.status()
                
                elif cmd == "tick":
                    steps = int(args[0]) if args else 1
                    self.tick(steps)
                
                elif cmd == "leader":
                    self.show_leader()
                
                elif cmd == "logs":
                    self.show_logs()
                
                elif cmd == "propose":
                    if len(args) >= 2:
                        self.propose(args[0], args[1])
                    else:
                        print("✗ Использование: propose KEY VALUE")
                
                elif cmd == "partition":
                    if len(args) >= 2:
                        self.partition(args[0], args[1])
                    else:
                        print("✗ Использование: partition GROUP1 GROUP2")
                        print("  Например: partition 1 2,3")
                
                elif cmd == "heal":
                    self.heal()
                
                elif cmd == "state_machine":
                    self.show_state_machine()
                
                elif cmd == "help":
                    self.print_header()
                
                elif cmd == "quit" or cmd == "exit":
                    print("До свидания!")
                    break
                
                else:
                    print(f"✗ Неизвестная команда: {cmd}")
            
            except KeyboardInterrupt:
                print("\nДо свидания!")
                break
            except Exception as e:
                print(f"✗ Ошибка: {e}")


if __name__ == "__main__":
    simulator = RaftSimulator(num_nodes=5)
    simulator.run()
