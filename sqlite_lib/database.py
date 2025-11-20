import sqlite3
from datetime import datetime, timedelta

def init_from_file(db_path='marketing_digest.db', schema_file='schema.sql'):
    """Инициализация базы данных из SQL файла схемы
    
    Args:
        db_path (str): Путь к файлу базы данных SQLite.
        schema_file (str): Путь к SQL файлу со схемой базы данных.
    """
    
    conn = sqlite3.connect(db_path)
    with open(schema_file, 'r', encoding='utf-8') as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()
    print(f"✅ База данных создана из {schema_file}")


class DataAggregator:
    def __init__(self, db_path='/Users/aleksejrusakov/Python/sql-lite-lib/marketing_digest.db'):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
    def add_project(self, name, vk_cabinet_id=None, 
                    yandex_cabinet_id=None, mytracker_project_id=None):
        """Добавить новый проект"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO projects (name, vk_cabinet_id, yandex_cabinet_id, mytracker_project_id)
            VALUES (?, ?, ?, ?)
        ''', (name, vk_cabinet_id, yandex_cabinet_id, mytracker_project_id))
        self.conn.commit()
        return cursor.lastrowid
    
    def get_list_of_projects(self):
        """Получить список всех проектов"""
        cursor = self.conn.cursor()
        cursor.execute('''
                       SELECT 
                            p.id, 
                            p.name, 
                            p.vk_cabinet_id, 
                            p.yandex_cabinet_id, 
                            p.mytracker_project_id 
                       FROM projects p
                       ''')
        return [dict(row) for row in cursor.fetchall()]

    def save_vk_balance(self, vk_cabinet_id, balance):
        """Сохранить баланс VK для кабинета"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO vk_balances (vk_cabinet_id, balance)
            VALUES (?, ?)
        ''', (vk_cabinet_id, balance))
        self.conn.commit()
    
    def save_yandex_balance(self, yandex_cabinet_id, balance):
        """Сохранить баланс Yandex для кабинета"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO yandex_balances (yandex_cabinet_id, balance)
            VALUES (?, ?)
        ''', (yandex_cabinet_id, balance))
        self.conn.commit()
    
    def save_mt_stats(self, mytracker_project_id, installs, cost):
        """Сохранить статистику MyTracker для проекта"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO mt_stats (mytracker_project_id, installs, cost)
            VALUES (?, ?, ?)
        ''', (mytracker_project_id, installs, cost))
        self.conn.commit()
    
    def get_project_by_vk_cabinet(self, vk_cabinet_id):
        """Найти проект по VK кабинету"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM projects 
            WHERE vk_cabinet_id = ? AND is_active = 1
        ''', (vk_cabinet_id,))
        return cursor.fetchone()
    
    def get_project_by_yandex_cabinet(self, yandex_cabinet_id):
        """Найти проект по Yandex кабинету"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM projects 
            WHERE yandex_cabinet_id = ? AND is_active = 1
        ''', (yandex_cabinet_id,))
        return cursor.fetchone()
    
    def get_project_by_mytracker_id(self, mytracker_project_id):
        """Найти проект по MyTracker ID"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM projects 
            WHERE mytracker_project_id = ? AND is_active = 1
        ''', (mytracker_project_id,))
        return cursor.fetchone()
    
    def get_all_vk_balances(self):
        """
        Получить все сохраненные балансы VK по проектам за сегодня:
        - Название проекта
        - Баланс
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                p.name as project_name,
                vb.balance,
                vb.fetched_at
            FROM vk_balances vb
            JOIN projects p ON vb.vk_cabinet_id = p.vk_cabinet_id
            WHERE DATE(vb.fetched_at) = DATE('now')
        ''')

        # Получаем названия колонок
        columns = [description[0] for description in cursor.description]

        # Преобразуем каждую строку в словарь
        results = cursor.fetchall()
        return [dict(zip(columns, row)) for row in results]

    def get_latest_data_for_digest(self):
        """Получить последние данные по всем активным проектам для дайджеста"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                p.name,
                (SELECT balance FROM vk_balances 
                 WHERE vk_cabinet_id = p.vk_cabinet_id 
                 ORDER BY fetched_at DESC LIMIT 1) as vk_balance,
                (SELECT balance FROM yandex_balances 
                 WHERE yandex_cabinet_id = p.yandex_cabinet_id 
                 ORDER BY fetched_at DESC LIMIT 1) as yandex_balance,
                (SELECT installs FROM mt_stats 
                 WHERE mytracker_project_id = p.mytracker_project_id 
                 ORDER BY fetched_at DESC LIMIT 1) as mt_installs,
                (SELECT cost FROM mt_stats 
                 WHERE mytracker_project_id = p.mytracker_project_id 
                 ORDER BY fetched_at DESC LIMIT 1) as mt_cost
            FROM projects p
            WHERE p.is_active = 1
        ''')
        return cursor.fetchall()
    
    def get_project_stats_for_period(self, vk_cabinet_id=None, yandex_cabinet_id=None, 
                                     mytracker_project_id=None, days=7):
        """Получить статистику проекта за период
        
        Args:
            vk_cabinet_id: ID кабинета VK (опционально)
            yandex_cabinet_id: ID кабинета Yandex (опционально)
            mytracker_project_id: ID проекта MyTracker (опционально)
            days: количество дней для статистики
        """
        date_from = datetime.now() - timedelta(days=days)
        stats = {}
        
        cursor = self.conn.cursor()
        
        # VK балансы
        if vk_cabinet_id:
            cursor.execute('''
                SELECT 
                    DATE(fetched_at) as date,
                    AVG(balance) as avg_balance
                FROM vk_balances
                WHERE vk_cabinet_id = ? AND fetched_at >= ?
                GROUP BY DATE(fetched_at)
                ORDER BY date
            ''', (vk_cabinet_id, date_from))
            stats['vk_balances'] = cursor.fetchall()
        
        # Yandex балансы
        if yandex_cabinet_id:
            cursor.execute('''
                SELECT 
                    DATE(fetched_at) as date,
                    AVG(balance) as avg_balance
                FROM yandex_balances
                WHERE yandex_cabinet_id = ? AND fetched_at >= ?
                GROUP BY DATE(fetched_at)
                ORDER BY date
            ''', (yandex_cabinet_id, date_from))
            stats['yandex_balances'] = cursor.fetchall()
        
        # MyTracker статистика
        if mytracker_project_id:
            cursor.execute('''
                SELECT 
                    DATE(fetched_at) as date,
                    SUM(installs) as total_installs,
                    SUM(cost) as total_cost
                FROM mt_stats
                WHERE mytracker_project_id = ? AND fetched_at >= ?
                GROUP BY DATE(fetched_at)
                ORDER BY date
            ''', (mytracker_project_id, date_from))
            stats['mt_stats'] = cursor.fetchall()
        
        return stats
    
    def delete_project(self, project_id):
        """Удалить проект и все связанные данные"""
        cursor = self.conn.cursor()
        
        # Получаем cabinet_id проекта
        cursor.execute('''
            SELECT vk_cabinet_id, yandex_cabinet_id, mytracker_project_id 
            FROM projects WHERE id = ?
        ''', (project_id,))
        project = cursor.fetchone()
        
        if not project:
            return False
        
        # Удаляем связанные данные по cabinet_id
        if project['vk_cabinet_id']:
            cursor.execute('DELETE FROM vk_balances WHERE vk_cabinet_id = ?', 
                         (project['vk_cabinet_id'],))
        
        if project['yandex_cabinet_id']:
            cursor.execute('DELETE FROM yandex_balances WHERE yandex_cabinet_id = ?', 
                         (project['yandex_cabinet_id'],))
        
        if project['mytracker_project_id']:
            cursor.execute('DELETE FROM mt_stats WHERE mytracker_project_id = ?', 
                         (project['mytracker_project_id'],))
        
        # Удаляем сам проект
        cursor.execute('DELETE FROM projects WHERE id = ?', (project_id,))
        
        self.conn.commit()
        return cursor.rowcount > 0

    def toggle_project_status(self, project_id, is_active):
        """Активировать/деактивировать проект
        
        Args:
            project_id: ID проекта
            is_active: True для активации, False для деактивации
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE projects 
            SET is_active = ? 
            WHERE id = ?
        ''', (1 if is_active else 0, project_id))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def reset_projects_counter(self):
        """Сбросить счетчик ID проектов (использовать только если таблица пустая!)"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='projects'")
        self.conn.commit()
    
    def close(self):
        """Закрыть соединение с БД"""
        self.conn.close()