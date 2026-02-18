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
                            p.mytracker_project_id,
                            p.is_active 
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

    def save_yandex_balances_bulk(self, balances):
        """Сохранить балансы Yandex для нескольких кабинетов одним запросом.

        Args:
            balances: список словарей в формате
                [{'login': 'cabinet_id', 'amount': 123.45}, ...]

        Returns:
            int: количество успешно добавленных записей.
        """
        rows_to_insert = []
        for item in balances:
            login = item.get('login')
            amount = item.get('amount')
            if not login or amount is None:
                continue
            rows_to_insert.append((login, amount))

        if not rows_to_insert:
            return 0

        cursor = self.conn.cursor()
        cursor.executemany('''
            INSERT INTO yandex_balances (yandex_cabinet_id, balance)
            VALUES (?, ?)
        ''', rows_to_insert)
        self.conn.commit()
        return len(rows_to_insert)
    
    def save_mt_stats(self, mytracker_project_id, registrations=0, first_logins=0, 
                     reactivations=0, fetched_at=None):
        """Сохранить статистику MyTracker для проекта
        
        Args:
            mytracker_project_id: ID проекта в MyTracker
            registrations: количество регистраций
            first_logins: количество первых входов
            reactivations: количество реактиваций
            fetched_at: дата/время записи (опционально)
        """
        cursor = self.conn.cursor()
        if fetched_at is None:
            cursor.execute('''
                INSERT INTO mt_stats (mytracker_project_id, registrations, first_logins, reactivations)
                VALUES (?, ?, ?, ?)
            ''', (mytracker_project_id, registrations, first_logins, reactivations))
        else:
            cursor.execute('''
                INSERT INTO mt_stats (mytracker_project_id, registrations, first_logins, reactivations, fetched_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (mytracker_project_id, registrations, first_logins, reactivations, fetched_at))
        self.conn.commit()

    def save_mt_stats_bulk(self, stats, fetched_at=None, replace_for_date=False):
        """Сохранить статистику MyTracker для нескольких проектов.

        Args:
            stats: список словарей формата
                [{'mytracker_project_id': '123', 'registrations': 10,
                  'first_logins': 8, 'reactivations': 1}, ...]
            fetched_at: общая дата/время для всех записей (опционально)
            replace_for_date: если True и fetched_at задан, предварительно
                удаляются записи за эту дату по указанным проектам.

        Returns:
            int: количество добавленных записей.
        """
        rows_to_insert = []
        for item in stats:
            project_id = item.get('mytracker_project_id')
            if not project_id:
                continue
            rows_to_insert.append((
                project_id,
                int(item.get('registrations', 0) or 0),
                int(item.get('first_logins', 0) or 0),
                int(item.get('reactivations', 0) or 0),
            ))

        if not rows_to_insert:
            return 0

        cursor = self.conn.cursor()

        if replace_for_date and fetched_at is not None:
            ids = sorted(set(row[0] for row in rows_to_insert))
            placeholders = ",".join(["?"] * len(ids))
            cursor.execute(
                f'''
                DELETE FROM mt_stats
                WHERE DATE(fetched_at) = DATE(?)
                  AND mytracker_project_id IN ({placeholders})
                ''',
                [fetched_at, *ids],
            )

        if fetched_at is None:
            cursor.executemany('''
                INSERT INTO mt_stats (mytracker_project_id, registrations, first_logins, reactivations)
                VALUES (?, ?, ?, ?)
            ''', rows_to_insert)
        else:
            rows_with_date = [(*row, fetched_at) for row in rows_to_insert]
            cursor.executemany('''
                INSERT INTO mt_stats (mytracker_project_id, registrations, first_logins, reactivations, fetched_at)
                VALUES (?, ?, ?, ?, ?)
            ''', rows_with_date)

        self.conn.commit()
        return len(rows_to_insert)
    
    def get_project_by_vk_cabinet(self, vk_cabinet_id):
        """Найти проект по VK кабинету"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM projects 
            WHERE vk_cabinet_id = ? AND is_active = 1
        ''', (vk_cabinet_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_project_by_yandex_cabinet(self, yandex_cabinet_id):
        """Найти проект по Yandex кабинету"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM projects 
            WHERE yandex_cabinet_id = ? AND is_active = 1
        ''', (yandex_cabinet_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_project_by_mytracker_id(self, mytracker_project_id):
        """Найти проект по MyTracker ID"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM projects 
            WHERE mytracker_project_id = ? AND is_active = 1
        ''', (mytracker_project_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
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

    def get_all_yandex_balances_today(self, latest_per_cabinet=True):
        """
        Получить балансы Yandex за сегодня.

        Args:
            latest_per_cabinet: если True, возвращается только последний
                баланс по каждому кабинету за сегодня.
        """
        cursor = self.conn.cursor()

        if latest_per_cabinet:
            cursor.execute('''
                SELECT
                    yb.yandex_cabinet_id,
                    p.name AS project_name,
                    yb.balance,
                    yb.fetched_at
                FROM yandex_balances yb
                LEFT JOIN projects p
                    ON yb.yandex_cabinet_id = p.yandex_cabinet_id
                INNER JOIN (
                    SELECT
                        yandex_cabinet_id,
                        MAX(fetched_at) AS max_fetched_at
                    FROM yandex_balances
                    WHERE DATE(fetched_at) = DATE('now')
                    GROUP BY yandex_cabinet_id
                ) latest
                    ON yb.yandex_cabinet_id = latest.yandex_cabinet_id
                   AND yb.fetched_at = latest.max_fetched_at
                ORDER BY yb.yandex_cabinet_id
            ''')
        else:
            cursor.execute('''
                SELECT
                    yb.yandex_cabinet_id,
                    p.name AS project_name,
                    yb.balance,
                    yb.fetched_at
                FROM yandex_balances yb
                LEFT JOIN projects p
                    ON yb.yandex_cabinet_id = p.yandex_cabinet_id
                WHERE DATE(yb.fetched_at) = DATE('now')
                ORDER BY yb.yandex_cabinet_id, yb.fetched_at
            ''')

        return [dict(row) for row in cursor.fetchall()]

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
    
    def get_digest_data(self, icon_path_template='logo/{project}.jpg', days_back=2):
        """Получить данные для дайджеста со всеми расчетами
        
        Args:
            icon_path_template: шаблон пути к иконкам проектов, {project} будет заменен на название
            days_back: количество дней для расчета изменений (по умолчанию 2 - вчера и позавчера)
        
        Returns:
            dict: готовые данные для дайджеста с разделами yandex, vk, mt
        """
        projects = self.get_list_of_projects()
        
        yandex_data = []
        vk_data = []
        mt_data = []
        
        for project in projects:
            if not project.get('is_active'):
                continue
                
            # Получаем статистику за период
            stats = self.get_project_stats_for_period(
                vk_cabinet_id=project.get('vk_cabinet_id'),
                yandex_cabinet_id=project.get('yandex_cabinet_id'),
                mytracker_project_id=project.get('mytracker_project_id'),
                days=days_back
            )
            
            # Yandex данные
            if project.get('yandex_cabinet_id') and 'yandex_balances' in stats:
                yandex_balances = stats['yandex_balances']
                if len(yandex_balances) >= 1:
                    current = yandex_balances[-1]['avg_balance']
                    previous = yandex_balances[-2]['avg_balance'] if len(yandex_balances) > 1 else current
                    
                    yandex_data.append({
                        'project': project['name'],
                        'spend': self._format_number(current),
                        'change': self._calculate_change(current, previous)
                    })
            
            # VK данные
            if project.get('vk_cabinet_id') and 'vk_balances' in stats:
                vk_balances = stats['vk_balances']
                if len(vk_balances) >= 1:
                    current = vk_balances[-1]['avg_balance']
                    previous = vk_balances[-2]['avg_balance'] if len(vk_balances) > 1 else current
                    
                    # Формируем путь к иконке
                    icon_path = icon_path_template.format(
                        project=project['name'].lower().replace(' ', '_')
                    )
                    
                    vk_data.append({
                        'icon_path': icon_path,
                        'name': project['name'],
                        'spend': self._format_number(current),
                        'change': self._calculate_change(current, previous)
                    })
            
            # MyTracker данные
            if project.get('mytracker_project_id') and 'mt_stats' in stats:
                mt_stats = stats['mt_stats']
                if len(mt_stats) >= 1:
                    current = mt_stats[-1]
                    previous = mt_stats[-2] if len(mt_stats) > 1 else current
                    
                    current_regs = current['total_registrations'] or 0
                    previous_regs = previous['total_registrations'] or 0
                    
                    mt_data.append({
                        'name': project['name'],
                        'regs': str(current_regs),
                        'fl': str(current['total_first_logins'] or 0),
                        'ret': str(current['total_reactivations'] or 0),
                        'users': current_regs,
                        'change': self._calculate_change(current_regs, previous_regs)
                    })
        
        return {
            'yandex': yandex_data,
            'vk': vk_data,
            'mt': mt_data
        }
    
    @staticmethod
    def _calculate_change(current, previous):
        """Вычислить процентное изменение"""
        if previous == 0 or previous is None:
            return 0
        return round(((current - previous) / previous) * 100, 2)
    
    @staticmethod
    def _format_number(num):
        """Форматировать число с разделителями тысяч"""
        if num is None:
            return "0"
        return f"{num:,.2f}".replace(",", " ").replace(".", ",")



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
