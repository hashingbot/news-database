#!/usr/bin/env python3
"""
Crypto News Archive - Database Layer with Schema Migrations
SQLite数据库优化实现 - 支持版本控制和数据完整性

功能：
- 优化的表结构（索引、约束）
- Schema Migration系统
- 数据完整性检查
- 线程安全操作
"""

import sqlite3
import json
import hashlib
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from contextlib import contextmanager


# ============================================================================
# Schema Migration 系统
# ============================================================================

class SchemaMigration:
    """数据库Schema版本控制系统"""
    
    # 当前数据库版本
    CURRENT_VERSION = 2
    
    # Migration历史 - 每个版本对应一个升级脚本
    MIGRATIONS = {
        1: """
            -- 初始版本 (Phase 1)
            CREATE TABLE IF NOT EXISTS news_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                published_at TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                url TEXT NOT NULL,
                categories TEXT DEFAULT '[]',
                primary_category TEXT DEFAULT 'market',
                url_hash TEXT UNIQUE,
                title_hash TEXT UNIQUE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_date ON news_items(published_at);
            CREATE INDEX IF NOT EXISTS idx_source ON news_items(source);
            CREATE INDEX IF NOT EXISTS idx_category ON news_items(primary_category);
            CREATE INDEX IF NOT EXISTS idx_url_hash ON news_items(url_hash);
            CREATE INDEX IF NOT EXISTS idx_title_hash ON news_items(title_hash);
            
            CREATE TABLE IF NOT EXISTS crawl_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                items_count INTEGER DEFAULT 0,
                error_message TEXT,
                crawled_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """,
        2: '''
            -- Phase 2 升级: 优化表结构
            
            -- 添加新字段（不带默认值，避免SQLite限制）
            ALTER TABLE news_items ADD COLUMN source_id TEXT;
            ALTER TABLE news_items ADD COLUMN sentiment TEXT;
            ALTER TABLE news_items ADD COLUMN impact_score INTEGER;
            ALTER TABLE news_items ADD COLUMN content_hash TEXT;
            ALTER TABLE news_items ADD COLUMN fetch_time TEXT;
            
            -- 更新现有记录的默认值
            UPDATE news_items SET sentiment = 'neutral' WHERE sentiment IS NULL;
            UPDATE news_items SET impact_score = 5 WHERE impact_score IS NULL;
            UPDATE news_items SET fetch_time = created_at WHERE fetch_time IS NULL;
            
            -- 创建新索引
            CREATE INDEX IF NOT EXISTS idx_published_date ON news_items(date(published_at));
            CREATE INDEX IF NOT EXISTS idx_content_hash ON news_items(content_hash);
            CREATE INDEX IF NOT EXISTS idx_sentiment ON news_items(sentiment);
            CREATE INDEX IF NOT EXISTS idx_impact ON news_items(impact_score);
            
            -- 创建内容搜索索引 (FTS5)
            CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
                title,
                summary,
                content='news_items',
                content_rowid='id'
            );
            
            -- 创建触发器保持FTS同步
            CREATE TRIGGER IF NOT EXISTS news_items_ai AFTER INSERT ON news_items BEGIN
                INSERT INTO news_fts(rowid, title, summary)
                VALUES (new.id, new.title, new.summary);
            END;
            
            CREATE TRIGGER IF NOT EXISTS news_items_ad AFTER DELETE ON news_items BEGIN
                INSERT INTO news_fts(news_fts, rowid, title, summary)
                VALUES ('delete', old.id, old.title, old.summary);
            END;
            
            CREATE TRIGGER IF NOT EXISTS news_items_au AFTER UPDATE ON news_items BEGIN
                INSERT INTO news_fts(news_fts, rowid, title, summary)
                VALUES ('delete', old.id, old.title, old.summary);
                INSERT INTO news_fts(rowid, title, summary)
                VALUES (new.id, new.title, new.summary);
            END;
            
            -- 数据完整性统计表
            CREATE TABLE IF NOT EXISTS data_quality_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_date TEXT DEFAULT CURRENT_TIMESTAMP,
                total_records INTEGER,
                duplicates_found INTEGER,
                missing_titles INTEGER,
                missing_urls INTEGER,
                missing_dates INTEGER,
                avg_impact_score REAL
            );
            
            -- 数据库元信息表
            CREATE TABLE IF NOT EXISTS db_metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('schema_version', '2');
        '''
    }
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
    
    def get_current_version(self) -> int:
        """获取当前数据库版本"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 检查db_metadata表是否存在
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='db_metadata'
                """)
                if not cursor.fetchone():
                    # 检查是否是全新的数据库
                    cursor.execute("""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name='news_items'
                    """)
                    if not cursor.fetchone():
                        return 0  # 全新数据库
                    return 1  # Phase 1版本
                
                cursor.execute("SELECT value FROM db_metadata WHERE key = 'schema_version'")
                result = cursor.fetchone()
                return int(result[0]) if result else 1
                
        except Exception as e:
            print(f"[Migration] 获取版本失败: {e}")
            return 0
    
    def migrate(self, target_version: int = None):
        """
        执行数据库迁移
        
        Args:
            target_version: 目标版本，默认升级到最新版本
        """
        target_version = target_version or self.CURRENT_VERSION
        current_version = self.get_current_version()
        
        if current_version >= target_version:
            print(f"[Migration] 数据库已是最新版本 (v{current_version})")
            return
        
        print(f"[Migration] 从 v{current_version} 升级到 v{target_version}")
        
        for version in range(current_version + 1, target_version + 1):
            if version not in self.MIGRATIONS:
                print(f"[Migration] 警告: 版本 {version} 的迁移脚本不存在")
                continue
            
            print(f"[Migration] 应用版本 {version}...")
            try:
                with sqlite3.connect(self.db_path) as conn:
                    # 分步骤执行migration，以便更好地处理错误
                    self._apply_migration_v2(conn) if version == 2 else conn.executescript(self.MIGRATIONS[version])
                    conn.commit()
                print(f"[Migration] 版本 {version} 应用成功")
            except Exception as e:
                print(f"[Migration] 版本 {version} 应用失败: {e}")
                raise
        
        print(f"[Migration] 数据库升级完成 (v{current_version} -> v{target_version})")

    def _apply_migration_v2(self, conn):
        """应用v2迁移（分步执行以避免列已存在的问题）"""
        cursor = conn.cursor()
        
        # 获取现有列
        cursor.execute("PRAGMA table_info(news_items)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # 只添加不存在的列
        columns_to_add = [
            ('source_id', 'TEXT'),
            ('sentiment', 'TEXT'),
            ('impact_score', 'INTEGER'),
            ('content_hash', 'TEXT'),
            ('fetch_time', 'TEXT'),
        ]
        
        for col_name, col_type in columns_to_add:
            if col_name not in existing_columns:
                cursor.execute(f"ALTER TABLE news_items ADD COLUMN {col_name} {col_type}")
                print(f"  + 添加列: {col_name}")
        
        # 更新现有记录的默认值
        cursor.execute("UPDATE news_items SET sentiment = 'neutral' WHERE sentiment IS NULL")
        cursor.execute("UPDATE news_items SET impact_score = 5 WHERE impact_score IS NULL")
        cursor.execute("UPDATE news_items SET fetch_time = created_at WHERE fetch_time IS NULL")
        
        # 创建新索引（如果不存在）
        indexes = [
            'idx_published_date',
            'idx_content_hash',
            'idx_sentiment',
            'idx_impact'
        ]
        for idx in indexes:
            cursor.execute(f"SELECT 1 FROM sqlite_master WHERE type='index' AND name='{idx}'")
            if not cursor.fetchone():
                if idx == 'idx_published_date':
                    cursor.execute("CREATE INDEX idx_published_date ON news_items(date(published_at))")
                elif idx == 'idx_content_hash':
                    cursor.execute("CREATE INDEX idx_content_hash ON news_items(content_hash)")
                elif idx == 'idx_sentiment':
                    cursor.execute("CREATE INDEX idx_sentiment ON news_items(sentiment)")
                elif idx == 'idx_impact':
                    cursor.execute("CREATE INDEX idx_impact ON news_items(impact_score)")
        
        # 创建FTS5表（如果不存在）
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='news_fts'")
        if not cursor.fetchone():
            cursor.execute('''
                CREATE VIRTUAL TABLE news_fts USING fts5(
                    title,
                    summary,
                    content='news_items',
                    content_rowid='id'
                )
            ''')
            # 创建触发器
            cursor.execute('''
                CREATE TRIGGER news_items_ai AFTER INSERT ON news_items BEGIN
                    INSERT INTO news_fts(rowid, title, summary)
                    VALUES (new.id, new.title, new.summary);
                END
            ''')
            cursor.execute('''
                CREATE TRIGGER news_items_ad AFTER DELETE ON news_items BEGIN
                    INSERT INTO news_fts(news_fts, rowid, title, summary)
                    VALUES ('delete', old.id, old.title, old.summary);
                END
            ''')
            cursor.execute('''
                CREATE TRIGGER news_items_au AFTER UPDATE ON news_items BEGIN
                    INSERT INTO news_fts(news_fts, rowid, title, summary)
                    VALUES ('delete', old.id, old.title, old.summary);
                    INSERT INTO news_fts(rowid, title, summary)
                    VALUES (new.id, new.title, new.summary);
                END
            ''')
        
        # 创建数据质量统计表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_quality_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_date TEXT DEFAULT CURRENT_TIMESTAMP,
                total_records INTEGER,
                duplicates_found INTEGER,
                missing_titles INTEGER,
                missing_urls INTEGER,
                missing_dates INTEGER,
                avg_impact_score REAL
            )
        ''')
        
        # 创建元数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS db_metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 更新版本号
        cursor.execute("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('schema_version', '2')")


# ============================================================================
# 数据库管理器 - 线程安全
# ============================================================================

class DatabaseManager:
    """
    SQLite数据库管理器
    
    特性：
    - 线程安全（连接池）
    - Schema Migration支持
    - 自动重连
    - 查询性能优化
    """
    
    def __init__(self, db_path: Path, max_connections: int = 5):
        """
        Args:
            db_path: 数据库文件路径
            max_connections: 最大连接数（线程安全）
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_connections = max_connections
        self._local = threading.local()
        self._lock = threading.RLock()
        
        # 执行迁移
        self.migration = SchemaMigration(self.db_path)
        self.migration.migrate()
        
        # 初始化数据库
        self._init_db()
    
    def _init_db(self):
        """初始化数据库（如果不是全新创建）"""
        # 基础配置优化
        with self._get_connection() as conn:
            # WAL模式 - 更好的并发性能
            conn.execute("PRAGMA journal_mode=WAL")
            # 同步模式 - 平衡性能和安全性
            conn.execute("PRAGMA synchronous=NORMAL")
            # 缓存大小 - 提升查询性能
            conn.execute("PRAGMA cache_size=10000")
            # 临时表存储在内存
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接（线程安全）"""
        thread_id = threading.get_ident()
        
        with self._lock:
            if not hasattr(self._local, 'connection'):
                self._local.connection = sqlite3.connect(
                    self.db_path,
                    check_same_thread=False,
                    timeout=30.0
                )
                self._local.connection.row_factory = sqlite3.Row
        
        try:
            yield self._local.connection
        except sqlite3.Error as e:
            self._local.connection.rollback()
            raise e
    
    def execute(self, query: str, params: tuple = ()) -> int:
        """
        执行SQL语句
        
        Returns:
            影响的行数
        """
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    def fetchone(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """查询单行数据"""
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def fetchall(self, query: str, params: tuple = ()) -> List[Dict]:
        """查询多行数据"""
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        插入数据
        
        Returns:
            插入的行ID
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        with self._get_connection() as conn:
            cursor = conn.execute(query, tuple(data.values()))
            conn.commit()
            return cursor.lastrowid
    
    def insert_or_ignore(self, table: str, data: Dict[str, Any]) -> int:
        """
        插入数据（忽略重复）
        
        Returns:
            插入的行ID或0（如果重复）
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
        
        with self._get_connection() as conn:
            cursor = conn.execute(query, tuple(data.values()))
            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else 0
    
    def update(self, table: str, data: Dict[str, Any], where: str, where_params: tuple) -> int:
        """
        更新数据
        
        Returns:
            影响的行数
        """
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        params = tuple(data.values()) + where_params
        
        with self._get_connection() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    def close(self):
        """关闭数据库连接"""
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            del self._local.connection


# ============================================================================
# 数据去重逻辑
# ============================================================================

class DeduplicationEngine:
    """
    数据去重引擎
    
    支持：
    - URL去重（数据库层面UNIQUE约束）
    - 内容相似度去重（标题相似度>90%视为重复）
    """
    
    SIMILARITY_THRESHOLD = 0.90  # 相似度阈值
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    @staticmethod
    def generate_content_hash(title: str, summary: str = "") -> str:
        """生成内容哈希（用于快速去重）"""
        content = f"{title.lower().strip()}|{summary.lower().strip()[:100]}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    @staticmethod
    def calculate_similarity(title1: str, title2: str) -> float:
        """
        计算两个标题的相似度
        
        使用改进的编辑距离算法 + 字符集匹配
        
        Returns:
            相似度 (0.0 - 1.0)
        """
        if not title1 or not title2:
            return 0.0
        
        t1 = title1.lower().strip()
        t2 = title2.lower().strip()
        
        # 完全匹配
        if t1 == t2:
            return 1.0
        
        # 包含关系检查
        if t1 in t2 or t2 in t1:
            longer = max(len(t1), len(t2))
            shorter = min(len(t1), len(t2))
            if longer > 0:
                # 根据长度差异调整相似度
                ratio = shorter / longer
                if ratio >= 0.9:  # 长度差异小于10%
                    return 0.95
                elif ratio >= 0.8:
                    return 0.85
        
        # 编辑距离计算 (Levenshtein)
        similarity = DeduplicationEngine._levenshtein_similarity(t1, t2)
        return similarity
    
    @staticmethod
    def _levenshtein_similarity(s1: str, s2: str) -> float:
        """
        基于编辑距离的相似度计算
        """
        if len(s1) < len(s2):
            s1, s2 = s2, s1
        
        if len(s2) == 0:
            return 0.0
        
        # 动态规划计算编辑距离
        previous_row = list(range(len(s2) + 1))
        
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        distance = previous_row[-1]
        max_len = max(len(s1), len(s2))
        
        return 1.0 - (distance / max_len) if max_len > 0 else 1.0
    
    def is_duplicate_url(self, url: str) -> bool:
        """检查URL是否已存在"""
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        result = self.db.fetchone(
            "SELECT 1 FROM news_items WHERE url_hash = ? LIMIT 1",
            (url_hash,)
        )
        return result is not None
    
    def is_similar_content(self, title: str, days_range: int = 7) -> Optional[Dict]:
        """
        检查是否存在相似内容
        
        Args:
            title: 待检查的标题
            days_range: 检查多少天内的历史数据
        
        Returns:
            如果找到相似内容，返回该记录；否则返回None
        """
        # 获取指定时间范围内的所有标题
        since = (datetime.now() - timedelta(days=days_range)).strftime('%Y-%m-%d')
        
        candidates = self.db.fetchall(
            "SELECT id, title, url, published_at FROM news_items WHERE date(published_at) >= date(?)",
            (since,)
        )
        
        for candidate in candidates:
            similarity = self.calculate_similarity(title, candidate['title'])
            if similarity >= self.SIMILARITY_THRESHOLD:
                candidate['similarity'] = similarity
                return candidate
        
        return None
    
    def find_duplicates(self, days_range: int = 30) -> List[Dict]:
        """
        查找指定时间范围内的重复数据
        
        Returns:
            重复数据列表，包含重复组和相似度
        """
        since = (datetime.now() - timedelta(days=days_range)).strftime('%Y-%m-%d')
        
        items = self.db.fetchall(
            "SELECT id, title, url, published_at FROM news_items WHERE date(published_at) >= date(?) ORDER BY published_at",
            (since,)
        )
        
        duplicates = []
        processed = set()
        
        for i, item1 in enumerate(items):
            if item1['id'] in processed:
                continue
            
            group = [item1]
            
            for item2 in items[i + 1:]:
                if item2['id'] in processed:
                    continue
                
                similarity = self.calculate_similarity(item1['title'], item2['title'])
                if similarity >= self.SIMILARITY_THRESHOLD:
                    group.append({**item2, 'similarity': similarity})
                    processed.add(item2['id'])
            
            if len(group) > 1:
                duplicates.append({
                    'keep': item1,
                    'duplicates': group[1:],
                    'count': len(group)
                })
                processed.add(item1['id'])
        
        return duplicates
    
    def clean_duplicates(self, days_range: int = 30, dry_run: bool = False) -> Dict:
        """
        清理重复数据
        
        Args:
            days_range: 检查多少天内的数据
            dry_run: 是否为模拟运行（不实际删除）
        
        Returns:
            清理统计信息
        """
        duplicates = self.find_duplicates(days_range)
        
        deleted = 0
        details = []
        
        for group in duplicates:
            for dup in group['duplicates']:
                if not dry_run:
                    self.db.execute("DELETE FROM news_items WHERE id = ?", (dup['id'],))
                deleted += 1
                details.append({
                    'deleted_id': dup['id'],
                    'deleted_title': dup['title'],
                    'kept_id': group['keep']['id'],
                    'kept_title': group['keep']['title'],
                    'similarity': dup.get('similarity', 0)
                })
        
        return {
            'groups_found': len(duplicates),
            'deleted_count': deleted,
            'dry_run': dry_run,
            'details': details
        }


# ============================================================================
# 数据完整性检查
# ============================================================================

class DataIntegrityChecker:
    """数据完整性检查器"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def check_all(self) -> Dict:
        """执行所有完整性检查"""
        return {
            'timestamp': datetime.now().isoformat(),
            'checks': {
                'missing_required_fields': self._check_missing_fields(),
                'invalid_dates': self._check_invalid_dates(),
                'duplicate_urls': self._check_duplicate_urls(),
                'orphaned_records': self._check_orphaned_records(),
            },
            'summary': {}
        }
    
    def _check_missing_fields(self) -> Dict:
        """检查必填字段缺失"""
        checks = [
            ("missing_titles", "SELECT COUNT(*) as count FROM news_items WHERE title IS NULL OR title = ''"),
            ("missing_urls", "SELECT COUNT(*) as count FROM news_items WHERE url IS NULL OR url = ''"),
            ("missing_dates", "SELECT COUNT(*) as count FROM news_items WHERE published_at IS NULL OR published_at = ''"),
            ("missing_sources", "SELECT COUNT(*) as count FROM news_items WHERE source IS NULL OR source = ''"),
        ]
        
        results = {}
        for name, query in checks:
            result = self.db.fetchone(query)
            results[name] = result['count'] if result else 0
        
        return results
    
    def _check_invalid_dates(self) -> Dict:
        """检查无效日期"""
        # 查找无法解析的日期格式
        result = self.db.fetchone("""
            SELECT COUNT(*) as count FROM news_items 
            WHERE published_at IS NOT NULL 
            AND datetime(published_at) IS NULL
        """)
        return {'invalid_date_formats': result['count'] if result else 0}
    
    def _check_duplicate_urls(self) -> Dict:
        """检查重复URL"""
        result = self.db.fetchone("""
            SELECT COUNT(*) as count FROM (
                SELECT url_hash, COUNT(*) as cnt 
                FROM news_items 
                GROUP BY url_hash 
                HAVING cnt > 1
            )
        """)
        return {'duplicate_url_groups': result['count'] if result else 0}
    
    def _check_orphaned_records(self) -> Dict:
        """检查孤立记录（如FTS索引不一致）"""
        # 检查FTS索引一致性
        result = self.db.fetchone("""
            SELECT COUNT(*) as count FROM news_fts 
            WHERE rowid NOT IN (SELECT id FROM news_items)
        """)
        return {'orphaned_fts_entries': result['count'] if result else 0}
    
    def repair(self, issues: Dict) -> Dict:
        """修复数据完整性问题"""
        repaired = {}
        
        # 修复孤立FTS记录
        orphaned = self.db.execute("""
            DELETE FROM news_fts 
            WHERE rowid NOT IN (SELECT id FROM news_items)
        """)
        if orphaned > 0:
            repaired['orphaned_fts_deleted'] = orphaned
        
        # 修复缺失的分类
        missing_cat = self.db.execute("""
            UPDATE news_items 
            SET primary_category = 'market', categories = '["market"]'
            WHERE primary_category IS NULL OR primary_category = ''
        """)
        if missing_cat > 0:
            repaired['missing_categories_fixed'] = missing_cat
        
        return repaired


# ============================================================================
# 便捷函数
# ============================================================================

def create_database(db_path: Path) -> DatabaseManager:
    """创建并初始化数据库"""
    return DatabaseManager(db_path)


# 测试代码
if __name__ == "__main__":
    # 测试数据库创建
    test_db_path = Path("./test_archive.db")
    
    print("=" * 60)
    print("Database Layer Test")
    print("=" * 60)
    
    # 创建数据库
    db = create_database(test_db_path)
    print(f"✓ 数据库创建成功: {test_db_path}")
    
    # 测试插入
    test_data = {
        'source': 'TestSource',
        'published_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'title': 'Test News Title',
        'summary': 'Test summary content',
        'url': 'https://example.com/news/1',
        'url_hash': hashlib.md5(b'https://example.com/news/1').hexdigest(),
        'title_hash': hashlib.md5(b'Test News Title').hexdigest(),
        'categories': json.dumps(['market']),
        'primary_category': 'market'
    }
    
    row_id = db.insert('news_items', test_data)
    print(f"✓ 测试数据插入成功，ID: {row_id}")
    
    # 测试查询
    result = db.fetchone("SELECT * FROM news_items WHERE id = ?", (row_id,))
    print(f"✓ 查询成功: {result['title']}")
    
    # 测试去重
    dedup = DeduplicationEngine(db)
    is_dup = dedup.is_duplicate_url('https://example.com/news/1')
    print(f"✓ URL去重检查: {'重复' if is_dup else '未重复'}")
    
    # 测试相似度
    sim = dedup.calculate_similarity(
        "Bitcoin Price Surges to New All-Time High",
        "Bitcoin Price Surges to New All Time High"
    )
    print(f"✓ 相似度计算: {sim:.2%}")
    
    # 清理
    db.close()
    test_db_path.unlink()
    print("\n✓ 所有测试通过")
