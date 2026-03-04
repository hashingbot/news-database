#!/usr/bin/env python3
"""
Crypto News Archive - Query Module with Caching
查询功能实现 - 支持灵活的查询接口和缓存机制

功能：
- 按日期范围查询
- 按分类查询
- 按关键词搜索
- 每日摘要生成
- 统计信息获取
- LRU缓存机制
"""

import json
import re
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Callable
from collections import OrderedDict
from functools import wraps
import threading

from database import DatabaseManager, create_database


# ============================================================================
# LRU缓存实现
# ============================================================================

class LRUCache:
    """
    线程安全的LRU缓存实现
    
    特性：
    - 最大容量限制
    - TTL过期时间
    - 线程安全
    """
    
    def __init__(self, max_size: int = 100, default_ttl: int = 1800):
        """
        Args:
            max_size: 最大缓存条目数
            default_ttl: 默认过期时间（秒），默认30分钟
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache = OrderedDict()  # {key: (value, expire_time)}
        self._lock = threading.RLock()
    
    def _generate_key(self, *args, **kwargs) -> str:
        """生成缓存键"""
        key_data = json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True)
        return hashlib.md5(key_data.encode('utf-8')).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        with self._lock:
            if key not in self._cache:
                return None
            
            value, expire_time = self._cache[key]
            
            # 检查是否过期
            if time.time() > expire_time:
                del self._cache[key]
                return None
            
            # 移动到末尾（最近使用）
            self._cache.move_to_end(key)
            return value
    
    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存值"""
        ttl = ttl or self.default_ttl
        expire_time = time.time() + ttl
        
        with self._lock:
            # 如果已存在，先删除
            if key in self._cache:
                del self._cache[key]
            
            # 检查容量
            while len(self._cache) >= self.max_size:
                # 移除最久未使用的
                self._cache.popitem(last=False)
            
            self._cache[key] = (value, expire_time)
    
    def clear(self):
        """清空缓存"""
        with self._lock:
            self._cache.clear()
    
    def invalidate(self, pattern: str = None):
        """
        使缓存失效
        
        Args:
            pattern: 如果指定，只删除匹配该正则的键
        """
        with self._lock:
            if pattern is None:
                self._cache.clear()
            else:
                regex = re.compile(pattern)
                keys_to_delete = [k for k in self._cache.keys() if regex.search(k)]
                for key in keys_to_delete:
                    del self._cache[key]
    
    def get_stats(self) -> Dict:
        """获取缓存统计"""
        with self._lock:
            now = time.time()
            valid_items = sum(1 for _, expire in self._cache.values() if expire > now)
            expired_items = len(self._cache) - valid_items
            
            return {
                'total_items': len(self._cache),
                'valid_items': valid_items,
                'expired_items': expired_items,
                'max_size': self.max_size,
                'hit_rate': None  # 需要额外统计
            }


def cached(cache_instance: LRUCache, ttl: int = None):
    """
    缓存装饰器
    
    用法：
        cache = LRUCache(max_size=100)
        
        @cached(cache, ttl=300)
        def expensive_query(param):
            return db.fetchall(...)
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键（基于函数名和参数）
            cache_key = f"{func.__name__}:{json.dumps({'args': args[1:], 'kwargs': kwargs}, sort_keys=True)}"
            cache_key = hashlib.md5(cache_key.encode('utf-8')).hexdigest()
            
            # 尝试获取缓存
            result = cache_instance.get(cache_key)
            if result is not None:
                return result
            
            # 执行函数
            result = func(*args, **kwargs)
            
            # 存入缓存
            cache_instance.set(cache_key, result, ttl)
            
            return result
        
        # 附加缓存操作
        wrapper.cache = cache_instance
        wrapper.invalidate = lambda: cache_instance.invalidate(f"^{func.__name__}:")
        
        return wrapper
    return decorator


# ============================================================================
# 查询引擎
# ============================================================================

class QueryEngine:
    """
    新闻查询引擎
    
    提供灵活的查询接口，支持：
    - 日期范围查询
    - 分类筛选
    - 关键词搜索
    - 排序和分页
    """
    
    def __init__(self, db: DatabaseManager, cache: LRUCache = None):
        self.db = db
        self.cache = cache or LRUCache(max_size=100, default_ttl=1800)
    
    # ========================================================================
    # 基础查询方法
    # ========================================================================
    
    def query_by_date_range(
        self, 
        start: str, 
        end: str,
        order_by: str = 'published_at DESC',
        limit: int = None
    ) -> List[Dict]:
        """
        按日期范围查询
        
        Args:
            start: 开始日期 (YYYY-MM-DD)
            end: 结束日期 (YYYY-MM-DD)
            order_by: 排序方式
            limit: 限制返回数量
        
        Returns:
            新闻条目列表
        """
        query = f"""
            SELECT * FROM news_items 
            WHERE date(published_at) BETWEEN date(?) AND date(?)
            ORDER BY {order_by}
        """
        params = [start, end]
        
        if limit:
            query += f" LIMIT {int(limit)}"
        
        results = self.db.fetchall(query, tuple(params))
        return [self._parse_item(row) for row in results]
    
    def query_by_category(
        self,
        category: str,
        start: str = None,
        end: str = None,
        limit: int = None
    ) -> List[Dict]:
        """
        按分类查询
        
        Args:
            category: 分类名称
            start: 开始日期 (可选)
            end: 结束日期 (可选)
            limit: 限制返回数量
        
        Returns:
            新闻条目列表
        """
        if start and end:
            query = """
                SELECT * FROM news_items 
                WHERE primary_category = ?
                AND date(published_at) BETWEEN date(?) AND date(?)
                ORDER BY published_at DESC
            """
            params = (category, start, end)
        else:
            query = """
                SELECT * FROM news_items 
                WHERE primary_category = ?
                ORDER BY published_at DESC
            """
            params = (category,)
        
        if limit:
            query += f" LIMIT {int(limit)}"
        
        results = self.db.fetchall(query, params)
        return [self._parse_item(row) for row in results]
    
    def query_by_keyword(
        self,
        keyword: str,
        start: str = None,
        end: str = None,
        search_in: List[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        按关键词查询
        
        支持FTS全文搜索（如果可用）或LIKE匹配
        
        Args:
            keyword: 搜索关键词
            start: 开始日期 (可选)
            end: 结束日期 (可选)
            search_in: 搜索字段 ['title', 'summary']，默认两者
            limit: 限制返回数量
        
        Returns:
            新闻条目列表
        """
        search_in = search_in or ['title', 'summary']
        
        # 尝试使用FTS全文搜索
        try:
            return self._fts_search(keyword, start, end, limit)
        except:
            # 回退到LIKE匹配
            return self._like_search(keyword, start, end, search_in, limit)
    
    def _fts_search(
        self,
        keyword: str,
        start: str = None,
        end: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """使用FTS5全文搜索"""
        # 构建FTS查询
        fts_query = keyword.replace('"', '""')
        
        if start and end:
            query = """
                SELECT n.* FROM news_items n
                JOIN news_fts f ON n.id = f.rowid
                WHERE news_fts MATCH ?
                AND date(n.published_at) BETWEEN date(?) AND date(?)
                ORDER BY rank
                LIMIT ?
            """
            params = (fts_query, start, end, limit)
        else:
            query = """
                SELECT n.* FROM news_items n
                JOIN news_fts f ON n.id = f.rowid
                WHERE news_fts MATCH ?
                ORDER BY rank
                LIMIT ?
            """
            params = (fts_query, limit)
        
        results = self.db.fetchall(query, params)
        return [self._parse_item(row) for row in results]
    
    def _like_search(
        self,
        keyword: str,
        start: str = None,
        end: str = None,
        search_in: List[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """使用LIKE进行模糊搜索"""
        search_in = search_in or ['title', 'summary']
        
        # 构建WHERE子句
        conditions = []
        for field in search_in:
            conditions.append(f"{field} LIKE ?")
        
        where_clause = " OR ".join(conditions)
        
        if start and end:
            query = f"""
                SELECT * FROM news_items 
                WHERE ({where_clause})
                AND date(published_at) BETWEEN date(?) AND date(?)
                ORDER BY published_at DESC
                LIMIT ?
            """
            params = [f"%{keyword}%"] * len(search_in) + [start, end, limit]
        else:
            query = f"""
                SELECT * FROM news_items 
                WHERE ({where_clause})
                ORDER BY published_at DESC
                LIMIT ?
            """
            params = [f"%{keyword}%"] * len(search_in) + [limit]
        
        results = self.db.fetchall(query, tuple(params))
        return [self._parse_item(row) for row in results]
    
    # ========================================================================
    # 高级查询方法
    # ========================================================================
    
    def get_daily_summary(self, date: str) -> Dict:
        """
        获取某天的摘要
        
        Args:
            date: 日期 (YYYY-MM-DD)
        
        Returns:
            包含统计和摘要的字典
        """
        # 获取当天所有新闻
        items = self.db.fetchall(
            "SELECT * FROM news_items WHERE date(published_at) = date(?) ORDER BY published_at",
            (date,)
        )
        
        if not items:
            return {
                'date': date,
                'total': 0,
                'categories': {},
                'sources': {},
                'highlights': [],
                'sentiment_summary': {}
            }
        
        parsed_items = [self._parse_item(row) for row in items]
        
        # 按分类统计
        categories = {}
        for item in parsed_items:
            cat = item.get('primary_category', 'market')
            categories[cat] = categories.get(cat, 0) + 1
        
        # 按来源统计
        sources = {}
        for item in parsed_items:
            src = item.get('source', 'Unknown')
            sources[src] = sources.get(src, 0) + 1
        
        # 高影响力新闻（按impact_score）
        highlights = sorted(
            parsed_items,
            key=lambda x: x.get('impact_score', 5),
            reverse=True
        )[:5]
        
        # 情绪统计
        sentiment_summary = {}
        for item in parsed_items:
            sent = item.get('sentiment', 'neutral')
            sentiment_summary[sent] = sentiment_summary.get(sent, 0) + 1
        
        return {
            'date': date,
            'total': len(parsed_items),
            'categories': categories,
            'sources': sources,
            'highlights': [
                {
                    'title': h['title'],
                    'source': h['source'],
                    'category': h.get('primary_category', 'market'),
                    'impact_score': h.get('impact_score', 5)
                }
                for h in highlights
            ],
            'sentiment_summary': sentiment_summary
        }
    
    def get_statistics(self, start: str, end: str) -> Dict:
        """
        获取统计信息
        
        Args:
            start: 开始日期 (YYYY-MM-DD)
            end: 结束日期 (YYYY-MM-DD)
        
        Returns:
            统计信息字典
        """
        # 总数量
        total_result = self.db.fetchone(
            "SELECT COUNT(*) as count FROM news_items WHERE date(published_at) BETWEEN date(?) AND date(?)",
            (start, end)
        )
        total = total_result['count'] if total_result else 0
        
        # 按分类统计
        category_results = self.db.fetchall(
            """
            SELECT primary_category, COUNT(*) as count 
            FROM news_items 
            WHERE date(published_at) BETWEEN date(?) AND date(?)
            GROUP BY primary_category
            ORDER BY count DESC
            """,
            (start, end)
        )
        categories = {row['primary_category']: row['count'] for row in category_results}
        
        # 按来源统计
        source_results = self.db.fetchall(
            """
            SELECT source, COUNT(*) as count 
            FROM news_items 
            WHERE date(published_at) BETWEEN date(?) AND date(?)
            GROUP BY source
            ORDER BY count DESC
            """,
            (start, end)
        )
        sources = {row['source']: row['count'] for row in source_results}
        
        # 按天统计
        daily_results = self.db.fetchall(
            """
            SELECT date(published_at) as day, COUNT(*) as count 
            FROM news_items 
            WHERE date(published_at) BETWEEN date(?) AND date(?)
            GROUP BY day
            ORDER BY day
            """,
            (start, end)
        )
        daily = {row['day']: row['count'] for row in daily_results}
        
        # 情绪分布
        sentiment_results = self.db.fetchall(
            """
            SELECT sentiment, COUNT(*) as count 
            FROM news_items 
            WHERE date(published_at) BETWEEN date(?) AND date(?)
            AND sentiment IS NOT NULL
            GROUP BY sentiment
            """,
            (start, end)
        )
        sentiments = {row['sentiment']: row['count'] for row in sentiment_results}
        
        # 平均影响力
        impact_result = self.db.fetchone(
            """
            SELECT AVG(impact_score) as avg_impact 
            FROM news_items 
            WHERE date(published_at) BETWEEN date(?) AND date(?)
            """,
            (start, end)
        )
        avg_impact = impact_result['avg_impact'] if impact_result else 0
        
        return {
            'period': {'start': start, 'end': end},
            'total': total,
            'categories': categories,
            'sources': sources,
            'daily_distribution': daily,
            'sentiment_distribution': sentiments,
            'average_impact_score': round(avg_impact, 2) if avg_impact else 0
        }
    
    def get_trending_keywords(self, start: str, end: str, top_n: int = 10) -> List[Dict]:
        """
        获取热门关键词
        
        基于标题词频统计
        """
        items = self.db.fetchall(
            "SELECT title FROM news_items WHERE date(published_at) BETWEEN date(?) AND date(?)",
            (start, end)
        )
        
        # 简单的词频统计
        word_counts = {}
        stop_words = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
                      'to', 'of', 'and', 'in', 'on', 'at', 'by', 'for', 'with',
                      'as', 'this', 'that', 'it', 'from', 'has', 'have', 'will',
                      'says', 'after', 'over', 'new', 'more', 'about', 'up', 'out'}
        
        for item in items:
            title = item['title'].lower()
            # 提取单词
            words = re.findall(r'\b[a-z]{3,}\b', title)
            for word in words:
                if word not in stop_words:
                    word_counts[word] = word_counts.get(word, 0) + 1
        
        # 排序并返回前N个
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'keyword': word, 'count': count} for word, count in sorted_words[:top_n]]
    
    # ========================================================================
    # 缓存装饰的方法
    # ========================================================================
    
    @cached(cache_instance=LRUCache(max_size=50, default_ttl=1800))
    def query_by_date_range_cached(self, start: str, end: str, limit: int = None) -> List[Dict]:
        """带缓存的日期范围查询"""
        return self.query_by_date_range(start, end, limit=limit)
    
    @cached(cache_instance=LRUCache(max_size=50, default_ttl=1800))
    def get_daily_summary_cached(self, date: str) -> Dict:
        """带缓存的每日摘要"""
        return self.get_daily_summary(date)
    
    @cached(cache_instance=LRUCache(max_size=20, default_ttl=3600))
    def get_statistics_cached(self, start: str, end: str) -> Dict:
        """带缓存的统计信息"""
        return self.get_statistics(start, end)
    
    # ========================================================================
    # 辅助方法
    # ========================================================================
    
    def _parse_item(self, row: Dict) -> Dict:
        """解析数据库行数据"""
        item = dict(row)
        
        # 解析JSON字段
        try:
            item['categories'] = json.loads(item.get('categories', '[]'))
        except:
            item['categories'] = []
        
        # 确保数值类型
        if 'impact_score' in item and item['impact_score'] is not None:
            item['impact_score'] = int(item['impact_score'])
        
        return item
    
    def clear_cache(self):
        """清空查询缓存"""
        self.cache.clear()


# ============================================================================
# 便捷函数
# ============================================================================

def create_query_engine(db_path: Path) -> QueryEngine:
    """创建查询引擎实例"""
    db = create_database(db_path)
    cache = LRUCache(max_size=100, default_ttl=1800)
    return QueryEngine(db, cache)


# ============================================================================
# 命令行接口
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="新闻查询工具")
    parser.add_argument("--db", default="./archive/news_archive.db", help="数据库路径")
    parser.add_argument("--start", required=True, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--category", help="按分类筛选")
    parser.add_argument("--keyword", help="按关键词搜索")
    parser.add_argument("--stats", action="store_true", help="显示统计信息")
    parser.add_argument("--daily", help="显示指定日期的摘要 (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=50, help="限制结果数量")
    parser.add_argument("--output", help="输出JSON文件路径")
    
    args = parser.parse_args()
    
    # 创建查询引擎
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"错误: 数据库不存在 {db_path}")
        exit(1)
    
    engine = create_query_engine(db_path)
    
    # 执行查询
    if args.stats:
        result = engine.get_statistics(args.start, args.end)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.daily:
        result = engine.get_daily_summary(args.daily)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    elif args.category:
        results = engine.query_by_category(args.category, args.start, args.end, args.limit)
        print(f"找到 {len(results)} 条 '{args.category}' 分类的新闻:")
        for item in results[:10]:
            print(f"  [{item['published_at'][:10]}] {item['title'][:60]}...")
    
    elif args.keyword:
        results = engine.query_by_keyword(args.keyword, args.start, args.end, limit=args.limit)
        print(f"找到 {len(results)} 条包含 '{args.keyword}' 的新闻:")
        for item in results[:10]:
            print(f"  [{item['published_at'][:10]}] {item['title'][:60]}...")
    
    else:
        results = engine.query_by_date_range(args.start, args.end, limit=args.limit)
        print(f"找到 {len(results)} 条新闻:")
        for item in results[:10]:
            print(f"  [{item['published_at'][:10]}] [{item['primary_category']}] {item['title'][:50]}...")
    
    # 保存结果
    if args.output and 'results' in locals():
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n结果已保存到 {args.output}")
