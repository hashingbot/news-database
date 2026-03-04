#!/usr/bin/env python3
"""
Crypto News Crawler - Phase 2 增强实现
无需API密钥的加密新闻爬虫系统

Phase 2 新增功能：
1. 集成优化的数据库层（Schema Migration、FTS全文搜索）
2. 增强的数据去重逻辑（URL + 内容相似度）
3. 数据完整性检查
4. 线程安全操作

生成时间: 2026-03-04
"""

import json
import time
import re
import hashlib
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
import feedparser

# Phase 2 新增导入
import sys
sys.path.insert(0, str(Path(__file__).parent))
from database import (
    DatabaseManager, DeduplicationEngine, DataIntegrityChecker,
    create_database
)


# ============================================================================
# 反爬虫策略 - User-Agent轮换池
# ============================================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
]


class RobotsChecker:
    """robots.txt 检查器 - 遵守网站爬虫规则"""
    
    def __init__(self, cache_duration: int = 3600):
        self.cache = {}
        self.cache_duration = cache_duration
        self.user_agent = "CryptoNewsCrawler"
    
    def _get_domain(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def _fetch_robots_txt(self, domain: str) -> Optional[RobotFileParser]:
        robots_url = urljoin(domain, "/robots.txt")
        try:
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            return rp
        except Exception as e:
            print(f"[robots.txt] 无法获取 {robots_url}: {e}")
            return None
    
    def can_fetch(self, url: str) -> bool:
        domain = self._get_domain(url)
        now = time.time()
        
        if domain in self.cache:
            cached_time, rp = self.cache[domain]
            if now - cached_time < self.cache_duration and rp is not None:
                return rp.can_fetch(self.user_agent, url)
        
        rp = self._fetch_robots_txt(domain)
        self.cache[domain] = (now, rp)
        
        if rp is None:
            return True
        
        return rp.can_fetch(self.user_agent, url)
    
    def get_crawl_delay(self, url: str) -> float:
        domain = self._get_domain(url)
        if domain in self.cache:
            _, rp = self.cache[domain]
            if rp:
                delay = rp.crawl_delay(self.user_agent)
                if delay:
                    return delay
        return 2.0


class RequestManager:
    """请求管理器 - 处理请求重试、User-Agent轮换、速率限制"""
    
    def __init__(self, delay: float = 2.0, max_retries: int = 3, respect_robots: bool = True):
        self.delay = delay
        self.max_retries = max_retries
        self.respect_robots = respect_robots
        self.session = requests.Session()
        self.robots_checker = RobotsChecker() if respect_robots else None
        self.last_request_time = 0
        self.domain_last_request = {}
    
    def _get_random_ua(self) -> str:
        return random.choice(USER_AGENTS)
    
    def _wait_for_delay(self, url: str):
        domain = urlparse(url).netloc
        required_delay = self.delay
        
        if self.robots_checker:
            robots_delay = self.robots_checker.get_crawl_delay(url)
            required_delay = max(required_delay, robots_delay)
        
        last_time = self.domain_last_request.get(domain, 0)
        elapsed = time.time() - last_time
        
        if elapsed < required_delay:
            wait_time = required_delay - elapsed
            print(f"[延迟] 等待 {wait_time:.1f}秒 ({domain})")
            time.sleep(wait_time)
        
        self.domain_last_request[domain] = time.time()
    
    def can_fetch(self, url: str) -> bool:
        if not self.respect_robots or not self.robots_checker:
            return True
        return self.robots_checker.can_fetch(url)
    
    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        if not self.can_fetch(url):
            print(f"[robots.txt] 禁止爬取: {url}")
            return None
        
        headers = kwargs.pop('headers', {})
        headers.setdefault('User-Agent', self._get_random_ua())
        headers.setdefault('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
        headers.setdefault('Accept-Language', 'en-US,en;q=0.5')
        headers.setdefault('Accept-Encoding', 'gzip, deflate, br')
        headers.setdefault('DNT', '1')
        headers.setdefault('Connection', 'keep-alive')
        
        for attempt in range(self.max_retries):
            try:
                self._wait_for_delay(url)
                
                print(f"[请求] {url} (尝试 {attempt + 1}/{self.max_retries})")
                response = self.session.get(url, headers=headers, timeout=30, **kwargs)
                response.raise_for_status()
                return response
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response else 0
                
                if status_code == 429:
                    retry_after = int(e.response.headers.get('Retry-After', 60))
                    print(f"[速率限制] 等待 {retry_after}秒...")
                    time.sleep(retry_after)
                    continue
                
                if 500 <= status_code < 600:
                    wait_time = 2 ** attempt
                    print(f"[服务器错误 {status_code}] 等待 {wait_time}秒后重试...")
                    time.sleep(wait_time)
                    continue
                
                print(f"[HTTP错误 {status_code}] {e}")
                return None
                
            except requests.exceptions.Timeout:
                wait_time = 2 ** attempt
                print(f"[超时] 等待 {wait_time}秒后重试...")
                time.sleep(wait_time)
                
            except requests.exceptions.ConnectionError:
                wait_time = 2 ** attempt
                print(f"[连接错误] 等待 {wait_time}秒后重试...")
                time.sleep(wait_time)
                
            except Exception as e:
                print(f"[请求错误] {e}")
                return None
        
        print(f"[失败] 达到最大重试次数: {url}")
        return None


class DateNormalizer:
    """日期标准化器"""
    
    DATE_FORMATS = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d %b %Y",
        "%d %b %Y %H:%M",
        "%b %d, %Y",
        "%b %d, %Y %H:%M",
        "%B %d, %Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%Y年%m月%d日",
        "%Y年%m月%d日 %H:%M",
    ]
    
    @classmethod
    def normalize(cls, date_str: str) -> Optional[datetime]:
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        for fmt in cls.DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        relative = cls._parse_relative(date_str)
        if relative:
            return relative
        
        return None
    
    @classmethod
    def _parse_relative(cls, text: str) -> Optional[datetime]:
        text = text.lower().strip()
        now = datetime.now()
        
        patterns = [
            (r'just now', 0),
            (r'less than an? hour ago', 0),
            (r'(\d+)\s*minute[s]?\s*ago', lambda m: int(m.group(1))),
            (r'(\d+)\s*hour[s]?\s*ago', lambda m: int(m.group(1)) * 60),
            (r'(\d+)\s*day[s]?\s*ago', lambda m: int(m.group(1)) * 24 * 60),
            (r'(\d+)\s*week[s]?\s*ago', lambda m: int(m.group(1)) * 7 * 24 * 60),
            (r'yesterday', 24 * 60),
        ]
        
        for pattern, minutes in patterns:
            match = re.search(pattern, text)
            if match:
                if callable(minutes):
                    minutes = minutes(match)
                return now - timedelta(minutes=minutes)
        
        return None
    
    @classmethod
    def to_iso(cls, dt: datetime) -> str:
        if dt:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return ""


class NewsCategorizer:
    """新闻分类器"""
    
    CATEGORIES = {
        'regulation': {
            'keywords': ['sec', 'regulation', 'regulatory', 'compliance', 'law', 'legal', 
                        'ban', 'govern', 'policy', 'license', 'approval', 'reject', 
                        'sue', 'lawsuit', 'court', 'judge', 'enforce', 'fine', 'penalty',
                        '监管', '合规', '法律', '法规', '禁令', '诉讼', '罚款'],
            'weight': 1.0
        },
        'institutional': {
            'keywords': ['etf', 'institutional', 'blackrock', 'fidelity', 'grayscale', 
                        'fund', 'bank', 'vaneck', 'invesco', 'ark', 'wisdomtree',
                        'issuer', 'custody', 'advisor', 'pension', 'endowment',
                        '机构', '基金', '银行', '托管', 'ETF', '养老金'],
            'weight': 1.0
        },
        'macro': {
            'keywords': ['fed', 'federal reserve', 'inflation', 'rate', 'interest rate',
                        'dollar', 'economy', 'recession', 'cpi', 'ppi', 'gdp', 'unemployment',
                        'treasury', 'bond', 'yield', 'dxy', 'dollar index',
                        '美联储', '通胀', '利率', '经济', '衰退', '国债', '就业'],
            'weight': 1.0
        },
        'technology': {
            'keywords': ['upgrade', 'fork', 'protocol', 'layer2', 'l2', 'rollup',
                        'ethereum', 'bitcoin', 'blockchain', 'consensus', 'validator',
                        'smart contract', 'defi', 'nft', 'dao', 'web3', 'bridge',
                        '升级', '分叉', '协议', '二层', '合约', '验证', '跨链'],
            'weight': 1.0
        },
        'market': {
            'keywords': ['price', 'surge', 'crash', 'rally', 'bear', 'bull', 'market',
                        'trading', 'volume', 'volatility', 'pump', 'dump', 'correction',
                        'support', 'resistance', 'ATH', 'all-time high', 'breakout',
                        '价格', '暴涨', '暴跌', '牛市', '熊市', '交易', '波动'],
            'weight': 1.0
        },
        'security': {
            'keywords': ['hack', 'exploit', 'attack', 'breach', 'theft', 'stolen',
                        'vulnerability', 'rug pull', 'scam', 'fraud', 'phishing',
                        '审计', '黑客', '攻击', '漏洞', '盗窃', '诈骗', '安全'],
            'weight': 1.0
        },
        'adoption': {
            'keywords': ['adoption', 'partner', 'integration', 'payment', 'merchant',
                        'adopt', 'accept', 'use case', 'real world', 'mainstream',
                        '采用', '支付', '合作', '集成', '商家', '应用'],
            'weight': 1.0
        }
    }
    
    @classmethod
    def categorize(cls, title: str, summary: str = "") -> List[str]:
        text = f"{title} {summary}".lower()
        scores = {}
        
        for category, config in cls.CATEGORIES.items():
            score = 0
            for keyword in config['keywords']:
                if keyword.lower() in text:
                    score += config['weight']
            if score > 0:
                scores[category] = score
        
        sorted_categories = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [cat for cat, _ in sorted_categories[:3]] or ['market']
    
    @classmethod
    def get_primary_category(cls, categories: List[str]) -> str:
        return categories[0] if categories else 'market'
    
    @classmethod
    def get_sentiment(cls, title: str, summary: str = "") -> str:
        """
        简单的情绪分析
        Returns: 'positive', 'neutral', 'negative'
        """
        text = f"{title} {summary}".lower()
        
        positive_words = ['surge', 'rally', 'bull', 'gain', 'rise', 'growth', 'adopt', 
                         'approve', 'breakthrough', 'partnership', '暴涨', '上涨', '利好']
        negative_words = ['crash', 'dump', 'bear', 'fall', 'decline', 'hack', 'exploit',
                         'ban', 'reject', 'scam', '暴跌', '下跌', '利空', '黑客']
        
        pos_count = sum(1 for w in positive_words if w in text)
        neg_count = sum(1 for w in negative_words if w in text)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        return 'neutral'
    
    @classmethod
    def get_impact_score(cls, title: str, summary: str = "") -> int:
        """
        评估新闻影响力 (1-10)
        """
        text = f"{title} {summary}".lower()
        score = 5  # 默认中等
        
        # 高影响力指标
        high_impact = ['sec', 'etf', 'blackrock', 'fed', 'hack', 'crash', 'ban',
                       '监管', 'ETF', '黑客', '暴跌']
        # 中影响力指标
        medium_impact = ['upgrade', 'fork', 'partnership', 'adoption',
                        '升级', '合作', '采用']
        
        for word in high_impact:
            if word in text:
                score += 2
                break
        
        for word in medium_impact:
            if word in text:
                score += 1
                break
        
        return min(10, max(1, score))


class CryptoNewsCrawler:
    """加密新闻爬虫 - Phase 2 增强实现"""
    
    RSS_FEEDS = [
        {"name": "CoinDesk", "url": "https://feeds.coindesk.com/coindesk/news", "enabled": True, "priority": 1},
        {"name": "Cointelegraph", "url": "https://cointelegraph.com/rss", "enabled": True, "priority": 2},
        {"name": "Decrypt", "url": "https://decrypt.co/feed", "enabled": True, "priority": 3},
        {"name": "TheBlock", "url": "https://www.theblock.co/rss.xml", "enabled": True, "priority": 4}
    ]
    
    def __init__(self, config_path: Optional[str] = None, 
                 delay: float = 2.0, 
                 max_retries: int = 3,
                 respect_robots: bool = True):
        self.config = self._load_config(config_path) if config_path else {}
        
        # 初始化请求管理器
        crawler_config = self.config.get('crawler', {})
        self.request_manager = RequestManager(
            delay=crawler_config.get('request_delay', delay),
            max_retries=crawler_config.get('max_retries', max_retries),
            respect_robots=crawler_config.get('respect_robots_txt', respect_robots)
        )
        
        # Phase 2: 初始化数据库管理器
        archive_config = self.config.get('archive', {})
        self.archive_dir = Path(archive_config.get('path', './archive'))
        self.archive_dir.mkdir(exist_ok=True)
        self.db_path = self.archive_dir / "news_archive.db"
        
        # 创建数据库连接
        self.db = create_database(self.db_path)
        self.dedup_engine = DeduplicationEngine(self.db)
        self.integrity_checker = DataIntegrityChecker(self.db)
        
        # 统计信息
        self.stats = {
            'rss_success': 0,
            'rss_failed': 0,
            'web_success': 0,
            'web_failed': 0,
            'total_items': 0,
            'duplicates_filtered': 0,
            'saved_to_db': 0
        }
    
    def _load_config(self, path: str) -> dict:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"[警告] 配置文件不存在: {path}")
            return {}
        except json.JSONDecodeError as e:
            print(f"[错误] 配置文件解析失败: {e}")
            return {}
    
    def _generate_hash(self, text: str) -> str:
        """生成文本哈希"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def _record_stat(self, source: str, status: str, count: int = 0, error: str = ""):
        """记录爬取统计"""
        self.db.insert('crawl_stats', {
            'source': source,
            'status': status,
            'items_count': count,
            'error_message': error
        })
    
    # =======================================================================
    # RSS 订阅解析器
    # =======================================================================
    
    def fetch_rss_feed(self, feed_config: dict, days_back: int = 30) -> List[Dict]:
        source_name = feed_config['name']
        feed_url = feed_config['url']
        
        if not feed_config.get('enabled', True):
            print(f"[RSS] {source_name} 已禁用")
            return []
        
        print(f"[RSS] 正在获取 {source_name}...")
        
        try:
            feed = feedparser.parse(feed_url)
            
            if feed.bozo:
                print(f"[RSS警告] {source_name}: {feed.bozo_exception}")
            
            items = []
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            for entry in feed.entries:
                try:
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        pub_date = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        pub_date = datetime(*entry.updated_parsed[:6])
                    elif hasattr(entry, 'published'):
                        pub_date = DateNormalizer.normalize(entry.published)
                    
                    if pub_date and pub_date < cutoff_date:
                        continue
                    
                    title = entry.get('title', '').strip()
                    url = entry.get('link', '')
                    
                    if not title or not url:
                        continue
                    
                    summary = entry.get('summary', '')
                    if not summary and hasattr(entry, 'description'):
                        summary = entry.description
                    
                    summary = self._clean_html(summary)[:500]
                    
                    # Phase 2: 增强分类信息
                    categories = NewsCategorizer.categorize(title, summary)
                    sentiment = NewsCategorizer.get_sentiment(title, summary)
                    impact_score = NewsCategorizer.get_impact_score(title, summary)
                    
                    item = {
                        "source": source_name,
                        "published_at": DateNormalizer.to_iso(pub_date) if pub_date else DateNormalizer.to_iso(datetime.now()),
                        "title": title,
                        "summary": summary,
                        "url": url,
                        "categories": categories,
                        "primary_category": NewsCategorizer.get_primary_category(categories),
                        "sentiment": sentiment,
                        "impact_score": impact_score
                    }
                    items.append(item)
                    
                except Exception as e:
                    print(f"[RSS] 解析条目失败: {e}")
                    continue
            
            print(f"[RSS] {source_name} 成功获取 {len(items)} 条")
            self.stats['rss_success'] += 1
            self._record_stat(source_name, 'success', len(items))
            return items
            
        except Exception as e:
            print(f"[RSS错误] {source_name}: {e}")
            self.stats['rss_failed'] += 1
            self._record_stat(source_name, 'failed', 0, str(e))
            return []
    
    def fetch_all_rss(self, days_back: int = 30) -> List[Dict]:
        """获取所有启用的RSS源"""
        all_items = []
        sorted_feeds = sorted(self.RSS_FEEDS, key=lambda x: x.get('priority', 99))
        
        for feed in sorted_feeds:
            items = self.fetch_rss_feed(feed, days_back)
            all_items.extend(items)
            time.sleep(1)
        
        return all_items
    
    # =======================================================================
    # 数据清洗和存储
    # =======================================================================
    
    def _clean_html(self, html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text(separator=' ', strip=True)
    
    def _deduplicate(self, items: List[Dict]) -> List[Dict]:
        """
        Phase 2: 增强去重逻辑
        - URL去重
        - 内容相似度去重
        """
        unique_items = []
        
        for item in items:
            # 1. URL去重检查
            if self.dedup_engine.is_duplicate_url(item['url']):
                self.stats['duplicates_filtered'] += 1
                continue
            
            # 2. 内容相似度检查（近7天）
            similar = self.dedup_engine.is_similar_content(item['title'], days_range=7)
            if similar:
                print(f"[去重] 发现相似内容 ({similar['similarity']:.1%}): {item['title'][:50]}...")
                self.stats['duplicates_filtered'] += 1
                continue
            
            unique_items.append(item)
        
        print(f"[去重] {len(items)} 条 -> {len(unique_items)} 条 (过滤 {len(items) - len(unique_items)} 条重复)")
        return unique_items
    
    def save_to_archive(self, items: List[Dict]) -> int:
        """
        Phase 2: 使用增强的数据库结构保存
        """
        if not items:
            return 0
        
        saved = 0
        for item in items:
            try:
                url_hash = self._generate_hash(item['url'])
                title_hash = self._generate_hash(item['title'])
                content_hash = DeduplicationEngine.generate_content_hash(
                    item['title'], item.get('summary', '')
                )
                
                data = {
                    'source': item['source'],
                    'published_at': item['published_at'],
                    'title': item['title'],
                    'summary': item.get('summary', ''),
                    'url': item['url'],
                    'categories': json.dumps(item.get('categories', ['market'])),
                    'primary_category': item.get('primary_category', 'market'),
                    'sentiment': item.get('sentiment', 'neutral'),
                    'impact_score': item.get('impact_score', 5),
                    'url_hash': url_hash,
                    'title_hash': title_hash,
                    'content_hash': content_hash,
                    'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                row_id = self.db.insert_or_ignore('news_items', data)
                if row_id > 0:
                    saved += 1
                    
            except Exception as e:
                print(f"[存档错误] 保存失败: {e}")
                continue
        
        print(f"[存档] 成功保存 {saved}/{len(items)} 条新闻")
        self.stats['saved_to_db'] = saved
        return saved
    
    # =======================================================================
    # Phase 2: 数据维护方法
    # =======================================================================
    
    def clean_duplicates(self, days_range: int = 30, dry_run: bool = False) -> Dict:
        """清理重复数据"""
        print(f"\n[维护] 清理重复数据 (近{days_range}天)...")
        result = self.dedup_engine.clean_duplicates(days_range, dry_run)
        print(f"[维护] 发现 {result['groups_found']} 组重复，清理 {result['deleted_count']} 条")
        return result
    
    def check_integrity(self) -> Dict:
        """检查数据完整性"""
        print("\n[维护] 检查数据完整性...")
        result = self.integrity_checker.check_all()
        
        print("[维护] 检查结果:")
        for check_name, check_result in result['checks'].items():
            print(f"  - {check_name}: {check_result}")
        
        return result
    
    def repair_data(self) -> Dict:
        """修复数据问题"""
        print("\n[维护] 修复数据问题...")
        issues = self.integrity_checker.check_all()
        repaired = self.integrity_checker.repair(issues)
        print(f"[维护] 修复完成: {repaired}")
        return repaired
    
    # =======================================================================
    # 主入口
    # =======================================================================
    
    def fetch_all(self, days_back: int = 7) -> List[Dict]:
        """获取所有数据源的新闻"""
        print("=" * 70)
        print("Crypto News Crawler - Phase 2")
        print(f"时间范围: 过去 {days_back} 天")
        print("=" * 70)
        
        all_items = []
        
        # RSS订阅源
        print("\n📡 阶段1: 获取RSS订阅源")
        rss_items = self.fetch_all_rss(days_back)
        all_items.extend(rss_items)
        
        # Phase 2: 数据清洗和去重
        print("\n🧹 阶段2: 数据清洗和去重")
        unique_items = self._deduplicate(all_items)
        
        # 保存到存档
        print("\n💾 阶段3: 保存到存档")
        saved_count = self.save_to_archive(unique_items)
        
        # 统计
        print("\n" + "=" * 70)
        print("📊 爬取统计")
        print("=" * 70)
        print(f"RSS源成功: {self.stats['rss_success']}")
        print(f"RSS源失败: {self.stats['rss_failed']}")
        print(f"总条目数: {len(all_items)}")
        print(f"去重后: {len(unique_items)}")
        print(f"过滤重复: {self.stats['duplicates_filtered']}")
        print(f"新增存档: {saved_count}")
        print("=" * 70)
        
        self.stats['total_items'] = len(unique_items)
        return unique_items
    
    def fetch_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """按日期范围获取新闻"""
        print("=" * 70)
        print(f"获取新闻: {start_date} 至 {end_date}")
        print("=" * 70)
        
        # Phase 2: 使用查询引擎查询本地存档
        from query import QueryEngine
        query_engine = QueryEngine(self.db)
        
        cached = query_engine.query_by_date_range(start_date, end_date)
        if cached:
            print(f"[缓存] 本地找到 {len(cached)} 条")
            return cached
        
        # 计算需要获取的天数
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d")
            start = datetime.strptime(start_date, "%Y-%m-%d")
            days_back = (end - start).days + 1
        except:
            days_back = 30
        
        return self.fetch_all(days_back=days_back)
    
    def get_stats(self) -> Dict:
        """获取爬取统计"""
        return self.stats.copy()


def main():
    """测试运行"""
    import argparse
    
    parser = argparse.ArgumentParser(description="加密新闻爬虫 (Phase 2)")
    parser.add_argument("--days", type=int, default=7, help="获取多少天内的数据")
    parser.add_argument("--config", default="./config/config.json", help="配置文件路径")
    parser.add_argument("--output", help="输出JSON文件路径")
    parser.add_argument("--clean-duplicates", action="store_true", help="清理重复数据")
    parser.add_argument("--check-integrity", action="store_true", help="检查数据完整性")
    
    args = parser.parse_args()
    
    # 初始化爬虫
    config_path = args.config if Path(args.config).exists() else None
    crawler = CryptoNewsCrawler(config_path=config_path)
    
    # 维护操作
    if args.clean_duplicates:
        crawler.clean_duplicates(days_range=30)
        return
    
    if args.check_integrity:
        crawler.check_integrity()
        return
    
    # 获取新闻
    items = crawler.fetch_all(days_back=args.days)
    
    # 保存结果
    output_file = args.output or f"news_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            "meta": {
                "generated_at": datetime.now().isoformat(),
                "total_items": len(items),
                "sources": list(set(item['source'] for item in items)),
                "stats": crawler.get_stats()
            },
            "items": items
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n[完成] 结果已保存到 {output_file}")


if __name__ == "__main__":
    main()
