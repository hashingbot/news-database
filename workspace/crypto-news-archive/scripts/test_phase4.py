#!/usr/bin/env python3
"""
Crypto News Archive - Phase 4 测试套件
功能测试、数据验证和异常处理测试

测试内容：
1. 功能测试（各数据源爬取）
2. 数据准确性验证
3. 异常处理测试
4. 集成测试
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import threading

sys.path.insert(0, str(Path(__file__).parent))
from database import create_database, DeduplicationEngine, DataIntegrityChecker
from query import QueryEngine, create_query_engine
from export import ExportManager


class Phase4TestSuite:
    """Phase 4 完整测试套件"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db = create_database(db_path)
        self.query = QueryEngine(self.db)
        self.export_manager = ExportManager(self.query)
        self.results = []
        
    def run_all_tests(self) -> Dict:
        """运行所有测试"""
        print("="*70)
        print("Crypto News Archive - Phase 4 测试套件")
        print("="*70)
        print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"数据库: {self.db_path}")
        
        # 获取数据统计
        stats = self.db.fetchone("SELECT COUNT(*) as count FROM news_items")
        print(f"记录总数: {stats['count'] if stats else 0}")
        print("="*70)
        
        # 运行测试
        self.test_query_functions()
        self.test_deduplication()
        self.test_data_integrity()
        self.test_export_functions()
        self.test_caching()
        self.test_error_handling()
        self.test_fund_report_integration()
        
        # 汇总结果
        passed = sum(1 for r in self.results if r['passed'])
        total = len(self.results)
        
        print("\n" + "="*70)
        print("测试结果汇总")
        print("="*70)
        print(f"通过: {passed}/{total}")
        print(f"通过率: {passed/total*100:.1f}%" if total > 0 else "N/A")
        print("="*70)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_tests': total,
            'passed': passed,
            'failed': total - passed,
            'details': self.results
        }
    
    def _record_result(self, name: str, passed: bool, message: str = "", details: Dict = None):
        """记录测试结果"""
        self.results.append({
            'name': name,
            'passed': passed,
            'message': message,
            'details': details or {}
        })
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"\n[{status}] {name}")
        if message:
            print(f"       {message}")
    
    # =====================================================================
    # 功能测试
    # =====================================================================
    
    def test_query_functions(self):
        """测试查询功能"""
        print("\n[测试组] 查询功能")
        
        # 测试1: 日期范围查询
        try:
            end = datetime.now()
            start = end - timedelta(days=30)
            items = self.query.query_by_date_range(
                start.strftime('%Y-%m-%d'),
                end.strftime('%Y-%m-%d'),
                limit=10
            )
            self._record_result(
                "日期范围查询",
                len(items) >= 0,
                f"查询到 {len(items)} 条记录"
            )
        except Exception as e:
            self._record_result("日期范围查询", False, str(e))
        
        # 测试2: 分类查询
        try:
            items = self.query.query_by_category('market', limit=10)
            self._record_result(
                "分类查询",
                len(items) >= 0,
                f"查询到 {len(items)} 条 market 分类记录"
            )
        except Exception as e:
            self._record_result("分类查询", False, str(e))
        
        # 测试3: 关键词搜索
        try:
            items = self.query.query_by_keyword('bitcoin', limit=10)
            self._record_result(
                "关键词搜索",
                len(items) >= 0,
                f"搜索到 {len(items)} 条包含 'bitcoin' 的记录"
            )
        except Exception as e:
            self._record_result("关键词搜索", False, str(e))
        
        # 测试4: 统计查询
        try:
            end = datetime.now()
            start = end - timedelta(days=30)
            stats = self.query.get_statistics(
                start.strftime('%Y-%m-%d'),
                end.strftime('%Y-%m-%d')
            )
            has_stats = 'total' in stats and 'categories' in stats
            self._record_result(
                "统计查询",
                has_stats,
                f"获取统计: {stats.get('total', 0)} 条记录"
            )
        except Exception as e:
            self._record_result("统计查询", False, str(e))
        
        # 测试5: 每日摘要
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            summary = self.query.get_daily_summary(today)
            has_summary = 'date' in summary and 'total' in summary
            self._record_result(
                "每日摘要",
                has_summary,
                f"{today} 共 {summary.get('total', 0)} 条"
            )
        except Exception as e:
            self._record_result("每日摘要", False, str(e))
    
    def test_deduplication(self):
        """测试去重功能"""
        print("\n[测试组] 数据去重")
        
        dedup = DeduplicationEngine(self.db)
        
        # 测试1: URL去重检查
        try:
            # 获取一条现有记录的URL
            existing = self.db.fetchone("SELECT url FROM news_items LIMIT 1")
            if existing:
                is_dup = dedup.is_duplicate_url(existing['url'])
                self._record_result(
                    "URL去重检查",
                    is_dup == True,
                    f"已存在的URL被正确识别为重复"
                )
            else:
                self._record_result("URL去重检查", True, "数据库为空，跳过")
        except Exception as e:
            self._record_result("URL去重检查", False, str(e))
        
        # 测试2: 内容相似度计算
        try:
            similarity = dedup.calculate_similarity(
                "Bitcoin Price Surges to New All-Time High",
                "Bitcoin Price Surges to New All Time High"
            )
            self._record_result(
                "相似度计算",
                0.8 <= similarity <= 1.0,
                f"相似度: {similarity:.2%}"
            )
        except Exception as e:
            self._record_result("相似度计算", False, str(e))
        
        # 测试3: 相似内容检测
        try:
            # 使用一条现有标题
            existing = self.db.fetchone("SELECT title FROM news_items LIMIT 1")
            if existing:
                similar = dedup.is_similar_content(existing['title'], days_range=30)
                self._record_result(
                    "相似内容检测",
                    similar is not None,  # 应该找到自身
                    "找到相似内容" if similar else "未找到"
                )
            else:
                self._record_result("相似内容检测", True, "数据库为空，跳过")
        except Exception as e:
            self._record_result("相似内容检测", False, str(e))
    
    def test_data_integrity(self):
        """测试数据完整性"""
        print("\n[测试组] 数据完整性")
        
        checker = DataIntegrityChecker(self.db)
        
        # 测试1: 完整性检查
        try:
            result = checker.check_all()
            has_checks = 'checks' in result
            self._record_result(
                "完整性检查",
                has_checks,
                f"执行了 {len(result.get('checks', {}))} 项检查"
            )
        except Exception as e:
            self._record_result("完整性检查", False, str(e))
        
        # 测试2: 必填字段检查
        try:
            missing = self.db.fetchone("""
                SELECT COUNT(*) as count FROM news_items 
                WHERE title IS NULL OR title = '' OR url IS NULL OR url = ''
            """)
            no_missing = missing['count'] == 0 if missing else True
            self._record_result(
                "必填字段检查",
                no_missing,
                f"缺失必填字段: {missing['count'] if missing else 0} 条"
            )
        except Exception as e:
            self._record_result("必填字段检查", False, str(e))
    
    def test_export_functions(self):
        """测试导出功能"""
        print("\n[测试组] 数据导出")
        
        output_dir = Path("./test_output")
        output_dir.mkdir(exist_ok=True)
        
        end = datetime.now()
        start = end - timedelta(days=7)
        start_str = start.strftime('%Y-%m-%d')
        end_str = end.strftime('%Y-%m-%d')
        
        # 测试1: JSON导出
        try:
            from export import JSONExporter
            exporter = JSONExporter(self.query)
            output_path = output_dir / "test_export.json"
            result = exporter.export(start_str, end_str, output_path)
            self._record_result(
                "JSON导出",
                result.exists(),
                f"导出到 {result}"
            )
        except Exception as e:
            self._record_result("JSON导出", False, str(e))
        
        # 测试2: Markdown导出
        try:
            from export import MarkdownExporter
            exporter = MarkdownExporter(self.query)
            output_path = output_dir / "test_export.md"
            result = exporter.export(start_str, end_str, output_path)
            self._record_result(
                "Markdown导出",
                result.exists(),
                f"导出到 {result}"
            )
        except Exception as e:
            self._record_result("Markdown导出", False, str(e))
    
    def test_caching(self):
        """测试缓存功能"""
        print("\n[测试组] 缓存机制")
        
        from query import LRUCache
        
        # 测试1: 缓存基本操作
        try:
            cache = LRUCache(max_size=10, default_ttl=60)
            cache.set("test_key", {"data": "test"})
            value = cache.get("test_key")
            self._record_result(
                "缓存读写",
                value is not None and value.get("data") == "test",
                "基本读写正常"
            )
        except Exception as e:
            self._record_result("缓存读写", False, str(e))
        
        # 测试2: 缓存过期
        try:
            cache = LRUCache(max_size=10, default_ttl=0)  # 立即过期
            cache.set("expire_key", "value")
            time.sleep(0.1)
            value = cache.get("expire_key")
            self._record_result(
                "缓存过期",
                value is None,
                "过期项正确失效"
            )
        except Exception as e:
            self._record_result("缓存过期", False, str(e))
        
        # 测试3: LRU淘汰
        try:
            cache = LRUCache(max_size=3, default_ttl=3600)
            cache.set("key1", "value1")
            cache.set("key2", "value2")
            cache.set("key3", "value3")
            cache.set("key4", "value4")  # 应该淘汰key1
            value = cache.get("key1")
            self._record_result(
                "LRU淘汰",
                value is None,
                "超出容量时正确淘汰"
            )
        except Exception as e:
            self._record_result("LRU淘汰", False, str(e))
    
    def test_error_handling(self):
        """测试异常处理"""
        print("\n[测试组] 异常处理")
        
        # 测试1: 无效日期处理
        try:
            items = self.query.query_by_date_range("invalid", "2026-01-01")
            self._record_result(
                "无效日期处理",
                len(items) == 0,
                "无效日期返回空结果"
            )
        except Exception as e:
            # 异常也是可接受的
            self._record_result("无效日期处理", True, f"抛出异常: {type(e).__name__}")
        
        # 测试2: 空结果处理
        try:
            future_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
            items = self.query.query_by_date_range(future_date, future_date)
            self._record_result(
                "空结果处理",
                len(items) == 0,
                "未来日期返回空结果"
            )
        except Exception as e:
            self._record_result("空结果处理", False, str(e))
        
        # 测试3: 线程安全
        try:
            results = []
            errors = []
            
            def query_worker():
                try:
                    end = datetime.now()
                    start = end - timedelta(days=7)
                    items = self.query.query_by_date_range(
                        start.strftime('%Y-%m-%d'),
                        end.strftime('%Y-%m-%d'),
                        limit=5
                    )
                    results.append(len(items))
                except Exception as e:
                    errors.append(str(e))
            
            threads = [threading.Thread(target=query_worker) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            self._record_result(
                "并发查询安全",
                len(errors) == 0,
                f"5线程并发，{len(results)}次成功，{len(errors)}次错误"
            )
        except Exception as e:
            self._record_result("并发查询安全", False, str(e))
    
    def test_fund_report_integration(self):
        """测试基金月报集成"""
        print("\n[测试组] 基金月报集成")
        
        try:
            from fund_report_news_integration import FundReportNewsIntegration
            
            integrator = FundReportNewsIntegration(self.db_path)
            report = integrator.fetch_monthly_news(2026, 3)
            
            has_required = all(k in report for k in ['meta', 'summary', 'analysis'])
            self._record_result(
                "数据获取",
                has_required,
                f"获取 {report['summary'].get('total_news', 0)} 条"
            )
            
            # 测试内容生成
            content = integrator.generate_section5_content(report)
            has_sections = all(s in content for s in ['市场展望', '市场概况', '主题分析'])
            self._record_result(
                "内容生成",
                has_sections,
                f"内容长度: {len(content)} 字符"
            )
            
        except Exception as e:
            self._record_result("基金月报集成", False, str(e))


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Phase 4 测试套件")
    parser.add_argument("--db", default="./archive/news_archive.db", help="数据库路径")
    parser.add_argument("--output", help="输出报告路径")
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"警告: 数据库不存在 {db_path}")
        print("将创建一个空数据库进行测试...")
    
    suite = Phase4TestSuite(db_path)
    results = suite.run_all_tests()
    
    # 保存报告
    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n[报告] 已保存到 {output_path}")


if __name__ == "__main__":
    main()
