#!/usr/bin/env python3
"""
Crypto News Archive - Performance Test Suite
查询性能测试套件

测试内容：
1. 查询响应时间（目标：<100ms）
2. 并发查询性能
3. 缓存效率
4. 大数据量查询性能
"""

import time
import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Tuple, Any, Dict, List
import sys

sys.path.insert(0, str(Path(__file__).parent))
from database import create_database, DatabaseManager
from query import QueryEngine, LRUCache


class PerformanceTester:
    """性能测试器"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db = create_database(db_path)
        self.query = QueryEngine(self.db)
        self.results = {}
    
    def _measure_time(self, func, *args, **kwargs) -> Tuple[Any, float]:
        """测量函数执行时间"""
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = (time.perf_counter() - start) * 1000  # 转换为毫秒
        return result, elapsed
    
    def test_date_range_query(self, iterations: int = 10) -> Dict:
        """测试日期范围查询性能"""
        print("\n[测试] 日期范围查询性能...")
        
        # 获取数据库中的实际日期范围
        date_range = self.db.fetchone(
            "SELECT MIN(date(published_at)) as min_date, MAX(date(published_at)) as max_date FROM news_items"
        )
        
        if not date_range or not date_range['min_date']:
            # 如果没有数据，生成测试数据
            print("  警告: 数据库为空，使用默认日期范围")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(date_range['min_date'], '%Y-%m-%d')
            end_date = datetime.strptime(date_range['max_date'], '%Y-%m-%d')
        
        times = []
        for i in range(iterations):
            # 随机生成查询范围
            days = (end_date - start_date).days
            if days <= 7:
                query_start = start_date
                query_end = end_date
            else:
                offset = (i * 7) % (days - 7)
                query_start = start_date + timedelta(days=offset)
                query_end = query_start + timedelta(days=7)
            
            _, elapsed = self._measure_time(
                self.query.query_by_date_range,
                query_start.strftime('%Y-%m-%d'),
                query_end.strftime('%Y-%m-%d')
            )
            times.append(elapsed)
        
        result = {
            'test_name': '日期范围查询',
            'iterations': iterations,
            'min_ms': min(times),
            'max_ms': max(times),
            'avg_ms': mean(times),
            'median_ms': median(times),
            'target_ms': 100,
            'passed': mean(times) < 100
        }
        
        print(f"  平均响应: {result['avg_ms']:.2f}ms")
        print(f"  目标: {result['target_ms']}ms - {'✅ 通过' if result['passed'] else '❌ 未通过'}")
        
        return result
    
    def test_category_query(self, iterations: int = 10) -> Dict:
        """测试分类查询性能"""
        print("\n[测试] 分类查询性能...")
        
        categories = ['market', 'regulation', 'technology', 'institutional', 'macro']
        times = []
        
        for i in range(iterations):
            cat = categories[i % len(categories)]
            _, elapsed = self._measure_time(
                self.query.query_by_category,
                cat
            )
            times.append(elapsed)
        
        result = {
            'test_name': '分类查询',
            'iterations': iterations,
            'min_ms': min(times),
            'max_ms': max(times),
            'avg_ms': mean(times),
            'median_ms': median(times),
            'target_ms': 100,
            'passed': mean(times) < 100
        }
        
        print(f"  平均响应: {result['avg_ms']:.2f}ms")
        print(f"  目标: {result['target_ms']}ms - {'✅ 通过' if result['passed'] else '❌ 未通过'}")
        
        return result
    
    def test_keyword_search(self, iterations: int = 10) -> Dict:
        """测试关键词搜索性能"""
        print("\n[测试] 关键词搜索性能...")
        
        keywords = ['bitcoin', 'ethereum', 'ETF', 'regulation', 'price', 'market', 'crypto']
        times = []
        
        for i in range(iterations):
            keyword = keywords[i % len(keywords)]
            _, elapsed = self._measure_time(
                self.query.query_by_keyword,
                keyword,
                limit=50
            )
            times.append(elapsed)
        
        result = {
            'test_name': '关键词搜索',
            'iterations': iterations,
            'min_ms': min(times),
            'max_ms': max(times),
            'avg_ms': mean(times),
            'median_ms': median(times),
            'target_ms': 200,  # 搜索允许稍慢
            'passed': mean(times) < 200
        }
        
        print(f"  平均响应: {result['avg_ms']:.2f}ms")
        print(f"  目标: {result['target_ms']}ms - {'✅ 通过' if result['passed'] else '❌ 未通过'}")
        
        return result
    
    def test_cache_efficiency(self) -> Dict:
        """测试缓存效率"""
        print("\n[测试] 缓存效率...")
        
        # 获取一个日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # 第一次查询（无缓存）
        _, time_no_cache = self._measure_time(
            self.query.query_by_date_range,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        # 第二次查询（有缓存）
        _, time_with_cache = self._measure_time(
            self.query.query_by_date_range,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        improvement = ((time_no_cache - time_with_cache) / time_no_cache * 100) if time_no_cache > 0 else 0
        
        result = {
            'test_name': '缓存效率',
            'no_cache_ms': time_no_cache,
            'with_cache_ms': time_with_cache,
            'improvement_percent': improvement,
            'target_improvement': 50,
            'passed': improvement >= 50
        }
        
        print(f"  无缓存: {time_no_cache:.2f}ms")
        print(f"  有缓存: {time_with_cache:.2f}ms")
        print(f"  提升: {improvement:.1f}% - {'✅ 通过' if result['passed'] else '❌ 未通过'}")
        
        return result
    
    def test_concurrent_queries(self, num_threads: int = 5) -> Dict:
        """测试并发查询性能"""
        print(f"\n[测试] 并发查询性能 ({num_threads}线程)...")
        
        times = []
        errors = []
        
        def query_worker(thread_id: int):
            try:
                end_date = datetime.now() - timedelta(days=thread_id * 7)
                start_date = end_date - timedelta(days=7)
                
                _, elapsed = self._measure_time(
                    self.query.query_by_date_range,
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d')
                )
                times.append(elapsed)
            except Exception as e:
                errors.append(str(e))
        
        threads = []
        start_time = time.perf_counter()
        
        for i in range(num_threads):
            t = threading.Thread(target=query_worker, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        total_time = (time.perf_counter() - start_time) * 1000
        
        result = {
            'test_name': '并发查询',
            'num_threads': num_threads,
            'total_time_ms': total_time,
            'avg_per_thread_ms': mean(times) if times else 0,
            'errors': len(errors),
            'passed': len(errors) == 0
        }
        
        print(f"  总时间: {total_time:.2f}ms")
        print(f"  平均/线程: {result['avg_per_thread_ms']:.2f}ms")
        print(f"  错误: {len(errors)} - {'✅ 通过' if result['passed'] else '❌ 未通过'}")
        
        return result
    
    def test_statistics_query(self) -> Dict:
        """测试统计查询性能"""
        print("\n[测试] 统计查询性能...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        _, elapsed = self._measure_time(
            self.query.get_statistics,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        result = {
            'test_name': '统计查询',
            'time_ms': elapsed,
            'target_ms': 500,
            'passed': elapsed < 500
        }
        
        print(f"  响应时间: {elapsed:.2f}ms")
        print(f"  目标: {result['target_ms']}ms - {'✅ 通过' if result['passed'] else '❌ 未通过'}")
        
        return result
    
    def run_all_tests(self) -> Dict:
        """运行所有性能测试"""
        print("=" * 70)
        print("Crypto News Archive - 性能测试套件")
        print("=" * 70)
        print(f"数据库: {self.db_path}")
        print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取数据统计
        stats = self.db.fetchone("SELECT COUNT(*) as count FROM news_items")
        print(f"记录总数: {stats['count'] if stats else 0}")
        print("=" * 70)
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'database': str(self.db_path),
            'total_records': stats['count'] if stats else 0,
            'tests': []
        }
        
        # 运行测试
        results['tests'].append(self.test_date_range_query())
        results['tests'].append(self.test_category_query())
        results['tests'].append(self.test_keyword_search())
        results['tests'].append(self.test_cache_efficiency())
        results['tests'].append(self.test_concurrent_queries())
        results['tests'].append(self.test_statistics_query())
        
        # 汇总
        passed = sum(1 for t in results['tests'] if t['passed'])
        total = len(results['tests'])
        
        results['summary'] = {
            'total_tests': total,
            'passed': passed,
            'failed': total - passed,
            'pass_rate': f"{passed/total*100:.1f}%"
        }
        
        print("\n" + "=" * 70)
        print("测试结果汇总")
        print("=" * 70)
        print(f"通过: {passed}/{total} ({results['summary']['pass_rate']})")
        print("=" * 70)
        
        return results
    
    def generate_report(self, output_path: Path = None) -> Path:
        """生成测试报告"""
        results = self.run_all_tests()
        
        # 生成Markdown报告
        lines = []
        lines.append("# 性能测试报告 - Crypto News Archive Phase 2")
        lines.append("")
        lines.append(f"**测试时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"**数据库**: {self.db_path}")
        lines.append(f"**记录总数**: {results['total_records']}")
        lines.append("")
        lines.append("## 测试结果汇总")
        lines.append("")
        lines.append(f"- **通过**: {results['summary']['passed']}/{results['summary']['total_tests']}")
        lines.append(f"- **通过率**: {results['summary']['pass_rate']}")
        lines.append("")
        lines.append("## 详细测试结果")
        lines.append("")
        lines.append("| 测试项 | 平均响应 | 目标 | 结果 |")
        lines.append("|--------|----------|------|------|")
        
        for test in results['tests']:
            name = test['test_name']
            
            if 'avg_ms' in test:
                value = f"{test['avg_ms']:.2f}ms"
                target = f"{test['target_ms']}ms"
            elif 'time_ms' in test:
                value = f"{test['time_ms']:.2f}ms"
                target = f"{test['target_ms']}ms"
            elif 'improvement_percent' in test:
                value = f"{test['improvement_percent']:.1f}%"
                target = f"{test['target_improvement']}%"
            else:
                value = f"{test.get('total_time_ms', 0):.2f}ms"
                target = "-"
            
            result = "✅ 通过" if test['passed'] else "❌ 未通过"
            lines.append(f"| {name} | {value} | {target} | {result} |")
        
        lines.append("")
        lines.append("## 性能指标说明")
        lines.append("")
        lines.append("- **查询响应时间**: 所有基础查询（日期范围、分类）目标 < 100ms")
        lines.append("- **搜索响应时间**: 关键词搜索允许稍慢，目标 < 200ms")
        lines.append("- **缓存效率**: 缓存命中应带来至少 50% 的性能提升")
        lines.append("- **并发性能**: 多线程查询不应产生错误")
        lines.append("- **统计查询**: 复杂统计查询目标 < 500ms")
        lines.append("")
        
        # 保存报告
        if output_path is None:
            output_path = Path("performance_test_report.md")
        
        output_path = Path(output_path)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
        
        # 同时保存JSON结果
        json_path = output_path.with_suffix('.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n[报告] 已生成测试报告:")
        print(f"  Markdown: {output_path}")
        print(f"  JSON: {json_path}")
        
        return output_path


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="性能测试工具")
    parser.add_argument("--db", default="./archive/news_archive.db", help="数据库路径")
    parser.add_argument("--output", help="输出报告路径")
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"错误: 数据库不存在 {db_path}")
        print("请先运行爬虫获取一些数据")
        exit(1)
    
    tester = PerformanceTester(db_path)
    tester.generate_report(args.output)


if __name__ == "__main__":
    main()
