#!/usr/bin/env python3
"""
Crypto News Archive - 历史新闻获取脚本 (Phase 1 完整版)
支持指定时间段查询，无需API密钥，使用爬虫获取公开数据

生成时间: 2026-03-04
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from collections import Counter

# 导入爬虫模块
sys.path.insert(0, str(Path(__file__).parent))
try:
    from crawler import CryptoNewsCrawler, DateNormalizer
except ImportError as e:
    print(f"[错误] 无法导入爬虫模块: {e}")
    print("[提示] 请安装依赖: pip install beautifulsoup4 feedparser requests")
    sys.exit(1)


class CryptoNewsArchive:
    """加密新闻存档管理器 - Phase 1 完整实现"""
    
    def __init__(self, archive_dir: str = "./archive", config_path: Optional[str] = None):
        """
        初始化存档管理器
        
        Args:
            archive_dir: 存档目录
            config_path: 配置文件路径
        """
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(exist_ok=True)
        
        # 初始化爬虫
        self.crawler = CryptoNewsCrawler(config_path=config_path)
    
    def fetch_news(self, start_date: str, end_date: str, 
                   force_refresh: bool = False) -> List[Dict]:
        """
        获取指定时间段的新闻
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            force_refresh: 是否强制刷新(忽略缓存)
        
        Returns:
            新闻条目列表
        """
        if not force_refresh:
            # 先尝试从缓存获取
            cached = self.crawler.query_archive(start_date, end_date)
            if cached:
                print(f"[缓存] 从本地存档找到 {len(cached)} 条新闻")
                return cached
        
        # 计算需要获取的天数
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d")
            start = datetime.strptime(start_date, "%Y-%m-%d")
            days_back = (end - start).days + 1
        except ValueError as e:
            print(f"[错误] 日期格式错误: {e}")
            return []
        
        # 使用爬虫获取数据
        print(f"[获取] 正在获取过去 {days_back} 天的新闻...")
        return self.crawler.fetch_all(days_back=days_back, use_cache=False)
    
    def generate_summary(self, items: List[Dict]) -> Dict:
        """
        生成摘要报告
        
        Args:
            items: 新闻条目列表
        
        Returns:
            摘要统计字典
        """
        if not items:
            return {
                "total_items": 0,
                "date_range": None,
                "by_source": {},
                "by_category": {},
                "by_day": {}
            }
        
        # 时间范围
        dates = [item.get("published_at", "") for item in items if item.get("published_at")]
        dates.sort()
        
        summary = {
            "total_items": len(items),
            "date_range": {
                "start": dates[0] if dates else None,
                "end": dates[-1] if dates else None
            },
            "by_source": dict(Counter(item.get("source", "Unknown") for item in items)),
            "by_category": {},
            "by_day": {}
        }
        
        # 按分类统计
        for item in items:
            categories = item.get("categories", [])
            if isinstance(categories, str):
                try:
                    categories = json.loads(categories)
                except:
                    categories = [categories]
            
            for cat in categories:
                summary["by_category"][cat] = summary["by_category"].get(cat, 0) + 1
        
        # 按天统计
        for item in items:
            pub_date = item.get("published_at", "")
            if pub_date:
                day = pub_date[:10]  # 提取 YYYY-MM-DD
                summary["by_day"][day] = summary["by_day"].get(day, 0) + 1
        
        return summary
    
    def export_to_markdown(self, items: List[Dict], output_path: str):
        """
        导出为Markdown报告
        
        Args:
            items: 新闻条目列表
            output_path: 输出文件路径
        """
        if not items:
            print("[警告] 没有新闻可导出")
            return
        
        # 生成分类
        summary = self.generate_summary(items)
        
        # 按分类分组
        by_category = {}
        for item in items:
            primary = item.get("primary_category", "market")
            if primary not in by_category:
                by_category[primary] = []
            by_category[primary].append(item)
        
        # 分类名称映射
        category_names = {
            "regulation": "📋 监管动态",
            "institutional": "🏦 机构动向",
            "macro": "🌍 宏观市场",
            "technology": "💻 技术发展",
            "market": "📈 市场行情",
            "security": "🔒 安全事件",
            "adoption": "🚀 采用进展"
        }
        
        # 生成Markdown
        lines = [
            "# 加密新闻存档报告",
            "",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**数据范围**: {summary['date_range']['start'][:10] if summary['date_range']['start'] else 'N/A'} 至 {summary['date_range']['end'][:10] if summary['date_range']['end'] else 'N/A'}",
            f"**新闻总数**: {len(items)} 条",
            "",
            "## 📊 数据概览",
            "",
            "### 按来源统计",
            ""
        ]
        
        for source, count in sorted(summary["by_source"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"- **{source}**: {count} 条")
        
        lines.extend(["", "### 按分类统计", ""])
        for cat, count in sorted(summary["by_category"].items(), key=lambda x: x[1], reverse=True):
            cat_name = category_names.get(cat, cat)
            lines.append(f"- **{cat_name}**: {count} 条")
        
        lines.extend(["", "---", ""])
        
        # 详细内容
        lines.extend(["## 📰 详细内容", ""])
        
        for category, cat_items in sorted(by_category.items()):
            cat_name = category_names.get(category, category)
            lines.extend([f"### {cat_name}", ""])
            
            for item in sorted(cat_items, key=lambda x: x.get("published_at", ""), reverse=True):
                title = item.get("title", "")
                url = item.get("url", "")
                pub_date = item.get("published_at", "")[:16]  # 截取到分钟
                source = item.get("source", "")
                summary_text = item.get("summary", "")
                
                lines.append(f"#### [{title}]({url})")
                lines.append(f"- **来源**: {source} | **时间**: {pub_date}")
                if summary_text:
                    lines.append(f"- **摘要**: {summary_text[:200]}...")
                lines.append("")
        
        lines.extend(["", "---", "", "*数据来源: RSS订阅 + 公开网页爬取（无需API密钥）*"])
        
        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print(f"[导出] Markdown报告已保存到 {output_path}")
    
    def export_to_json(self, items: List[Dict], output_path: str, 
                       include_summary: bool = True):
        """
        导出为JSON
        
        Args:
            items: 新闻条目列表
            output_path: 输出文件路径
            include_summary: 是否包含摘要统计
        """
        output_data = {
            "meta": {
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "total_items": len(items),
                "note": "数据来源: RSS订阅 + 公开网页爬取（无需API密钥）"
            },
            "items": items
        }
        
        if include_summary:
            output_data["summary"] = self.generate_summary(items)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"[导出] JSON数据已保存到 {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="加密新闻历史存档获取（无需API密钥）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 获取指定日期范围的新闻
  python fetch_archive.py --start-date 2026-01-01 --end-date 2026-01-31
  
  # 输出为Markdown格式
  python fetch_archive.py --start-date 2026-01-01 --end-date 2026-01-31 --format markdown
  
  # 强制刷新(忽略缓存)
  python fetch_archive.py --start-date 2026-01-01 --end-date 2026-01-31 --force
  
  # 自定义输出路径
  python fetch_archive.py --start-date 2026-01-01 --end-date 2026-01-31 --output ./reports/jan_news.json
        """
    )
    
    parser.add_argument("--start-date", required=True, 
                       help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, 
                       help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--output", 
                       help="输出文件路径(默认为 news_archive_{日期}.json)")
    parser.add_argument("--format", choices=["json", "markdown"], default="json",
                       help="输出格式")
    parser.add_argument("--force", action="store_true",
                       help="强制刷新(忽略本地缓存)")
    parser.add_argument("--config", default="./config/config.json",
                       help="配置文件路径")
    
    args = parser.parse_args()
    
    # 验证日期格式
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        print("[错误] 日期格式错误，请使用 YYYY-MM-DD 格式")
        sys.exit(1)
    
    # 确保开始日期 <= 结束日期
    if args.start_date > args.end_date:
        print("[错误] 开始日期不能晚于结束日期")
        sys.exit(1)
    
    print("=" * 70)
    print("Crypto News Archive - 历史新闻获取（爬虫版）")
    print(f"时间范围: {args.start_date} 到 {args.end_date}")
    print("无需API密钥，使用公开数据源")
    print("=" * 70)
    
    # 初始化存档管理器
    config_path = args.config if Path(args.config).exists() else None
    archive = CryptoNewsArchive(config_path=config_path)
    
    # 获取新闻
    items = archive.fetch_news(args.start_date, args.end_date, force_refresh=args.force)
    
    if not items:
        print("\n[警告] 未获取到任何新闻")
        sys.exit(0)
    
    # 生成摘要
    summary = archive.generate_summary(items)
    
    print("\n" + "=" * 70)
    print("📊 数据摘要")
    print("=" * 70)
    print(f"新闻总数: {summary['total_items']}")
    print(f"时间范围: {summary['date_range']['start'][:10] if summary['date_range']['start'] else 'N/A'} 至 {summary['date_range']['end'][:10] if summary['date_range']['end'] else 'N/A'}")
    print("\n按来源统计:")
    for source, count in sorted(summary["by_source"].items(), key=lambda x: x[1], reverse=True):
        print(f"  - {source}: {count} 条")
    print("\n按分类统计:")
    category_names = {
        "regulation": "监管动态",
        "institutional": "机构动向",
        "macro": "宏观市场",
        "technology": "技术发展",
        "market": "市场行情",
        "security": "安全事件",
        "adoption": "采用进展"
    }
    for cat, count in sorted(summary["by_category"].items(), key=lambda x: x[1], reverse=True):
        cat_name = category_names.get(cat, cat)
        print(f"  - {cat_name}: {count} 条")
    
    # 确定输出路径
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = f"news_archive_{args.start_date}_to_{args.end_date}_{timestamp}"
    
    # 根据格式导出
    if args.format == "markdown":
        if not output_path.endswith('.md'):
            output_path += '.md'
        archive.export_to_markdown(items, output_path)
    else:
        if not output_path.endswith('.json'):
            output_path += '.json'
        archive.export_to_json(items, output_path)
    
    print("\n" + "=" * 70)
    print(f"[完成] 已保存到 {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
