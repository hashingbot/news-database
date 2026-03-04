#!/usr/bin/env python3
"""
Query Tool - 新闻数据库查询工具
支持日期范围、分类、关键词查询和导出功能
"""

import json
import sys
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# 添加crypto-news-archive到路径
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir / "crypto-news-archive" / "scripts"))

from database import create_database, DatabaseManager
from query import QueryEngine

# 项目路径
PROJECT_DIR = Path("/root/.openclaw/workspace/projects/news-database")
ARCHIVE_DIR = PROJECT_DIR / "archive"
DB_PATH = ARCHIVE_DIR / "news_archive.db"


def get_date_range(days: int = None, start: str = None, end: str = None) -> tuple:
    """获取日期范围"""
    if days:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    elif start and end:
        return start, end
    elif start:
        return start, datetime.now().strftime("%Y-%m-%d")
    else:
        # 默认最近7天
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def print_results(items: list, title: str = "查询结果"):
    """打印查询结果"""
    print("\n" + "=" * 70)
    print(f"📰 {title} (共 {len(items)} 条)")
    print("=" * 70)
    
    if not items:
        print("未找到匹配的新闻")
        return
    
    for i, item in enumerate(items, 1):
        print(f"\n{i}. [{item.get('primary_category', 'market').upper()}] {item.get('title', 'No title')}")
        print(f"   📅 {item.get('published_at', 'Unknown date')}")
        print(f"   📰 {item.get('source', 'Unknown')}")
        print(f"   😊 情绪: {item.get('sentiment', 'neutral')} | 影响力: {item.get('impact_score', 5)}/10")
        if item.get('summary'):
            summary = item['summary'][:150] + "..." if len(item['summary']) > 150 else item['summary']
            print(f"   📝 {summary}")
        print(f"   🔗 {item.get('url', 'No URL')}")


def print_stats(stats: dict):
    """打印统计信息"""
    print("\n" + "=" * 70)
    print("📊 统计信息")
    print("=" * 70)
    
    print(f"\n总计: {stats.get('total', 0)} 条新闻")
    
    print("\n📁 按分类:")
    for cat, count in stats.get('by_category', {}).items():
        bar = "█" * (count // 2)
        print(f"   {cat:15} {count:4} {bar}")
    
    print("\n📡 按来源:")
    for src, count in stats.get('by_source', {}).items():
        bar = "█" * (count // 2)
        print(f"   {src:15} {count:4} {bar}")
    
    print("\n😊 情绪分布:")
    sentiment = stats.get('by_sentiment', {})
    total_sent = sum(sentiment.values())
    if total_sent > 0:
        for sent, count in sentiment.items():
            pct = count / total_sent * 100
            bar = "█" * int(pct / 5)
            emoji = {"positive": "😀", "neutral": "😐", "negative": "😔"}.get(sent, "")
            print(f"   {emoji} {sent:10} {count:4} ({pct:5.1f}%) {bar}")


def export_json(items: list, output_path: str):
    """导出为JSON"""
    output = {
        "meta": {
            "export_type": "crypto_news_query",
            "generated_at": datetime.now().isoformat(),
            "total_items": len(items)
        },
        "items": items
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 已导出到: {output_path}")


def export_html(items: list, stats: dict, output_path: str, start_date: str, end_date: str):
    """导出为HTML报告"""
    
    # 生成HTML
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crypto News Report - {start_date} to {end_date}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
        h1 {{ color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .meta {{ background: #fff; padding: 15px; border-radius: 8px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
        .stat-card {{ background: #fff; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .stat-card h3 {{ margin: 0 0 10px 0; color: #666; font-size: 14px; }}
        .stat-card .value {{ font-size: 28px; font-weight: bold; color: #4CAF50; }}
        .news-item {{ background: #fff; padding: 15px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .news-item .title {{ font-size: 16px; font-weight: bold; color: #333; margin-bottom: 8px; }}
        .news-item .meta {{ background: transparent; padding: 0; box-shadow: none; margin: 5px 0; font-size: 12px; color: #666; }}
        .news-item .summary {{ color: #555; margin: 10px 0; line-height: 1.5; }}
        .category {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }}
        .category-regulation {{ background: #ffebee; color: #c62828; }}
        .category-institutional {{ background: #e3f2fd; color: #1565c0; }}
        .category-macro {{ background: #f3e5f5; color: #6a1b9a; }}
        .category-technology {{ background: #e8f5e9; color: #2e7d32; }}
        .category-market {{ background: #fff3e0; color: #e65100; }}
        .category-security {{ background: #fce4ec; color: #c2185b; }}
        .category-adoption {{ background: #e0f2f1; color: #00695c; }}
        .sentiment-positive {{ color: #4CAF50; }}
        .sentiment-neutral {{ color: #757575; }}
        .sentiment-negative {{ color: #f44336; }}
        .bar {{ height: 20px; background: #e0e0e0; border-radius: 10px; overflow: hidden; margin: 5px 0; }}
        .bar-fill {{ height: 100%; background: #4CAF50; border-radius: 10px; }}
    </style>
</head>
<body>
    <h1>📰 Crypto News Report</h1>
    <div class="meta">
        <strong>报告周期:</strong> {start_date} 至 {end_date}<br>
        <strong>生成时间:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}<br>
        <strong>总计新闻:</strong> {len(items)} 条
    </div>
"""
    
    # 统计部分
    if stats:
        html += """
    <h2>📊 统计概览</h2>
    <div class="stats">
        <div class="stat-card">
            <h3>总新闻数</h3>
            <div class="value">{}</div>
        </div>
    </div>
    
    <h3>📁 分类分布</h3>
""".format(stats.get('total', 0))
        
        for cat, count in stats.get('by_category', {}).items():
            pct = count / stats.get('total', 1) * 100
            html += f"""
        <div style="margin: 10px 0;">
            <span style="display:inline-block; width:100px;">{cat}</span>
            <div class="bar" style="display:inline-block; width:200px; vertical-align:middle;">
                <div class="bar-fill" style="width:{pct}%"></div>
            </div>
            <span style="margin-left:10px;">{count} ({pct:.1f}%)</span>
        </div>
"""
    
    # 新闻列表
    html += """
    <h2>📰 新闻列表</h2>
"""
    
    for item in items:
        cat = item.get('primary_category', 'market')
        sent = item.get('sentiment', 'neutral')
        sent_class = f"sentiment-{sent}"
        sent_emoji = {"positive": "😀", "neutral": "😐", "negative": "😔"}.get(sent, "")
        
        html += f"""
    <div class="news-item">
        <div class="title">{item.get('title', 'No title')}</div>
        <div class="meta">
            <span class="category category-{cat}">{cat.upper()}</span>
            <span style="margin-left:10px;">📅 {item.get('published_at', 'Unknown')}</span>
            <span style="margin-left:10px;">📰 {item.get('source', 'Unknown')}</span>
            <span style="margin-left:10px;" class="{sent_class}">{sent_emoji} {sent}</span>
            <span style="margin-left:10px;">⭐ {item.get('impact_score', 5)}/10</span>
        </div>
        <div class="summary">{item.get('summary', '')}</div>
        <a href="{item.get('url', '#')}" target="_blank">阅读原文 →</a>
    </div>
"""
    
    html += """
</body>
</html>
"""
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"✅ 已导出到: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="新闻数据库查询工具")
    parser.add_argument("--start", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="查询最近N天")
    parser.add_argument("--category", help="按分类查询 (regulation/institutional/macro/technology/market/security/adoption)")
    parser.add_argument("--keyword", help="关键词搜索")
    parser.add_argument("--stats", action="store_true", help="显示统计信息")
    parser.add_argument("--export", choices=["json", "html"], help="导出格式")
    parser.add_argument("--output", help="输出文件路径")
    
    args = parser.parse_args()
    
    # 检查数据库是否存在
    if not DB_PATH.exists():
        print(f"⚠️ 数据库不存在: {DB_PATH}")
        print("请先运行 daily_fetch.py 拉取数据")
        return 1
    
    # 初始化查询引擎
    db = create_database(DB_PATH)
    query_engine = QueryEngine(db)
    
    # 确定日期范围
    start_date, end_date = get_date_range(args.days, args.start, args.end)
    
    print(f"📅 查询日期范围: {start_date} 至 {end_date}")
    
    # 执行查询
    items = []
    
    if args.keyword:
        print(f"🔍 关键词搜索: '{args.keyword}'")
        items = query_engine.query_by_keyword(args.keyword, start_date, end_date)
    elif args.category:
        print(f"📁 分类查询: {args.category}")
        items = query_engine.query_by_category(args.category, start_date, end_date)
    else:
        items = query_engine.query_by_date_range(start_date, end_date)
    
    # 获取统计信息
    stats = query_engine.get_statistics(start_date, end_date)
    
    # 输出结果
    if args.export:
        output_path = args.output
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            ext = "json" if args.export == "json" else "html"
            output_path = str(PROJECT_DIR / "deliverables" / f"news_report_{timestamp}.{ext}")
            # 确保deliverables目录存在
            (PROJECT_DIR / "deliverables").mkdir(parents=True, exist_ok=True)
        
        if args.export == "json":
            export_json(items, output_path)
        else:
            export_html(items, stats, output_path, start_date, end_date)
    else:
        print_results(items)
        
        if args.stats:
            print_stats(stats)
    
    return 0


if __name__ == "__main__":
    exit(main())
