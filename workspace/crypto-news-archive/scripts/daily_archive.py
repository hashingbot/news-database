#!/usr/bin/env python3
"""
Crypto News Archive - 每日自动存档脚本 (Phase 1 更新版)
用于长期运行，持续积累历史数据

生成时间: 2026-03-04
"""

import json
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
import sys

# 导入爬虫模块
sys.path.insert(0, str(Path(__file__).parent))
from crawler import CryptoNewsCrawler


def daily_archive(days_back: int = 1):
    """
    每日存档任务
    
    Args:
        days_back: 获取多少天内的数据(默认1天)
    
    Returns:
        存档的新闻数量
    """
    archive_dir = Path("./archive")
    archive_dir.mkdir(exist_ok=True)
    
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"[{datetime.now()}] 开始每日存档任务")
    print(f"       存档日期: {yesterday}")
    
    # 初始化爬虫
    crawler = CryptoNewsCrawler()
    
    # 获取新闻数据
    items = crawler.fetch_all(days_back=days_back, use_cache=False)
    
    # 统计今日新增
    today_items = [item for item in items 
                   if item.get('published_at', '').startswith(yesterday)]
    
    # 记录存档日志
    log_entry = {
        "date": yesterday,
        "archived_at": datetime.now().isoformat(),
        "items_count": len(today_items),
        "total_fetched": len(items),
        "sources": list(set(item.get('source') for item in items)),
        "status": "success" if items else "warning",
        "stats": crawler.get_stats()
    }
    
    # 保存日志
    log_file = archive_dir / "archive_log.jsonl"
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
    
    print(f"[{datetime.now()}] 存档完成: {len(today_items)} 条新闻")
    return len(today_items)


def check_data_integrity():
    """检查数据完整性"""
    archive_dir = Path("./archive")
    db_path = archive_dir / "news_archive.db"
    
    if not db_path.exists():
        print("[警告] 数据库不存在")
        return False
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 检查最近7天的数据量
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    cursor.execute(
        "SELECT COUNT(*) FROM news_items WHERE date(published_at) >= date(?)",
        (seven_days_ago,)
    )
    count = cursor.fetchone()[0]
    
    # 检查各数据源分布
    cursor.execute("""
        SELECT source, COUNT(*) as cnt 
        FROM news_items 
        WHERE date(published_at) >= date(?)
        GROUP BY source
        ORDER BY cnt DESC
    """, (seven_days_ago,))
    
    source_stats = cursor.fetchall()
    
    conn.close()
    
    print(f"[检查] 最近7天数据分布:")
    for source, cnt in source_stats:
        print(f"       - {source}: {cnt} 条")
    
    if count < 20:  # 调整为20条，因为网页爬虫可能被禁止
        print(f"[警告] 最近7天只有 {count} 条新闻，数据可能不完整")
        return False
    
    print(f"[检查] 最近7天有 {count} 条新闻，数据正常")
    return True


def generate_weekly_report():
    """生成周度新闻报告"""
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    print(f"[报告] 生成周度新闻报告 ({start_date} 至 {end_date})...")
    
    # 调用爬虫查询
    crawler = CryptoNewsCrawler()
    items = crawler.query_archive(start_date, end_date)
    
    if not items:
        print("[警告] 没有数据可生成报告")
        return None
    
    # 生成摘要
    from collections import Counter
    summary = {
        "total_items": len(items),
        "by_source": dict(Counter(item.get("source", "Unknown") for item in items)),
        "by_category": {}
    }
    
    for item in items:
        cats = item.get("categories", [])
        if isinstance(cats, str):
            try:
                cats = json.loads(cats)
            except:
                cats = [cats]
        for cat in cats:
            summary["by_category"][cat] = summary["by_category"].get(cat, 0) + 1
    
    # 保存报告
    report_dir = Path("./reports")
    report_dir.mkdir(exist_ok=True)
    
    week_str = datetime.now().strftime("%Y-W%U")
    report_file = report_dir / f"{week_str}-weekly-report.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            "week": week_str,
            "date_range": {"start": start_date, "end": end_date},
            "generated_at": datetime.now().isoformat(),
            "summary": summary,
            "items": items[:100]  # 限制条目数
        }, f, ensure_ascii=False, indent=2)
    
    print(f"[报告] 已保存到 {report_file}")
    return report_file


def generate_monthly_report(year: int, month: int):
    """生成月度新闻报告"""
    start_date = f"{year}-{month:02d}-01"
    if month == 12:
        end_date = f"{year+1}-01-01"
    else:
        end_date = f"{year}-{month+1:02d}-01"
    
    print(f"[报告] 生成 {year}-{month:02d} 月度新闻报告...")
    
    # 调用爬虫查询
    crawler = CryptoNewsCrawler()
    items = crawler.query_archive(start_date, end_date)
    
    if not items:
        print("[警告] 没有数据可生成报告")
        return None
    
    # 生成摘要
    from collections import Counter
    summary = {
        "total_items": len(items),
        "by_source": dict(Counter(item.get("source", "Unknown") for item in items)),
        "by_category": {}
    }
    
    for item in items:
        cats = item.get("categories", [])
        if isinstance(cats, str):
            try:
                cats = json.loads(cats)
            except:
                cats = [cats]
        for cat in cats:
            summary["by_category"][cat] = summary["by_category"].get(cat, 0) + 1
    
    # 保存报告
    report_dir = Path("./reports")
    report_dir.mkdir(exist_ok=True)
    
    report_file = report_dir / f"{year}-{month:02d}-monthly-report.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            "month": f"{year}-{month:02d}",
            "date_range": {"start": start_date, "end": end_date},
            "generated_at": datetime.now().isoformat(),
            "summary": summary,
            "items": items
        }, f, ensure_ascii=False, indent=2)
    
    print(f"[报告] 已保存到 {report_file}")
    return report_file


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="每日存档任务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 执行每日存档
  python daily_archive.py --archive
  
  # 检查数据完整性
  python daily_archive.py --check
  
  # 生成周度报告
  python daily_archive.py --weekly-report
  
  # 生成月度报告
  python daily_archive.py --monthly-report --month 2 --year 2026
        """
    )
    
    parser.add_argument("--archive", action="store_true", help="执行每日存档")
    parser.add_argument("--check", action="store_true", help="检查数据完整性")
    parser.add_argument("--weekly-report", action="store_true", help="生成周度报告")
    parser.add_argument("--monthly-report", action="store_true", help="生成月度报告")
    parser.add_argument("--month", type=int, help="月份 (1-12)")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="年份")
    parser.add_argument("--days", type=int, default=1, help="获取多少天的数据")
    
    args = parser.parse_args()
    
    if args.archive:
        daily_archive(days_back=args.days)
    elif args.check:
        check_data_integrity()
    elif args.weekly_report:
        generate_weekly_report()
    elif args.monthly_report and args.month:
        generate_monthly_report(args.year, args.month)
    else:
        # 默认执行每日存档
        daily_archive(days_back=args.days)
