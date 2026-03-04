#!/usr/bin/env python3
"""
Daily Fetch Script - 每日新闻拉取脚本
自动拉取当天加密市场新闻并生成摘要报告
"""

import json
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# 添加crypto-news-archive到路径
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir / "crypto-news-archive" / "scripts"))

from crawler import CryptoNewsCrawler
from database import create_database

# 项目路径
PROJECT_DIR = Path("/root/.openclaw/workspace/projects/news-database")
LOGS_DIR = PROJECT_DIR / "logs"
ARCHIVE_DIR = PROJECT_DIR / "workspace" / "crypto-news-archive" / "archive"


def get_today_filename():
    """获取今日文件名"""
    today = datetime.now().strftime("%Y-%m-%d")
    return f"news_{today}.json"


def generate_summary(items: list) -> dict:
    """生成每日摘要"""
    if not items:
        return {
            "total": 0,
            "by_category": {},
            "by_source": {},
            "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            "high_impact": []
        }
    
    by_category = {}
    by_source = {}
    sentiment = {"positive": 0, "neutral": 0, "negative": 0}
    high_impact = []
    
    for item in items:
        # 分类统计
        cat = item.get("primary_category", "market")
        by_category[cat] = by_category.get(cat, 0) + 1
        
        # 来源统计
        src = item.get("source", "Unknown")
        by_source[src] = by_source.get(src, 0) + 1
        
        # 情绪统计
        sent = item.get("sentiment", "neutral")
        sentiment[sent] = sentiment.get(sent, 0) + 1
        
        # 高影响力新闻
        if item.get("impact_score", 5) >= 8:
            high_impact.append({
                "title": item["title"],
                "category": cat,
                "impact_score": item["impact_score"],
                "url": item.get("url", "")
            })
    
    # 按影响力排序
    high_impact.sort(key=lambda x: x["impact_score"], reverse=True)
    
    return {
        "total": len(items),
        "by_category": by_category,
        "by_source": by_source,
        "sentiment": sentiment,
        "high_impact": high_impact[:5]  # 前5条高影响力
    }


def log_message(message: str):
    """记录日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    print(log_line)
    
    # 写入日志文件
    log_file = LOGS_DIR / "cron.log"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")


def main():
    log_message("=" * 70)
    log_message("开始每日新闻拉取任务")
    log_message("=" * 70)
    
    try:
        # 确保目录存在
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        
        # 初始化爬虫
        crawler = CryptoNewsCrawler(
            config_path=str(PROJECT_DIR / "workspace" / "crypto-news-archive" / "config" / "config.json"),
            delay=2.0,
            max_retries=3
        )
        
        # 拉取过去1天的新闻
        log_message("正在拉取新闻数据...")
        items = crawler.fetch_all(days_back=1)
        
        # 生成摘要
        summary = generate_summary(items)
        
        # 今日文件名
        filename = get_today_filename()
        filepath = ARCHIVE_DIR / filename
        
        # 保存结果
        output = {
            "meta": {
                "generated_at": datetime.now().isoformat(),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "total_items": len(items),
                "sources": list(set(item["source"] for item in items)),
                "stats": crawler.get_stats()
            },
            "summary": summary,
            "items": items
        }
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        log_message(f"✅ 成功拉取 {len(items)} 条新闻")
        log_message(f"✅ 数据已保存到: {filepath}")
        log_message(f"📊 分类统计: {summary['by_category']}")
        log_message(f"📊 来源统计: {summary['by_source']}")
        log_message(f"📊 情绪分布: {summary['sentiment']}")
        
        if summary["high_impact"]:
            log_message(f"🔥 高影响力新闻 ({len(summary['high_impact'])}条):")
            for item in summary["high_impact"]:
                log_message(f"   [{item['category'].upper()}] {item['title'][:60]}...")
        
        log_message("✅ 任务完成")
        return 0
        
    except Exception as e:
        log_message(f"❌ 错误: {str(e)}")
        import traceback
        log_message(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit(main())
