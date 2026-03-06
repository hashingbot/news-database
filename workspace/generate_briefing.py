#!/usr/bin/env python3
"""
每日行业新闻简报生成脚本（中文版）
生成昨日加密行业新闻摘要、简评并发送到Telegram
"""

import json
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# 项目路径
PROJECT_DIR = Path("/root/.openclaw/workspace/projects/news-database")
ARCHIVE_DIR = PROJECT_DIR / "archive"
LOGS_DIR = PROJECT_DIR / "logs"

def get_yesterday_filename():
    """获取昨天文件名"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return f"news_{yesterday}.json"

def get_yesterday_date():
    """获取昨天日期字符串"""
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def load_news_data():
    """加载昨天的新闻数据"""
    filename = get_yesterday_filename()
    filepath = ARCHIVE_DIR / filename
    
    if not filepath.exists():
        # 尝试加载今天数据（如果是早上第一次运行）
        today_filename = f"news_{datetime.now().strftime('%Y-%m-%d')}.json"
        today_filepath = ARCHIVE_DIR / today_filename
        if today_filepath.exists():
            filepath = today_filepath
        else:
            return None, f"未找到新闻数据: {filename}"
    
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f), None

def get_category_name(cat):
    """获取分类中文名"""
    category_names = {
        'regulation': '监管政策',
        'institutional': '机构动态',
        'market': '市场行情',
        'technology': '技术创新',
        'security': '安全事件',
        'macro': '宏观环境',
        'adoption': '应用落地'
    }
    return category_names.get(cat, '其他')

def get_category_emoji(cat):
    """获取分类emoji"""
    return {
        'regulation': '📜',
        'institutional': '🏦',
        'market': '📊',
        'technology': '💻',
        'security': '🔒',
        'macro': '🌍',
        'adoption': '🚀'
    }.get(cat, '📰')

def generate_summary_text(item):
    """生成新闻摘要（基于已有内容简化）"""
    title = item.get('title', '')
    summary = item.get('summary', '')
    
    # 如果有摘要就使用，否则基于标题生成简化版
    if summary and len(summary) > 20:
        # 截取前100字符
        text = summary[:100].strip()
        if len(summary) > 100:
            text += "..."
        return text
    else:
        # 基于标题的简单处理
        return "该新闻涉及加密市场的最新动态，详情请点击查看原文。"

def generate_commentary(items, summary_stats):
    """生成市场简评"""
    by_category = summary_stats.get('by_category', {})
    sentiment = summary_stats.get('sentiment', {})
    total = summary_stats.get('total', 0)
    
    if total == 0:
        return "暂无足够数据进行市场评论。"
    
    # 计算情绪比例
    pos = sentiment.get('positive', 0)
    neg = sentiment.get('negative', 0)
    neu = sentiment.get('neutral', 0)
    total_sent = pos + neg + neu
    
    if total_sent == 0:
        sentiment_comment = "市场情绪尚不明朗。"
    else:
        pos_pct = pos / total_sent * 100
        neg_pct = neg / total_sent * 100
        
        if pos_pct > 40:
            sentiment_comment = "市场情绪整体偏正面，投资者信心较强。"
        elif neg_pct > 40:
            sentiment_comment = "市场情绪偏谨慎，需关注潜在风险因素。"
        else:
            sentiment_comment = "市场情绪中性，建议投资者保持观望。"
    
    # 分类评论
    cat_comments = []
    if by_category.get('regulation', 0) >= 5:
        cat_comments.append("监管政策方面有较多动态，建议密切关注合规风险。")
    if by_category.get('institutional', 0) >= 5:
        cat_comments.append("机构资金流入迹象明显，市场成熟度持续提升。")
    if by_category.get('market', 0) >= 5:
        cat_comments.append("市场波动加剧，注意控制仓位风险。")
    if by_category.get('technology', 0) >= 3:
        cat_comments.append("技术创新活跃，可能存在新的投资机会。")
    if by_category.get('security', 0) >= 3:
        cat_comments.append("安全事件频发，提醒注意资产安全防护。")
    
    if not cat_comments:
        cat_comments.append("各板块表现平稳，暂无重大事件驱动。")
    
    # 组合评论
    commentary = f"{sentiment_comment}\n"
    commentary += "\n".join([f"• {c}" for c in cat_comments])
    
    return commentary

def generate_briefing(data):
    """生成新闻简报（中文版）"""
    if not data:
        return "📭 暂无新闻数据"
    
    meta = data.get('meta', {})
    summary = data.get('summary', {})
    items = data.get('items', [])
    
    date_str = meta.get('date', get_yesterday_date())
    total = summary.get('total', 0)
    
    # 分类统计
    by_category = summary.get('by_category', {})
    sentiment = summary.get('sentiment', {})
    
    # 获取Top 5高影响力新闻
    high_impact = sorted(
        [item for item in items if item.get('impact_score', 5) >= 6],
        key=lambda x: x.get('impact_score', 0),
        reverse=True
    )[:5]
    
    # 如果没有高影响力新闻，取最新的5条
    if not high_impact and items:
        high_impact = items[:5]
    
    # 构建简报
    lines = []
    lines.append(f"📰 加密行业新闻简报")
    lines.append(f"📅 日期: {date_str}")
    lines.append("=" * 42)
    lines.append("")
    
    # 总体统计
    lines.append(f"📊 昨日收录: {total} 条新闻")
    lines.append("")
    
    # 情绪分析
    if sentiment:
        pos = sentiment.get('positive', 0)
        neu = sentiment.get('neutral', 0)
        neg = sentiment.get('negative', 0)
        total_sent = pos + neu + neg
        if total_sent > 0:
            pos_pct = pos / total_sent * 100
            neg_pct = neg / total_sent * 100
            lines.append(f"📈 情绪指数: 正面{pos_pct:.0f}% | 中性{neu/total_sent*100:.0f}% | 负面{neg_pct:.0f}%")
            lines.append("")
    
    # 分类统计
    if by_category:
        lines.append("📂 分类统计:")
        for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
            name = get_category_name(cat)
            emoji = get_category_emoji(cat)
            lines.append(f"  {emoji} {name}: {count}条")
        lines.append("")
    
    # 市场简评
    lines.append("💬 市场简评")
    lines.append("-" * 42)
    commentary = generate_commentary(items, summary)
    lines.append(commentary)
    lines.append("")
    
    # Top 5 新闻
    lines.append("🔥 重点新闻")
    lines.append("-" * 42)
    
    for i, item in enumerate(high_impact, 1):
        title = item.get('title', '无标题')
        source = item.get('source', '未知来源')
        category = item.get('primary_category', 'market')
        impact = item.get('impact_score', 5)
        url = item.get('url', '')
        
        emoji = get_category_emoji(category)
        cat_name = get_category_name(category)
        
        # 新闻标题和来源
        lines.append(f"{i}. {emoji} [{cat_name}] {title}")
        lines.append(f"   📍 来源: {source}")
        
        # 影响力标记
        if impact >= 8:
            lines.append(f"   🔥 影响力: 高 ({impact}/10)")
        elif impact >= 6:
            lines.append(f"   ⚡ 影响力: 中 ({impact}/10)")
        
        # 简要摘要
        summary_text = generate_summary_text(item)
        lines.append(f"   📝 摘要: {summary_text}")
        
        lines.append("")
    
    lines.append("=" * 42)
    lines.append("💡 提示: 回复\"查看更多\"可获取详细新闻列表")
    
    return "\n".join(lines)

def log_message(message):
    """记录日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    print(log_line)
    
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "briefing.log"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")

def main():
    log_message("开始生成每日新闻简报(中文版)...")
    
    # 加载数据
    data, error = load_news_data()
    if error:
        log_message(f"错误: {error}")
        return 1
    
    # 生成简报
    briefing = generate_briefing(data)
    
    # 输出生成的简报（用于cron捕获）
    print("\n" + "="*50)
    print("GENERATED_BRIEFING_START")
    print(briefing)
    print("GENERATED_BRIEFING_END")
    print("="*50 + "\n")
    
    log_message("简报生成完成")
    return 0

if __name__ == "__main__":
    exit(main())
