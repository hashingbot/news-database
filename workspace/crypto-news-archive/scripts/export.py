#!/usr/bin/env python3
"""
Crypto News Archive - Export Module
数据导出功能实现 - 支持JSON和Markdown格式

功能：
- 导出为JSON格式
- 导出为Markdown格式（供基金月报使用）
- 支持按时间范围筛选
- 专业金融分析报告生成
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any
from collections import defaultdict

from query import QueryEngine, create_query_engine
from database import create_database


# ============================================================================
# 导出器基类
# ============================================================================

class BaseExporter:
    """导出器基类"""
    
    def __init__(self, query_engine: QueryEngine):
        self.query = query_engine
    
    def export(self, start: str, end: str, output_path: Path, **kwargs) -> Path:
        """
        导出数据
        
        Args:
            start: 开始日期 (YYYY-MM-DD)
            end: 结束日期 (YYYY-MM-DD)
            output_path: 输出文件路径
            **kwargs: 额外参数
        
        Returns:
            输出文件路径
        """
        raise NotImplementedError


# ============================================================================
# JSON导出器
# ============================================================================

class JSONExporter(BaseExporter):
    """
    JSON格式导出器
    
    输出结构化JSON数据，便于程序处理
    """
    
    def export(
        self,
        start: str,
        end: str,
        output_path: Path,
        include_stats: bool = True,
        indent: int = 2,
        **kwargs
    ) -> Path:
        """
        导出为JSON格式
        
        Args:
            start: 开始日期
            end: 结束日期
            output_path: 输出文件路径
            include_stats: 是否包含统计信息
            indent: JSON缩进空格数
        """
        print(f"[Export] 正在导出JSON数据 ({start} 至 {end})...")
        
        # 获取数据
        items = self.query.query_by_date_range(start, end)
        
        output = {
            "meta": {
                "export_type": "crypto_news",
                "version": "2.0",
                "generated_at": datetime.now().isoformat(),
                "period": {
                    "start": start,
                    "end": end
                },
                "total_items": len(items),
                "data_source": "Crypto News Archive"
            },
            "items": items
        }
        
        # 添加统计信息
        if include_stats:
            output["statistics"] = self.query.get_statistics(start, end)
        
        # 写入文件
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=indent)
        
        print(f"[Export] JSON导出完成: {output_path}")
        return output_path


# ============================================================================
# Markdown导出器（基金月报专用）
# ============================================================================

class MarkdownExporter(BaseExporter):
    """
    Markdown格式导出器
    
    生成专业的金融分析报告，适用于基金月报的市场展望章节
    """
    
    # 分类中文映射
    CATEGORY_NAMES = {
        'regulation': '监管动态',
        'institutional': '机构动向',
        'macro': '宏观市场',
        'technology': '技术发展',
        'market': '市场行情',
        'security': '安全事件',
        'adoption': '采用进展'
    }
    
    # 情绪中文映射
    SENTIMENT_NAMES = {
        'positive': '积极',
        'neutral': '中性',
        'negative': '消极'
    }
    
    def export(
        self,
        start: str,
        end: str,
        output_path: Path,
        report_title: str = None,
        include_toc: bool = True,
        **kwargs
    ) -> Path:
        """
        导出为Markdown格式
        
        Args:
            start: 开始日期
            end: 结束日期
            output_path: 输出文件路径
            report_title: 报告标题
            include_toc: 是否包含目录
        """
        print(f"[Export] 正在生成Markdown报告 ({start} 至 {end})...")
        
        # 获取数据
        items = self.query.query_by_date_range(start, end)
        stats = self.query.get_statistics(start, end)
        
        # 生成报告
        report_title = report_title or f"加密市场新闻分析报告 ({start} 至 {end})"
        markdown = self._generate_markdown(items, stats, report_title, include_toc)
        
        # 写入文件
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(markdown)
        
        print(f"[Export] Markdown导出完成: {output_path}")
        return output_path
    
    def _generate_markdown(
        self,
        items: List[Dict],
        stats: Dict,
        title: str,
        include_toc: bool
    ) -> str:
        """生成Markdown内容"""
        lines = []
        
        # 标题
        lines.append(f"# {title}")
        lines.append("")
        lines.append(f"*生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
        lines.append("")
        
        # 目录
        if include_toc:
            lines.append("## 目录")
            lines.append("")
            lines.append("1. [执行摘要](#执行摘要)")
            lines.append("2. [市场概况](#市场概况)")
            lines.append("3. [主题分析](#主题分析)")
            lines.append("4. [时间线](#时间线)")
            lines.append("5. [数据来源说明](#数据来源说明)")
            lines.append("")
        
        # 执行摘要
        lines.append("## 执行摘要")
        lines.append("")
        lines.append(self._generate_executive_summary(items, stats))
        lines.append("")
        
        # 市场概况
        lines.append("## 市场概况")
        lines.append("")
        lines.append(self._generate_market_overview(stats))
        lines.append("")
        
        # 主题分析
        lines.append("## 主题分析")
        lines.append("")
        lines.append(self._generate_theme_analysis(items, stats))
        lines.append("")
        
        # 时间线
        lines.append("## 时间线")
        lines.append("")
        lines.append(self._generate_timeline(items))
        lines.append("")
        
        # 数据来源
        lines.append("## 数据来源说明")
        lines.append("")
        lines.append(self._generate_data_source_note())
        lines.append("")
        
        return "\n".join(lines)
    
    def _generate_executive_summary(self, items: List[Dict], stats: Dict) -> str:
        """生成执行摘要"""
        total = stats.get('total', 0)
        period = stats.get('period', {})
        start = period.get('start', '')
        end = period.get('end', '')
        
        # 计算报告期间
        try:
            start_dt = datetime.strptime(start, '%Y-%m-%d')
            end_dt = datetime.strptime(end, '%Y-%m-%d')
            days = (end_dt - start_dt).days + 1
        except:
            days = 0
        
        # 主要主题
        categories = stats.get('categories', {})
        top_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:3]
        
        # 情绪倾向
        sentiments = stats.get('sentiment_distribution', {})
        total_sentiment = sum(sentiments.values()) if sentiments else 0
        
        summary_parts = [
            f"本报告期间（{start} 至 {end}，共{days}天），",
            f"共收录加密市场相关新闻 **{total}** 条，",
        ]
        
        if top_categories:
            cat_names = [f"{self.CATEGORY_NAMES.get(c, c)}({n}条)" for c, n in top_categories]
            summary_parts.append(f"主要关注主题包括：{'、'.join(cat_names)}。")
        
        if total_sentiment > 0:
            pos_ratio = sentiments.get('positive', 0) / total_sentiment
            neg_ratio = sentiments.get('negative', 0) / total_sentiment
            
            if pos_ratio > 0.5:
                summary_parts.append("整体市场情绪偏向**积极**。")
            elif neg_ratio > 0.5:
                summary_parts.append("整体市场情绪偏向**谨慎**。")
            else:
                summary_parts.append("整体市场情绪保持**中性**。")
        
        return "".join(summary_parts)
    
    def _generate_market_overview(self, stats: Dict) -> str:
        """生成市场概况"""
        lines = []
        
        # 总体统计
        lines.append("### 总体统计")
        lines.append("")
        lines.append(f"- **新闻总数**: {stats.get('total', 0)} 条")
        
        avg_impact = stats.get('average_impact_score', 0)
        if avg_impact:
            lines.append(f"- **平均影响力**: {avg_impact}/10")
        lines.append("")
        
        # 分类分布
        categories = stats.get('categories', {})
        if categories:
            lines.append("### 主题分布")
            lines.append("")
            lines.append("| 主题 | 数量 | 占比 |")
            lines.append("|------|------|------|")
            
            total = stats.get('total', 1)
            for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total) * 100
                cat_name = self.CATEGORY_NAMES.get(cat, cat)
                lines.append(f"| {cat_name} | {count} | {percentage:.1f}% |")
            lines.append("")
        
        # 来源分布
        sources = stats.get('sources', {})
        if sources:
            lines.append("### 数据来源")
            lines.append("")
            for src, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- {src}: {count} 条")
            lines.append("")
        
        # 情绪分布
        sentiments = stats.get('sentiment_distribution', {})
        if sentiments:
            lines.append("### 市场情绪")
            lines.append("")
            lines.append("| 情绪 | 数量 |")
            lines.append("|------|------|")
            for sent, count in sorted(sentiments.items(), key=lambda x: x[1], reverse=True):
                sent_name = self.SENTIMENT_NAMES.get(sent, sent)
                lines.append(f"| {sent_name} | {count} |")
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_theme_analysis(self, items: List[Dict], stats: Dict) -> str:
        """生成主题分析"""
        lines = []
        
        # 按分类分组
        by_category = defaultdict(list)
        for item in items:
            cat = item.get('primary_category', 'market')
            by_category[cat].append(item)
        
        # 为每个主要分类生成分析
        for cat, cat_items in sorted(by_category.items(), key=lambda x: len(x[1]), reverse=True):
            cat_name = self.CATEGORY_NAMES.get(cat, cat)
            lines.append(f"### {cat_name}")
            lines.append("")
            
            # 生成该主题的概括性描述
            description = self._generate_category_description(cat, cat_items)
            lines.append(description)
            lines.append("")
            
            # 高影响力事件（前3个）
            high_impact = sorted(
                cat_items,
                key=lambda x: x.get('impact_score', 5),
                reverse=True
            )[:3]
            
            if high_impact:
                lines.append("**重点事件:**")
                lines.append("")
                for item in high_impact:
                    date = item.get('published_at', '')[:10]
                    title = item.get('title', '')
                    source = item.get('source', 'Unknown')
                    lines.append(f"- [{date}] {title} (*{source}*)")
                lines.append("")
        
        return "\n".join(lines)
    
    def _generate_category_description(self, category: str, items: List[Dict]) -> str:
        """生成分类描述"""
        count = len(items)
        
        descriptions = {
            'regulation': f"报告期内共收录监管相关新闻 **{count}** 条，主要涉及政策法规更新、监管机构表态及合规要求变化。建议关注监管环境对市场的潜在影响。",
            'institutional': f"报告期内共收录机构动向新闻 **{count}** 条，涵盖传统金融机构入场、投资产品发行及大额资金流动情况。机构参与度变化是重要市场信号。",
            'macro': f"报告期内共收录宏观经济新闻 **{count}** 条，包括货币政策、通胀数据及全球经济形势对加密市场的影响。宏观环境是资产定价的重要背景。",
            'technology': f"报告期内共收录技术发展新闻 **{count}** 条，涉及协议升级、技术创新及基础设施建设进展。技术进步是行业长期价值的基础。",
            'market': f"报告期内共收录市场行情新闻 **{count}** 条，反映价格波动、交易量变化及市场情绪。短期价格走势需结合基本面综合分析。",
            'security': f"报告期内共收录安全事件新闻 **{count}** 条，包括黑客攻击、漏洞披露及资金安全事件。安全风险管理是投资的重要考量。",
            'adoption': f"报告期内共收录采用进展新闻 **{count}** 条，涵盖支付应用、企业采用及用户增长情况。实际采用是价值实现的关键驱动力。"
        }
        
        return descriptions.get(category, f"报告期内该主题相关新闻共 **{count}** 条。")
    
    def _generate_timeline(self, items: List[Dict]) -> str:
        """生成时间线"""
        lines = []
        
        # 按日期分组
        by_date = defaultdict(list)
        for item in items:
            date = item.get('published_at', '')[:10]
            if date:
                by_date[date].append(item)
        
        # 生成时间线
        for date in sorted(by_date.keys(), reverse=True):
            date_items = by_date[date]
            lines.append(f"### {date}")
            lines.append("")
            
            # 只显示高影响力事件
            important = sorted(
                date_items,
                key=lambda x: x.get('impact_score', 5),
                reverse=True
            )[:5]
            
            for item in important:
                cat = item.get('primary_category', 'market')
                cat_name = self.CATEGORY_NAMES.get(cat, cat)
                title = item.get('title', '')
                source = item.get('source', 'Unknown')
                
                lines.append(f"- **[{cat_name}]** {title} (*{source}*)")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_data_source_note(self) -> str:
        """生成数据来源说明"""
        return """本报告数据来源为公开渠道收集的加密市场新闻资讯，包括：

- **RSS订阅源**: CoinDesk、Cointelegraph、Decrypt、TheBlock
- **数据范围**: 公开可获取的市场资讯
- **更新频率**: 每日自动采集
- **数据质量**: 经过去重和标准化处理

**免责声明**:
1. 本报告仅供参考，不构成投资建议
2. 数据来源为公开信息，不保证完整性和准确性
3. 市场有风险，投资需谨慎

*本报告由 Crypto News Archive 系统自动生成*
"""


# ============================================================================
# 导出管理器
# ============================================================================

class ExportManager:
    """导出管理器 - 统一管理各种导出格式"""
    
    def __init__(self, query_engine: QueryEngine):
        self.query = query_engine
        self.exporters = {
            'json': JSONExporter(query_engine),
            'markdown': MarkdownExporter(query_engine)
        }
    
    def export(
        self,
        start: str,
        end: str,
        output_path: Path,
        format: str = 'json',
        **kwargs
    ) -> Path:
        """
        导出数据
        
        Args:
            start: 开始日期 (YYYY-MM-DD)
            end: 结束日期 (YYYY-MM-DD)
            output_path: 输出文件路径
            format: 导出格式 ('json' 或 'markdown')
            **kwargs: 传递给具体导出器的参数
        
        Returns:
            输出文件路径
        """
        if format not in self.exporters:
            raise ValueError(f"不支持的格式: {format}。支持的格式: {list(self.exporters.keys())}")
        
        exporter = self.exporters[format]
        return exporter.export(start, end, output_path, **kwargs)
    
    def export_both(self, start: str, end: str, output_dir: Path, **kwargs) -> Dict[str, Path]:
        """
        同时导出JSON和Markdown格式
        
        Returns:
            {'json': json_path, 'markdown': md_path}
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        base_name = f"news_report_{start}_to_{end}"
        
        result = {}
        
        # 导出JSON
        json_path = output_dir / f"{base_name}.json"
        result['json'] = self.export(start, end, json_path, 'json', **kwargs)
        
        # 导出Markdown
        md_path = output_dir / f"{base_name}.md"
        result['markdown'] = self.export(start, end, md_path, 'markdown', **kwargs)
        
        return result


# ============================================================================
# 便捷函数
# ============================================================================

def export_to_json(
    db_path: Path,
    start: str,
    end: str,
    output_path: Path,
    **kwargs
) -> Path:
    """便捷函数：导出为JSON"""
    query = create_query_engine(db_path)
    exporter = JSONExporter(query)
    return exporter.export(start, end, output_path, **kwargs)


def export_to_markdown(
    db_path: Path,
    start: str,
    end: str,
    output_path: Path,
    **kwargs
) -> Path:
    """便捷函数：导出为Markdown"""
    query = create_query_engine(db_path)
    exporter = MarkdownExporter(query)
    return exporter.export(start, end, output_path, **kwargs)


# ============================================================================
# 命令行接口
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="新闻导出工具")
    parser.add_argument("--db", default="./archive/news_archive.db", help="数据库路径")
    parser.add_argument("--start", required=True, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--format", choices=['json', 'markdown', 'both'], default='json',
                        help="导出格式")
    parser.add_argument("--output", required=True, help="输出文件路径")
    parser.add_argument("--title", help="报告标题（仅Markdown）")
    
    args = parser.parse_args()
    
    # 检查数据库
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"错误: 数据库不存在 {db_path}")
        exit(1)
    
    # 创建导出管理器
    query = create_query_engine(db_path)
    manager = ExportManager(query)
    
    # 执行导出
    try:
        if args.format == 'both':
            result = manager.export_both(args.start, args.end, Path(args.output))
            print(f"\n导出成功:")
            print(f"  JSON: {result['json']}")
            print(f"  Markdown: {result['markdown']}")
        else:
            output_path = manager.export(
                args.start,
                args.end,
                Path(args.output),
                args.format,
                report_title=args.title
            )
            print(f"\n导出成功: {output_path}")
    except Exception as e:
        print(f"导出失败: {e}")
        exit(1)
