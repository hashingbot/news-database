#!/usr/bin/env python3
"""
基金月报新闻集成脚本 - Phase 3 实现
为基金月报Section 5提供新闻数据支持

Phase 3 特性：
- 使用Phase 2的查询引擎
- 专业金融分析文字生成
- 概括性描述（无直接标题引用）
- 与 content.md 集成

生成时间: 2026-03-04
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# 添加skill路径
sys.path.insert(0, str(Path(__file__).parent))

from database import create_database
from query import QueryEngine, create_query_engine
from export import ExportManager, create_query_engine


class FundReportNewsIntegration:
    """基金月报新闻集成器 - Phase 3"""
    
    # 分类中文映射
    CATEGORY_NAMES = {
        'regulation': '监管政策',
        'institutional': '机构动向',
        'macro': '宏观环境',
        'technology': '技术发展',
        'market': '市场行情',
        'security': '安全事件',
        'adoption': '采用进展'
    }
    
    # 情绪中文映射
    SENTIMENT_NAMES = {
        'positive': '积极',
        'neutral': '中性',
        'negative': '谨慎'
    }
    
    def __init__(self, db_path: Path = None):
        """
        Args:
            db_path: 数据库路径，默认为skill目录下的archive/news_archive.db
        """
        if db_path is None:
            db_path = Path(__file__).parent.parent / "archive" / "news_archive.db"
        
        self.db_path = db_path
        self.query_engine = create_query_engine(db_path)
        self.export_manager = ExportManager(self.query_engine)
    
    def fetch_monthly_news(self, year: int, month: int) -> dict:
        """
        获取指定月份的新闻数据，用于基金月报
        
        Args:
            year: 年份
            month: 月份 (1-12)
        
        Returns:
            结构化的新闻数据，包含摘要和详细列表
        """
        # 计算日期范围
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1}-01-01"
        else:
            end_date = f"{year}-{month+1:02d}-01"
        
        print(f"[基金月报] 获取 {year}-{month:02d} 新闻数据...")
        print(f"           时间范围: {start_date} 至 {end_date}")
        
        # 获取数据
        items = self.query_engine.query_by_date_range(start_date, end_date)
        
        if not items:
            print(f"[警告] 未找到 {year}-{month:02d} 的新闻数据")
            print(f"       请先运行爬虫获取数据")
        
        # 获取统计信息
        stats = self.query_engine.get_statistics(start_date, end_date)
        
        # 生成结构化报告
        report = {
            'meta': {
                'year': year,
                'month': month,
                'period': {'start': start_date, 'end': end_date},
                'generated_at': datetime.now().isoformat(),
                'data_source': 'Crypto News Archive',
                'version': '2.0'
            },
            'summary': {
                'total_news': len(items),
                'by_category': stats.get('categories', {}),
                'by_source': stats.get('sources', {}),
                'sentiment_distribution': stats.get('sentiment_distribution', {}),
                'average_impact_score': stats.get('average_impact_score', 0)
            },
            'daily_distribution': stats.get('daily_distribution', {}),
            'items': items,
            'analysis': self._generate_analysis(items, stats)
        }
        
        return report
    
    def _generate_analysis(self, items: list, stats: dict) -> dict:
        """
        生成专业金融分析
        
        Returns:
            包含各类分析结果的字典
        """
        categories = stats.get('categories', {})
        sentiments = stats.get('sentiment_distribution', {})
        
        # 主导主题
        dominant_themes = sorted(
            categories.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]
        
        # 市场情绪判断
        total_sentiment = sum(sentiments.values()) if sentiments else 0
        sentiment_trend = 'neutral'
        if total_sentiment > 0:
            pos_ratio = sentiments.get('positive', 0) / total_sentiment
            neg_ratio = sentiments.get('negative', 0) / total_sentiment
            if pos_ratio > 0.4:
                sentiment_trend = 'positive'
            elif neg_ratio > 0.3:
                sentiment_trend = 'negative'
        
        # 生成各类分析文字
        return {
            'dominant_themes': dominant_themes,
            'sentiment_trend': sentiment_trend,
            'regulation_analysis': self._analyze_regulation(items, categories.get('regulation', 0)),
            'institutional_analysis': self._analyze_institutional(items, categories.get('institutional', 0)),
            'macro_analysis': self._analyze_macro(items, categories.get('macro', 0)),
            'technology_analysis': self._analyze_technology(items, categories.get('technology', 0)),
            'market_outlook': self._generate_market_outlook(items, stats)
        }
    
    def _analyze_regulation(self, items: list, count: int) -> str:
        """生成监管动态分析（概括性描述）"""
        if count == 0:
            return "报告期内未监测到重大监管政策变化。"
        
        # 获取监管相关新闻
        regulation_items = [item for item in items if item.get('primary_category') == 'regulation']
        
        # 按情绪分类
        positive = sum(1 for i in regulation_items if i.get('sentiment') == 'positive')
        negative = sum(1 for i in regulation_items if i.get('sentiment') == 'negative')
        
        if positive > negative:
            tone = "整体偏向积极"
        elif negative > positive:
            tone = "存在一定不确定性"
        else:
            tone = "保持中性"
        
        return (
            f"报告期内共监测到 **{count}** 条监管相关动态，{tone}。"
            f"监管环境的变化是影响加密资产价格的重要外部因素，"
            f"建议持续关注政策走向及其对市场的潜在影响。"
        )
    
    def _analyze_institutional(self, items: list, count: int) -> str:
        """生成机构动向分析"""
        if count == 0:
            return "报告期内机构端未见显著动作。"
        
        institutional_items = [item for item in items if item.get('primary_category') == 'institutional']
        
        # 统计高影响力事件
        high_impact = sum(1 for i in institutional_items if i.get('impact_score', 5) >= 7)
        
        if high_impact > 0:
            emphasis = f"其中包含 **{high_impact}** 项高影响力事件，"
        else:
            emphasis = ""
        
        return (
            f"报告期内共监测到 **{count}** 条机构动向相关资讯，{emphasis}"
            f"反映传统金融机构和大型投资机构对加密资产的关注度。"
            f"机构资金的流入流出是判断市场长期趋势的重要参考指标。"
        )
    
    def _analyze_macro(self, items: list, count: int) -> str:
        """生成宏观环境分析"""
        if count == 0:
            return "报告期内宏观因素对加密市场影响相对平稳。"
        
        return (
            f"报告期内共监测到 **{count}** 条宏观市场相关资讯，"
            f"涉及货币政策、通胀数据及全球经济形势。"
            f"宏观环境是加密资产定价的重要背景因素，"
            f"利率变化和流动性状况对风险资产表现具有显著影响。"
        )
    
    def _analyze_technology(self, items: list, count: int) -> str:
        """生成技术发展分析"""
        if count == 0:
            return "报告期内技术发展相对平稳。"
        
        return (
            f"报告期内共监测到 **{count}** 条技术发展相关资讯，"
            f"涵盖协议升级、基础设施建设和技术创新等方面。"
            f"技术进步是区块链行业长期价值创造的基础，"
            f"关注技术路线图的执行进展有助于评估项目基本面。"
        )
    
    def _generate_market_outlook(self, items: list, stats: dict) -> str:
        """生成市场展望段落"""
        sentiments = stats.get('sentiment_distribution', {})
        total = sum(sentiments.values()) if sentiments else 0
        
        if total == 0:
            return "基于现有信息，市场展望需结合更多数据综合判断。"
        
        pos_ratio = sentiments.get('positive', 0) / total
        neg_ratio = sentiments.get('negative', 0) / total
        
        if pos_ratio > 0.45:
            outlook = "偏向积极"
            factors = "正面资讯占据主导，市场情绪整体向好"
        elif neg_ratio > 0.35:
            outlook = "保持谨慎"
            factors = "负面因素需要关注，建议密切跟踪风险演变"
        else:
            outlook = "维持中性"
            factors = "多空因素交织，市场方向尚不明确"
        
        return (
            f"综合上述分析，对未来市场展望持**{outlook}**态度。{factors}。"
            f"建议投资者关注监管政策进展、机构资金流向及宏观环境变化，"
            f"根据风险承受能力审慎配置资产。"
        )
    
    def generate_section5_content(self, report: dict) -> str:
        """
        生成Section 5的市场展望content.md内容
        
        与基金月报项目集成，直接生成可用的Markdown文件
        """
        meta = report['meta']
        summary = report['summary']
        analysis = report['analysis']
        
        year = meta['year']
        month = meta['month']
        
        lines = []
        lines.append("# 市场展望")
        lines.append("")
        lines.append(f"> 数据期间: {year}年{month}月  ")
        lines.append(f"> 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}  ")
        lines.append(f"> 数据来源: Crypto News Archive (无需API密钥)")
        lines.append("")
        
        # 执行摘要
        lines.append("## 一、市场概况")
        lines.append("")
        lines.append(self._generate_executive_summary(summary, analysis))
        lines.append("")
        
        # 主题分析
        lines.append("## 二、主题分析")
        lines.append("")
        
        lines.append("### 1. 监管动态")
        lines.append(analysis['regulation_analysis'])
        lines.append("")
        
        lines.append("### 2. 机构动向")
        lines.append(analysis['institutional_analysis'])
        lines.append("")
        
        lines.append("### 3. 宏观环境")
        lines.append(analysis['macro_analysis'])
        lines.append("")
        
        lines.append("### 4. 技术发展")
        lines.append(analysis['technology_analysis'])
        lines.append("")
        
        # 市场展望
        lines.append("## 三、市场展望")
        lines.append("")
        lines.append(analysis['market_outlook'])
        lines.append("")
        
        # 风险提示
        lines.append("## 四、风险提示")
        lines.append("")
        lines.append(
            "1. **监管风险**: 各国对加密资产的监管政策存在不确定性，"
            "政策变化可能对市场产生重大影响。"
        )
        lines.append("")
        lines.append(
            "2. **市场风险**: 加密资产价格波动剧烈，"
            "投资者应充分了解风险并审慎决策。"
        )
        lines.append("")
        lines.append(
            "3. **数据局限性**: 本分析基于公开可获取的市场资讯，"
            "不保证信息的完整性和准确性。"
        )
        lines.append("")
        
        # 数据来源说明
        lines.append("---")
        lines.append("")
        lines.append("**数据来源说明**:")
        lines.append("")
        lines.append("- 本报告数据来源于公开渠道收集的加密市场新闻资讯")
        lines.append("- 包括RSS订阅源（CoinDesk、Cointelegraph、Decrypt、TheBlock）")
        lines.append("- 经过去重和标准化处理，仅供参考")
        lines.append("")
        lines.append(f"*本报告由 Crypto News Archive 系统自动生成*")
        
        return "\n".join(lines)
    
    def _generate_executive_summary(self, summary: dict, analysis: dict) -> str:
        """生成执行摘要"""
        total = summary['total_news']
        categories = summary['by_category']
        
        # 主要主题
        top_cats = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:3]
        themes_text = "、".join([
            f"{self.CATEGORY_NAMES.get(c, c)}({n}条)" 
            for c, n in top_cats
        ])
        
        # 情绪
        sentiment = self.SENTIMENT_NAMES.get(analysis['sentiment_trend'], '中性')
        
        return (
            f"{summary.get('month', '本')}月共监测到加密市场相关新闻 **{total}** 条，"
            f"主要关注主题包括：{themes_text}。"
            f"基于情绪分析，整体市场情绪呈**{sentiment}**态度。"
            f"平均影响力评分为 **{summary.get('average_impact_score', 0):.1f}**/10，"
            f"反映市场资讯整体质量{'较高' if summary.get('average_impact_score', 0) >= 6 else '一般'}。"
        )
    
    def save_to_project(self, report: dict, section5_path: Path):
        """
        保存到基金月报项目 Section 5
        
        Args:
            report: 生成的报告数据
            section5_path: Section 5 目录路径
        """
        section5_path = Path(section5_path)
        section5_path.mkdir(parents=True, exist_ok=True)
        
        year = report['meta']['year']
        month = report['meta']['month']
        
        # 1. 保存JSON数据
        json_file = section5_path / "news_data.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"[保存] 数据已保存: {json_file}")
        
        # 2. 生成并保存Markdown分析
        content_md = self.generate_section5_content(report)
        content_file = section5_path / "content.md"
        with open(content_file, 'w', encoding='utf-8') as f:
            f.write(content_md)
        print(f"[保存] 分析已保存: {content_file}")
        
        # 3. 同时导出标准格式的JSON和Markdown报告
        output_dir = section5_path / "reports"
        output_dir.mkdir(exist_ok=True)
        
        start_date = report['meta']['period']['start']
        end_date = report['meta']['period']['end']
        
        try:
            self.export_manager.export_both(
                start_date,
                end_date,
                output_dir,
                report_title=f"{year}年{month}月加密市场新闻分析"
            )
            print(f"[保存] 详细报告已保存: {output_dir}")
        except Exception as e:
            print(f"[警告] 导出详细报告失败: {e}")
        
        return {
            'content_md': content_file,
            'data_json': json_file,
            'reports_dir': output_dir
        }


def main():
    parser = argparse.ArgumentParser(description="基金月报新闻集成 (Phase 3)")
    parser.add_argument("--year", type=int, required=True, help="年份")
    parser.add_argument("--month", type=int, required=True, help="月份 (1-12)")
    parser.add_argument("--db-path", help="数据库路径 (默认: skill目录)")
    parser.add_argument("--section5-path", help="Section 5目录路径")
    parser.add_argument("--output-only", action="store_true", help="仅输出到控制台")
    
    args = parser.parse_args()
    
    # 初始化集成器
    db_path = Path(args.db_path) if args.db_path else None
    integrator = FundReportNewsIntegration(db_path)
    
    # 获取新闻数据
    report = integrator.fetch_monthly_news(args.year, args.month)
    
    print(f"\n[统计] 共获取 {report['summary']['total_news']} 条新闻")
    
    # 生成内容
    content_md = integrator.generate_section5_content(report)
    
    if args.output_only:
        print("\n" + "="*70)
        print(content_md)
        print("="*70)
    elif args.section5_path:
        # 保存到项目
        result = integrator.save_to_project(report, Path(args.section5_path))
        print(f"\n[完成] 已保存到:")
        print(f"  - {result['content_md']}")
        print(f"  - {result['data_json']}")
    else:
        # 默认保存到当前目录
        output_dir = Path(f"fund_report_{args.year}{args.month:02d}")
        output_dir.mkdir(exist_ok=True)
        result = integrator.save_to_project(report, output_dir)
        print(f"\n[完成] 已保存到 {output_dir}/")


if __name__ == "__main__":
    main()
