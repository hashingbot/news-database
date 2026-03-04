---
name: crypto-news-archive
description: "无需API密钥的加密市场新闻存档与获取系统。通过RSS订阅和网页爬虫获取公开新闻数据，支持指定时间段查询。专为基金月报、市场分析等需要历史新闻支持的场景设计。"
---

# Crypto News Archive Skill

无需API密钥的加密市场新闻存档与获取系统。

## ✅ Phase 1 已完成 (2026-03-04)
核心爬虫功能已全部实现并测试通过。

## ✅ Phase 2 已完成 (2026-03-04)
数据库优化、查询系统和数据导出功能已实现。

---

## 核心特性

### ✅ 无需API密钥
- 通过RSS订阅源获取新闻
- 网页爬虫抓取公开数据
- 零成本部署和运行

### 💾 SQLite数据库 (Phase 2)
- Schema版本控制（自动迁移）
- 多索引优化查询性能
- FTS5全文搜索支持
- 数据完整性检查
- WAL模式提升并发性能

### 🎯 数据去重 (Phase 2)
- URL哈希去重（数据库UNIQUE约束）
- 内容相似度去重（Levenshtein算法，90%阈值）
- 自动清理重复数据

### 🔍 灵活查询 (Phase 2)
- 按日期范围查询
- 按分类筛选（7大维度）
- 关键词全文搜索
- 每日摘要统计
- 情绪分析分布

### 📊 数据导出 (Phase 2)
- JSON结构化导出
- Markdown分析报告（基金月报专用）
- 支持自定义时间范围
- 统计信息自动生成

### ⚡ 性能优化 (Phase 2)
- LRU缓存机制（TTL过期）
- 查询响应 < 1ms
- 线程安全设计
- 并发查询支持

### 📅 时间段查询
- 支持任意历史时间段查询（如：2026-01-01 到 2026-01-30）
- 自动处理跨月、跨年的数据整合
- 支持多粒度输出（日度、周度、月度汇总）

### 📡 多源聚合

| 数据源 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| CoinDesk | RSS | ⚠️ 部分可用 | DNS问题待修复 |
| Cointelegraph | RSS | ✅ 正常 | 主要数据源 |
| Decrypt | RSS | ✅ 正常 | 深度报道 |
| TheBlock | RSS | ⚠️ 部分可用 | XML格式问题 |
| CoinGecko | 爬虫 | ⛔ 禁止 | robots.txt限制 |
| CryptoPanic | 爬虫 | ⛔ 禁止 | robots.txt限制 |

### 🛡️ 反爬虫策略
- ✅ User-Agent轮换池 (5种主流UA)
- ✅ 请求间隔控制 (默认2秒，按域名隔离)
- ✅ 错误重试机制 (指数退避，最多3次)
- ✅ robots.txt检查与遵守
- ✅ 速率限制处理 (429自动等待)

### 🧹 数据标准化
- ✅ 统一日期格式 (支持15+种格式)
- ✅ 提取标题、摘要、URL
- ✅ 自动分类 (7个维度)
- ✅ URL+标题双重去重
- ✅ SQLite本地存档

---

## 快速开始

### 安装依赖

```bash
cd /root/.openclaw/workspace/skills/crypto-news-archive
python3 -m venv .venv
source .venv/bin/activate
pip install beautifulsoup4 feedparser requests
```

### 使用场景

#### 场景1：获取最新新闻
```bash
python3 scripts/crawler.py --days 7
```

#### 场景2：查询本地存档
```bash
# 按日期范围查询
python3 scripts/query.py --start 2026-01-01 --end 2026-01-31

# 按分类查询
python3 scripts/query.py --start 2026-01-01 --end 2026-01-31 --category regulation

# 关键词搜索
python3 scripts/query.py --start 2026-01-01 --end 2026-01-31 --keyword "bitcoin etf"

# 显示统计
python3 scripts/query.py --start 2026-01-01 --end 2026-01-31 --stats
```

#### 场景3：导出报告
```bash
# 导出JSON
python3 scripts/export.py --start 2026-01-01 --end 2026-01-31 --format json --output report.json

# 导出Markdown（基金月报）
python3 scripts/export.py --start 2026-01-01 --end 2026-01-31 --format markdown --output report.md --title "1月市场分析"

# 同时导出两种格式
python3 scripts/export.py --start 2026-01-01 --end 2026-01-31 --format both --output ./reports
```

#### 场景4：数据维护
```bash
# 清理重复数据
python3 scripts/crawler.py --clean-duplicates

# 检查数据完整性
python3 scripts/crawler.py --check-integrity
```

#### 场景5：基金月报市场展望
```bash
python3 scripts/fetch_archive.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --output reports/2026-01-news.json
```

---

## 项目结构

```
crypto-news-archive/
├── scripts/
│   ├── crawler.py              # 核心爬虫 (Phase 1/2)
│   ├── database.py             # 数据库层 (Phase 2) ⭐新增
│   ├── query.py                # 查询工具 (Phase 2) ⭐新增
│   ├── export.py               # 导出工具 (Phase 2) ⭐新增
│   ├── test_performance.py     # 性能测试 (Phase 2) ⭐新增
│   ├── fetch_archive.py        # 存档获取脚本
│   ├── daily_archive.py        # 每日自动存档
│   └── fund_report_news_integration.py  # 基金报告集成
├── config/
│   └── config.json             # 配置文件
├── archive/
│   └── news_archive.db         # SQLite数据库
├── docs/
│   ├── phase1_test_report.md   # Phase 1 测试报告
│   ├── phase1_technical_doc.md # Phase 1 技术文档
│   ├── phase2_test_report.md   # Phase 2 测试报告 ⭐新增
│   └── phase2_technical_doc.md # Phase 2 技术文档 ⭐新增
└── SKILL.md                    # 本文件
```

---

## 技术实现

### Phase 2 核心模块

| 模块 | 功能 | 文件 |
|------|------|------|
| DatabaseManager | SQLite数据库管理 | database.py |
| SchemaMigration | 数据库版本控制 | database.py |
| DeduplicationEngine | 数据去重引擎 | database.py |
| DataIntegrityChecker | 数据完整性检查 | database.py |
| QueryEngine | 查询引擎 | query.py |
| LRUCache | LRU缓存实现 | query.py |
| JSONExporter | JSON导出器 | export.py |
| MarkdownExporter | Markdown导出器 | export.py |

### Phase 1 核心模块

| 模块 | 功能 | 文件 |
|------|------|------|
| CryptoNewsCrawler | 主爬虫类 | crawler.py |
| RequestManager | 请求管理(反爬虫) | crawler.py |
| RobotsChecker | robots.txt检查 | crawler.py |
| DateNormalizer | 日期标准化 | crawler.py |
| NewsCategorizer | 新闻分类 | crawler.py |

### 分类维度

- **regulation** 📋 - 监管动态 (SEC, ETF, 法规)
- **institutional** 🏦 - 机构动向 (BlackRock, 基金, 银行)
- **macro** 🌍 - 宏观市场 (Fed, 通胀, 利率)
- **technology** 💻 - 技术发展 (升级, Layer2, DeFi)
- **market** 📈 - 市场行情 (价格, 涨跌, 交易)
- **security** 🔒 - 安全事件 (黑客, 漏洞, 诈骗)
- **adoption** 🚀 - 采用进展 (支付, 合作, 集成)

---

## 性能指标 (Phase 2)

| 操作 | 平均响应 | 目标 | 状态 |
|------|----------|------|------|
| 日期范围查询 | 0.35ms | <100ms | ✅ |
| 分类查询 | 0.09ms | <100ms | ✅ |
| 关键词搜索 | 0.05ms | <200ms | ✅ |
| 统计查询 | 0.47ms | <500ms | ✅ |
| 并发查询 | 1.87ms | 无错误 | ✅ |

---

## 输出格式

### JSON结构化数据
```json
{
  "meta": {
    "export_type": "crypto_news",
    "version": "2.0",
    "generated_at": "2026-03-04T01:14:52",
    "period": {"start": "2026-01-01", "end": "2026-01-31"},
    "total_items": 156
  },
  "items": [...],
  "statistics": {...}
}
```

### Markdown分析报告
- 执行摘要
- 市场概况（分类统计、情绪分布）
- 主题分析（按分类生成描述）
- 时间线视图
- 数据来源说明

---

## 长期稳定机制

### 1. 多层数据源备份
- RSS订阅源（主要）
- 网页爬虫（备用）
- 本地SQLite数据库缓存

### 2. 反爬虫策略
- respectful爬虫（遵守robots.txt）
- 请求间隔控制（默认2秒）
- User-Agent轮换
- 错误重试机制

### 3. 质量保障
- 自动去重（URL + 内容相似度）
- 数据完整性检查
- Schema版本控制
- 缺失数据标记

### 4. 持续迭代
- 每月评估数据源有效性
- 自动记录爬取成功率
- 支持新增RSS源配置

---

## 注意事项

1. **网络依赖**：需要稳定的网络连接访问RSS源和目标网站
2. **爬取限制**：尊重目标网站的robots.txt，控制请求频率
3. **数据完整性**：依赖公开数据源，部分历史数据可能缺失
4. **版权声明**：仅获取公开数据，使用时遵守各平台使用条款

---

## 故障排除

### 无法获取数据
- 检查网络连接
- 确认目标网站可访问
- 检查是否被封IP（降低爬取频率）

### 数据重复
- 系统会自动去重
- 手动清理：`python3 scripts/crawler.py --clean-duplicates`

### 分类不准确
- 基于关键词规则分类
- 可在 `crawler.py` 中调整 `CATEGORIES` 配置

### 数据库问题
- 检查数据完整性：`python3 scripts/crawler.py --check-integrity`
- 重建数据库：删除 `archive/news_archive.db` 后重新获取数据

---

## 文档

### Phase 1
- [Phase 1 测试报告](docs/phase1_test_report.md)
- [Phase 1 技术文档](docs/phase1_technical_doc.md)

### Phase 2
- [Phase 2 测试报告](docs/phase2_test_report.md)
- [Phase 2 技术文档](docs/phase2_technical_doc.md)

---

## Roadmap

### Phase 1 ✅ 已完成
- 核心爬虫实现
- RSS订阅解析
- 反爬虫策略
- 数据标准化

### Phase 2 ✅ 已完成
- SQLite数据库优化
- Schema版本控制
- 数据去重（URL + 相似度）
- 查询系统（日期、分类、关键词）
- 数据导出（JSON/Markdown）
- LRU缓存机制
- 性能测试

### Phase 3 计划
- 更多数据源 (Twitter/X, Telegram)
- 情绪分析增强
- Web UI 查询界面
- 定时任务调度

---

*最后更新: 2026-03-04*
