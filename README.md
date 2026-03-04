# 新闻数据库 (News Database)

本地加密市场新闻数据库，每日自动拉取并分类存储。

## 项目目的

为基金月报、市场分析等场景提供历史新闻数据支持：
- 自动拉取 Crypto Market 新闻
- 按7大维度分类（监管、机构、宏观、技术、市场、安全、采用）
- 支持全文搜索和日期范围查询
- 生成统计报告和导出功能

## 使用方法

### 每日自动拉取

定时任务每天 10:00 AM 自动执行：
```bash
0 10 * * * cd /root/.openclaw/workspace/projects/news-database && /usr/bin/python3 workspace/daily_fetch.py >> logs/cron.log 2>&1
```

### 手动拉取

```bash
cd /root/.openclaw/workspace/projects/news-database
python3 workspace/daily_fetch.py
```

### 查询工具

**按日期范围查询：**
```bash
python3 workspace/query_tool.py --start 2026-01-01 --end 2026-01-31
```

**按分类查询：**
```bash
python3 workspace/query_tool.py --category regulation
python3 workspace/query_tool.py --category institutional
```

**按关键词搜索：**
```bash
python3 workspace/query_tool.py --keyword bitcoin
python3 workspace/query_tool.py --keyword "ETF approval"
```

**导出报告：**
```bash
# JSON 格式
python3 workspace/query_tool.py --export json

# Markdown 格式（基金月报专用）
python3 workspace/query_tool.py --export html
```

**显示统计：**
```bash
python3 workspace/query_tool.py --stats
```

## 目录结构

```
news-database/
├── PROJECT.md                    # 项目元数据
├── README.md                     # 项目说明
├── background/                   # 背景文件
├── deliverables/                 # 交付物
├── workspace/                    # 工作空间
│   ├── crypto-news-archive/      # Skill副本
│   │   ├── scripts/              # 爬虫脚本
│   │   ├── config/               # 配置文件
│   │   ├── archive/              # 数据库和存档
│   │   └── SKILL.md              # Skill文档
│   ├── daily_fetch.py            # 每日拉取脚本
│   └── query_tool.py             # 查询工具
└── logs/                         # 运行日志
    └── cron.log                  # 定时任务日志
```

## 定时任务说明

| 项目 | 配置 |
|------|------|
| 执行时间 | 每天 10:00 AM |
| 执行命令 | `/usr/bin/python3 workspace/daily_fetch.py` |
| 工作目录 | `/root/.openclaw/workspace/projects/news-database` |
| 日志文件 | `logs/cron.log` |

### 管理定时任务

```bash
# 查看当前crontab
crontab -l

# 编辑crontab
crontab -e

# 查看日志
tail -f logs/cron.log
```

## 查询示例

### 场景1：基金月报市场展望

获取过去一个月的所有新闻并导出报告：
```bash
# 查看统计
python3 workspace/query_tool.py --start 2026-02-01 --end 2026-02-28 --stats

# 导出完整报告
python3 workspace/query_tool.py --start 2026-02-01 --end 2026-02-28 --export html
```

### 场景2：关注监管动态

```bash
# 查询近7天的监管新闻
python3 workspace/query_tool.py --category regulation --days 7

# 搜索SEC相关
python3 workspace/query_tool.py --keyword SEC
```

### 场景3：机构动向追踪

```bash
# ETF相关新闻
python3 workspace/query_tool.py --keyword ETF --days 30

# BlackRock相关
python3 workspace/query_tool.py --keyword BlackRock
```

## 数据源

| 数据源 | 类型 | 状态 |
|--------|------|------|
| CoinDesk | RSS | ⚠️ 部分可用 |
| Cointelegraph | RSS | ✅ 正常 |
| Decrypt | RSS | ✅ 正常 |
| TheBlock | RSS | ⚠️ 部分可用 |

## 技术特性

- **无需API密钥** - 通过RSS和公开数据获取
- **SQLite数据库** - 本地存储，FTS5全文搜索
- **自动去重** - URL哈希 + 内容相似度检测
- **反爬虫策略** - User-Agent轮换，请求间隔控制
- **数据完整性** - Schema版本控制，完整性检查

## 维护

```bash
# 清理重复数据
cd workspace/crypto-news-archive
python3 scripts/crawler.py --clean-duplicates

# 检查数据完整性
python3 scripts/crawler.py --check-integrity

# 查看日志
tail -f ../../logs/cron.log
```

## License

MIT
