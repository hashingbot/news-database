# News Database Project

## Project Metadata

- **project-id**: news-database
- **project-name**: 新闻数据库
- **created-at**: 2026-03-04
- **description**: 本地加密市场新闻数据库，每日自动拉取并分类存储
- **tags**: crypto, news, database, automation

## Structure

```
news-database/
├── PROJECT.md                    # 项目元数据
├── README.md                     # 项目说明
├── background/                   # 背景文件
├── deliverables/                 # 交付物
├── workspace/                    # 工作空间
│   ├── crypto-news-archive/      # Skill副本（用于迭代更新）
│   │   ├── scripts/
│   │   ├── config/
│   │   ├── archive/
│   │   └── SKILL.md
│   ├── daily_fetch.py            # 每日拉取脚本
│   └── query_tool.py             # 查询工具
└── logs/                         # 运行日志
```

## Cron Job

- **Schedule**: 每天 10:00 AM
- **Command**: `0 10 * * * cd /root/.openclaw/workspace/projects/news-database && /usr/bin/python3 workspace/daily_fetch.py >> logs/cron.log 2>&1`

## Database

- **Type**: SQLite
- **Location**: `workspace/crypto-news-archive/archive/news_archive.db`
- **Features**: FTS5全文搜索，自动去重，Schema版本控制

## Status

- [x] Project created
- [x] Skill copied
- [x] Daily fetch script
- [x] Query tool
- [x] Cron job configured
- [x] Git initialized
