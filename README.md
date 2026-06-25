# 搜狗新闻大数据实战

一个可直接运行的教学演示项目：

```text
日志模拟/回放 -> Kafka -> Flink -> MySQL -> Streamlit 可视化大屏
```

不需要手动安装 Java、Maven、Flink、Kafka、MySQL 或 Python。安装 Docker Desktop 后，使用脚本即可启动整套链路。

## 功能效果

- 新闻实时点击榜与趋势大屏
- Kafka/Flink 实时写入 MySQL
- 新闻标题聚合、时段窗口统计
- 系统状态页，辅助判断数据链路是否正在写入
- 增强可视化、数据预测、异常预警、热点聚类、关联规则
- 数据库为空时自动补充演示数据
- 前端提供“补充演示数据”和“清空并重置演示数据”按钮
- 可选大模型洞察模块，API Key 只放在本地 `.env`

## 原始流程与 Docker 快速流程

课程或虚拟机环境中的原始流程通常是：

```text
sougou.sh
  -> hadoop1 Flume taildir 采集
  -> hadoop2/hadoop3 Flume Avro 聚合
  -> Kafka 集群
  -> IDEA/Flink 实时程序
  -> MySQL(newscount, periodcount)
  -> streamlit run app.py
```

本仓库默认提供的是 Docker 快速版：

```text
log-producer
  -> Kafka(news-kafka)
  -> Flink(KafkaFlinkMySQL)
  -> MySQL(news-mysql)
  -> Streamlit(news-dashboard)
```

两者的对应关系：

| 原始虚拟机流程 | Docker 快速版 |
| --- | --- |
| `sougou.sh` 写入日志目录 | `log-producer` 生成或回放日志 |
| hadoop1 Flume taildir 采集 | 快速版暂用 `log-producer` 简化替代 |
| hadoop2/hadoop3 Flume 聚合到 Kafka | 快速版暂用 `log-producer` 直接写 Kafka |
| IDEA 中手动启动 Flink 程序 | `flink-job` 容器自动提交 `KafkaFlinkMySQL` |
| 手动 `streamlit run app.py` | `dashboard` 容器自动启动 Streamlit |

这样设计是为了让别人能先一键跑通。如果需要完整展示 Flume 三节点采集/聚合链路，建议后续增加可选的 `docker-compose.flume.yml`，不要替换当前快速版。

## 电脑要求

1. Windows 10/11、macOS 或 Linux。
2. 安装 [Docker Desktop](https://www.docker.com/products/docker-desktop/)。
3. Docker Desktop 建议分配 8 GB 内存，至少 6 GB。
4. 首次运行需要下载约 5 GB 镜像，请保持网络稳定。

## 一键启动

### Windows

在项目目录打开 PowerShell，执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

常用参数：

```powershell
# 不重新拉取镜像，适合已经下载过镜像的情况
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1 -NoPull

# 已有 Flink 作业时默认不重复提交；如需强制重提
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1 -NoPull -RestartFlinkJob

# 数据库为空时不自动写入演示数据
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1 -NoSeedFallback
```

### macOS / Linux

```bash
chmod +x scripts/*.sh
./scripts/start.sh
```

常用参数：

```bash
./scripts/start.sh --no-pull
./scripts/start.sh --restart-flink-job
./scripts/start.sh --no-seed-fallback
```

启动完成后打开：

- 数据大屏：http://localhost:8501
- Flink 管理界面：http://localhost:8081

MySQL 默认端口：`3308`

Kafka 默认端口：`9092`

## 数据从哪里来

默认情况下，`log-producer` 会启动并等待前端控制开关；点击“启动实时生成”后才会持续写入 Kafka。

没有真实 `data/sougou.log` 时，producer 会进入动态模拟模式，新闻标题会带运行批次号，例如：

```text
人工智能-最新进展-20260625034913-0001
新能源车-最新进展-20260625034913-0002
```

这样 `newscount` 不会固定在少量标题上，重启 producer 后也会继续产生新的新闻标题。

为了便于课堂演示，producer 默认受前端按钮控制：

1. 打开 Dashboard。
2. 在左侧点击“启动实时生成”。
3. 等待几秒后刷新页面，`不同新闻标题数` 会继续增长。
4. 点击“暂停实时生成”后，producer 会停止向 Kafka 发送新日志。

按钮不会直接执行 Docker 命令，而是写入 MySQL 的 `producer_control` 表。producer 每隔几秒读取这个开关，只有开关开启时才生成数据。

标题池默认不封顶：

```env
PRODUCER_TOPIC_POOL_SIZE=0
```

如果设置为大于 0 的数字，例如 `200`，标题达到上限后会循环更新旧标题，`不同新闻标题数` 就可能不再增长。

如果要使用自己的搜狗日志，请放到：

```text
data/sougou.log
```

每行格式：

```text
时间,用户ID,新闻主题,排名,页码,来源
```

示例：

```text
12:00:01,100001,人工智能,1,1,web
```

## 前端补数

如果数据库暂时没有数据，前端不会再报 `RangeError` 或 `ZeroDivisionError`。可以使用两种方式补数：

1. 左侧栏点击“补充演示数据”。
2. 左侧栏点击“清空并重置演示数据”。

默认也开启了空库自动补数：

```env
AUTO_SEED_ON_EMPTY=true
```

如需关闭，修改 `.env`：

```env
AUTO_SEED_ON_EMPTY=false
```

## 重要指标说明

首页的“不同新闻标题数”来自 MySQL 的 `newscount` 聚合表，表示当前不同新闻标题的数量。

“新闻表累计点击量”和“时段表累计点击量”来自 Flink 写入 MySQL 后的统计结果。

## 系统状态页

前端导航里的“系统状态”页面用于快速判断链路是否正常：

- `新闻标题行数`：来自 `newscount`。
- `时段窗口行数`：来自 `periodcount`。
- `最近时段`：用于判断时段表是否刷新。
- `新闻累计点击`、`时段累计点击`：用于观察数据是否继续增长。

该页面只读取 MySQL，不直接执行 Docker 命令。这样更安全，也更适合交给普通使用者排查。

## 停止和重置

停止服务并保留数据库数据：

```powershell
.\scripts\stop.ps1
```

macOS / Linux：

```bash
./scripts/stop.sh
```

彻底清空数据并重新开始：

```powershell
.\scripts\reset.ps1
```

也可以手动执行：

```powershell
docker compose down -v --remove-orphans
```

## 修改端口或密码

首次启动时脚本会自动把 `.env.example` 复制为 `.env`。修改 `.env` 即可调整本机配置：

```env
MYSQL_PORT=3308
FLINK_PORT=8081
STREAMLIT_PORT=8501
DB_PASSWORD=请换成自己的密码
```

`.env` 已被 Git 忽略，不会上传到 GitHub。

## Docker Hub 镜像

项目镜像：

- `shizr/sogou-news-bigdata-lab-flink:1.0.0`
- `shizr/sogou-news-bigdata-lab-dashboard:1.0.0`
- `shizr/sogou-news-bigdata-lab-producer:1.0.0`

Compose 还会拉取官方 MySQL、Kafka 和 ZooKeeper 镜像。

## 隐私保护

- 不要上传 `.env`、API Key、数据库备份和原始用户日志。
- 使用真实日志前，请删除或匿名化姓名、手机号、邮箱、IP、Cookie、设备 ID 等个人信息。
- `.env.example` 只用于本地演示，正式部署必须修改默认密码。
- 不要把 MySQL、Kafka、Flink 或 Streamlit 端口直接暴露到公网。
- 大模型 API Key 只应写在本机 `.env`，不要写进源码、README 或截图。
- 发布前请阅读 [SECURITY.md](SECURITY.md)。

## 常见问题

### 页面打不开

```powershell
docker compose ps
docker compose logs --tail=200
```

确认 Docker Desktop 正在运行，且 `8501`、`8081`、`3308`、`9092` 没有被占用。

### Flink 作业重复

启动脚本默认会检测 `KafkaFlinkMySQL` 是否已经是 `RUNNING`。如果已经运行，会跳过重复提交。

确实需要重提作业时：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1 -NoPull -RestartFlinkJob
```

### 想完全重新安装

```powershell
docker compose down -v --remove-orphans
docker compose pull
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

### 修改代码后本地构建

```powershell
docker compose build
docker compose up -d --no-build
```

普通使用者不需要本地构建。

## 技术栈

- Apache Kafka 7.6.1
- Apache Flink 1.13.6
- MySQL 8.0
- Streamlit
- Docker Compose

本项目用于学习和演示，不建议直接作为生产系统使用。
