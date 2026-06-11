# 搜狗新闻大数据实战

一个可直接运行的教学演示项目：

`日志模拟/回放 -> Kafka -> Flink -> MySQL -> Streamlit`

不需要虚拟机，不需要逐台启动 Zookeeper、Kafka、Flume，也不需要手工执行
`streamlit run app.py`。Docker Compose 会自动完成整套流程。

## 运行效果

- Streamlit 新闻热搜分析大屏
- 新闻主题实时计数
- 时段访问趋势
- 聚类、关联规则、异常预警等分析
- 可选的大模型分析模块

## 电脑要求

1. Windows 10/11、macOS 或 Linux。
2. 安装 [Docker Desktop](https://www.docker.com/products/docker-desktop/)。
3. Docker Desktop 至少分配 6 GB 内存，推荐 8 GB。
4. 首次运行需要下载约 5 GB 镜像，请保持网络稳定。

不需要另外安装 Java、Maven、Flink、Kafka、MySQL 或 Python。

## 小白一键运行

### Windows

1. 下载本项目 ZIP 并解压，或使用 Git 克隆。
2. 启动 Docker Desktop，等待左下角显示 Docker 正常运行。
3. 在项目文件夹空白处按住 `Shift` 并点击鼠标右键，选择“在终端中打开”。
4. 执行：

```powershell
.\scripts\start.ps1
```

如果 PowerShell 不允许执行脚本，可执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

### Linux / macOS

```bash
chmod +x scripts/*.sh
./scripts/start.sh
```

启动完成后打开：

- 数据大屏：http://localhost:8501
- Flink 管理界面：http://localhost:8081

MySQL 对外端口为 `3308`，Kafka 对外端口为 `9092`。

## 第一次启动需要多久

镜像第一次下载通常需要 5 到 20 分钟，取决于网络。以后启动会直接使用本地缓存，
一般几十秒即可完成。

可用下面的命令查看启动进度：

```powershell
.\scripts\logs.ps1
```

当 `news-dashboard`、`news-mysql`、`news-kafka` 显示正常，并且 Flink 页面中
`KafkaFlinkMySQL` 状态为 `RUNNING`，说明链路已启动。

`news-flink-job` 容器显示 `Exited (0)` 是正常现象：它只负责向 Flink 提交任务，
任务提交成功后就会退出，实际任务继续在 JobManager/TaskManager 中运行。

## 数据从哪里来

默认情况下，`log-producer` 会自动生成演示新闻数据，无需手工输入。

需要使用自己的搜狗日志时，将文件命名为：

```text
data/sougou.log
```

每行必须有 6 个逗号分隔字段：

```text
时间,用户ID,新闻主题,排名,页码,来源
```

请先删除或匿名化真实姓名、手机号、邮箱、IP、Cookie、设备 ID 等个人信息。

## 停止与重置

停止服务并保留数据库数据：

```powershell
.\scripts\stop.ps1
```

删除数据库数据并重新开始：

```powershell
.\scripts\reset.ps1
```

Linux/macOS 使用对应的 `.sh` 脚本。

## 修改端口或密码

第一次运行时脚本会自动将 `.env.example` 复制为 `.env`。可以编辑 `.env`：

```dotenv
MYSQL_PORT=3308
FLINK_PORT=8081
STREAMLIT_PORT=8501
DB_PASSWORD=请换成自己的密码
```

修改后执行重置脚本。`.env` 已被 Git 忽略，不会上传到 GitHub。

如果端口被占用，请修改 `.env` 中对应端口，例如：

```dotenv
STREAMLIT_PORT=8502
FLINK_PORT=8082
```

## Docker Hub 镜像

- `shizr/sogou-news-bigdata-lab-flink:1.0.0`
- `shizr/sogou-news-bigdata-lab-dashboard:1.0.0`
- `shizr/sogou-news-bigdata-lab-producer:1.0.0`

Compose 还会拉取官方 MySQL、Kafka 和 Zookeeper 镜像。

## 隐私保护

- 不要上传 `.env`、API Key、数据库备份和原始用户日志。
- `.env.example` 中只有本地演示密码，正式部署必须修改。
- 不要将 MySQL、Kafka、Flink 或 Streamlit 端口直接暴露到公网。
- 大模型 API Key 只应写在本机 `.env`，不要写进源码。
- 发布前阅读 [SECURITY.md](SECURITY.md)。

## 常见问题

### 页面打不开

运行：

```powershell
docker compose ps
docker compose logs --tail=200
```

确认 Docker Desktop 正在运行，且 `8501`、`8081`、`3308`、`9092` 没有被占用。

### 想完全重新安装

```powershell
docker compose down -v --remove-orphans
docker compose pull
docker compose up -d --no-build
```

### 修改代码后本地构建

```powershell
docker compose up -d --build
```

普通使用者不需要执行本地构建。

## 技术栈

- Apache Kafka 7.6.1
- Apache Flink 1.13.6
- MySQL 8.0
- Streamlit
- Docker Compose

本项目用于学习和演示，不建议直接作为生产系统使用。
