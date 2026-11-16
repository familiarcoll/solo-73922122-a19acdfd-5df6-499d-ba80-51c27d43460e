FROM python:3.12-slim

# 不把 pyc 写入容器、日志不缓冲，便于容器内排查
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 非 root 运行；数据卷归属该用户
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /data /app
USER appuser

# SQLite 数据（控制库、保留集索引、任务产物）全部落在挂载卷
VOLUME ["/data"]

EXPOSE 8000

# 单 worker：任务串行扫描，避免多进程叠加冲破 128 MiB 内存
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
