"""全局配置与硬性约束。"""

# 题目约束：单文件 <= 256 MiB，单行 <= 64 KiB
MAX_FILE_BYTES = 256 * 1024 * 1024
MAX_LINE_BYTES = 64 * 1024

# 连续共享词数
NGRAM_SIZE = 8

# 流式上传 / 读取块大小
UPLOAD_CHUNK = 1024 * 1024

# 进度落库最小间隔（秒）与最小行数
PROGRESS_FLUSH_SECONDS = 0.5
PROGRESS_FLUSH_LINES = 2048

# SQLite 单连接页缓存上限（负值单位 KiB），保证容器 128 MiB 内存约束
SQLITE_CACHE_KIB = -2000

# 控制库写锁等待：外部连接占用写库时，内部写入最多等待的秒数。
# 超时后 API 写入返回“稍后重试”，后台 worker 则退避后继续重试、线程不退出。
SQLITE_BUSY_TIMEOUT = 30.0
# 进度落库属于可丢弃的最佳努力写入，使用更短的等待，避免拖慢扫描主循环
PROGRESS_DB_BUSY_TIMEOUT = 2.0
# worker 锁等待失败后的退避上限（秒），释锁后立即恢复处理
WORKER_BACKOFF_MAX = 5.0

# 数据目录（控制库、保留集索引、任务产物）由 DATA_DIR 环境变量指定，
# 默认 /data；在 manager 与 main 中运行时读取，避免导入期固化。
DEFAULT_DATA_DIR = "/data"
