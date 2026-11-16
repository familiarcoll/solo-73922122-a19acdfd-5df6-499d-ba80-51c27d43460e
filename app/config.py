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

# 数据目录（控制库、保留集索引、任务产物）由 DATA_DIR 环境变量指定，
# 默认 /data；在 manager 与 main 中运行时读取，避免导入期固化。
DEFAULT_DATA_DIR = "/data"
