"""FastAPI 入口：上传、任务、取消、下载与演示页面。"""
from __future__ import annotations

import os
import tempfile
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import demo_data
from .config import DEFAULT_DATA_DIR, MAX_FILE_BYTES, UPLOAD_CHUNK
from .errors import ValidationError
from .manager import JobManager


def _data_dir() -> str:
    # 运行时读取，便于测试通过环境变量隔离数据目录
    return os.environ.get("DATA_DIR", DEFAULT_DATA_DIR)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 应用启动时创建管理器，便于测试通过 DATA_DIR 环境变量隔离数据目录
    global manager
    manager = JobManager(_data_dir())
    try:
        yield
    finally:
        manager.shutdown()


app = FastAPI(
    title="语料清洗服务",
    description="剔除与保留题集共享连续 8 个 Unicode 空白分词的训练记录",
    version="1.0.0",
    lifespan=lifespan,
)

# lifespan 启动后赋值
manager: JobManager | None = None

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.exception_handler(ValidationError)
async def validation_error_handler(_request, exc: ValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code, content={"error": exc.message}
    )


def _spool_upload(upload: UploadFile) -> tuple[str, int]:
    """把上传内容流式落盘到临时文件，强制 256 MiB 上限，内存只保留 1 MiB 块。"""
    fd, staged = tempfile.mkstemp(prefix="upload-", suffix=".jsonl", dir=_data_dir())
    total = 0
    try:
        with os.fdopen(fd, "wb") as out:
            while True:
                chunk = upload.file.read(UPLOAD_CHUNK)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_FILE_BYTES:
                    raise ValidationError(
                        f"文件超过 {MAX_FILE_BYTES} 字节（256 MiB）上限"
                    )
                out.write(chunk)
    except Exception:
        try:
            os.remove(staged)
        except FileNotFoundError:
            pass
        raise
    if total == 0:
        os.remove(staged)
        raise ValidationError("上传文件为空")
    return staged, total


# ---- 保留题集 ---------------------------------------------------------------
@app.get("/api/retention")
def get_retention():
    info = manager.get_retention()
    return {"retention": info}


@app.post("/api/retention")
def upload_retention(file: UploadFile = File(...)):
    staged, size = _spool_upload(file)
    try:
        info = manager.replace_retention(staged, file.filename or "retention.jsonl", size)
    except ValidationError:
        raise
    except ValueError as exc:
        # 保留集 id 重复等业务错误
        raise ValidationError(str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"保留集构建失败：{exc}") from exc
    return {"retention": info}


# ---- 清洗任务 ---------------------------------------------------------------
@app.post("/api/jobs")
def create_job(file: UploadFile = File(...)):
    staged, size = _spool_upload(file)
    try:
        info = manager.create_job(staged, file.filename or "training.jsonl", size)
    except Exception:
        try:
            os.remove(staged)
        except FileNotFoundError:
            pass
        raise
    return {"job": info}


@app.get("/api/jobs")
def list_jobs():
    return {"jobs": manager.list_jobs()}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    info = manager.get_job(job_id)
    if info is None:
        raise ValidationError("任务不存在", 404)
    return {"job": info}


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    info = manager.cancel_job(job_id)
    return {"job": info, "cancel_requested": True}


@app.get("/api/jobs/{job_id}/download/cleaned")
def download_cleaned(job_id: str):
    path, filename = manager.download_cleaned(job_id)
    return FileResponse(
        path,
        media_type="application/x-ndjson",
        filename=filename,
    )


@app.get("/api/jobs/{job_id}/download/removals")
def download_removals(job_id: str):
    path, filename = manager.download_report(job_id)
    return FileResponse(
        path,
        media_type="application/x-ndjson",
        filename=filename,
    )


# ---- 演示一键数据 -----------------------------------------------------------
@app.post("/api/demo/retention")
def demo_retention():
    staged, name, size = demo_data.demo_retention()
    info = manager.replace_retention(staged, name, size)
    return {"retention": info}


@app.post("/api/demo/training")
def demo_training():
    staged, name, size = demo_data.demo_training()
    info = manager.create_job(staged, name, size)
    return {"job": info}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
