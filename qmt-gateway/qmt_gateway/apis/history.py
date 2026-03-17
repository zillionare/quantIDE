"""历史分钟线下载 API."""

from loguru import logger
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, JSONResponse

from qmt_gateway.services.history_download_service import history_download_service


def _login_required(request) -> dict:
    """检查用户登录状态."""
    user = request.scope.get("session", {}).get("user")
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    return user


def register_routes(app):
    """注册历史分钟线下载路由."""

    @app.post("/api/history/minutes/jobs")
    def create_minutes_job(
        request,
        trade_date: str,
        period: str = "1m",
        universe: str = "ashare",
    ):
        _login_required(request)
        try:
            job = history_download_service.create_job(
                trade_date=trade_date,
                period=period,
                universe=universe,
            )
            return JSONResponse({"code": 0, "message": "任务已创建", "data": job})
        except ValueError as exc:
            return JSONResponse({"code": 1, "message": str(exc)}, status_code=400)
        except Exception as exc:
            logger.error(f"创建历史分钟下载任务失败: {exc}")
            return JSONResponse({"code": 1, "message": str(exc)}, status_code=500)

    @app.get("/api/history/minutes/jobs/{job_id}")
    def get_minutes_job(request, job_id: str):
        _login_required(request)
        try:
            job = history_download_service.get_job(job_id)
            return JSONResponse({"code": 0, "message": "ok", "data": job})
        except KeyError:
            return JSONResponse({"code": 1, "message": "任务不存在"}, status_code=404)
        except Exception as exc:
            logger.error(f"查询历史分钟下载任务失败: {exc}")
            return JSONResponse({"code": 1, "message": str(exc)}, status_code=500)

    @app.get("/api/history/minutes/jobs/{job_id}/file")
    def download_minutes_file(request, job_id: str):
        _login_required(request)
        try:
            job = history_download_service.get_job(job_id)
            file_path = history_download_service.get_download_path(job_id)
            return FileResponse(
                path=str(file_path),
                filename=job["file_name"],
                media_type="application/octet-stream",
            )
        except KeyError:
            return JSONResponse({"code": 1, "message": "任务不存在"}, status_code=404)
        except RuntimeError:
            return JSONResponse({"code": 1, "message": "任务尚未完成"}, status_code=409)
        except Exception as exc:
            logger.error(f"下载历史分钟文件失败: {exc}")
            return JSONResponse({"code": 1, "message": str(exc)}, status_code=500)
