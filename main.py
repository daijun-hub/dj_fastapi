# coding:utf-8
"""                          _
_._ _..._ .-',     _.._(`))
'-. `     '  /-._.-'    ',/
    )         \            '.
   / _    _    |             \
  |  a    a    /              |
  \   .-.                     ;
   '-('' ).-'       ,'       ;
      '-;           |      .'
         \           \    /
         | 7  .__  _.-\   \
         | |  |  ``/  /`  /
        /,_|  |   /,_/   /
           /,_/      '`-'
@Date   : 2021/5/18 10:54
@Author : 戴军 
@Email  : 18018030656@163.com
"""
from typing import Optional
from fastapi.exceptions import RequestValidationError
from fastapi import FastAPI

import uvicorn
from starlette.middleware.cors import CORSMiddleware

import settings
from commons.mongo_util import MongoDBConf
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse
from commons import logging
from apps.admin import handlers as admin_handlers, validation as admin_validation
from starlette.status import HTTP_200_OK

logger = logging.get_logging()
app = FastAPI()
origins = [
    "http://127.0.0.1",
    "http://127.0.0.1:8080",
]


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}


@app.on_event('startup')
async def start_app():
    """
    APP启动触发
    :return:
    """
    # 初始化DB
    MongoDBConf().client()


@app.on_event("shutdown")
async def stop_app():
    """
    APP关闭触发
    :return:
    """
    # 关闭数据库
    MongoDBConf().close_client()


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(exc.detail, status_code=exc.status_code)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    logger.debug(exc.errors())
    ret = admin_validation.form_errors_exception_to_front(exc.errors())
    return JSONResponse(ret, status_code=HTTP_200_OK)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    uvicorn.run(app, host=settings.SERVER_HOST, port=settings.SERVER_PORT)
