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
@Date   : 2021/5/18 15:15
@Author : 戴军 
@Email  : 18018030656@163.com
"""
import json
from commons.common_utils import ComplexJsonEncoder, ComplexJsonDecoder
from starlette.status import HTTP_200_OK


class ErrorData(object):
    """
    数据结果
    """

    def __init__(self, status_code=HTTP_200_OK, code=0, msg='', **kwargs):
        self.status_code = status_code
        self.code = code
        self.msg = msg
        if kwargs:
            res = json.dumps(kwargs, cls=ComplexJsonEncoder)
            self.data = json.loads(res, cls=ComplexJsonDecoder)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    # def __repr__(self):
    #     return str(self.data)
    #
    # def __str__(self):
    #     return str(self.data)

    def to_dict(self):
        return dict(
            status_code=self.status_code,
            code=self.code,
            msg=self.msg
        )
