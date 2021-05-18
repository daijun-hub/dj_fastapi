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
@Date   : 2021/5/18 15:39
@Author : 戴军 
@Email  : 18018030656@163.com
"""
from apps import ErrorData


####################################
# FIXME: main.py validation_exception_handler 使用后期需要调整
# 错误异常转换code给前端
####################################
def form_errors_exception_to_front(errors):
    field_code_dict = {
        'username': {
            'code': AppValidationError.UserNameEmpty,
            'msg': '用户名为空'
        },
        'password': {
            'code': AppValidationError.PwdEmpty,
            'msg': '密码为空'
        }
    }
    if errors:
        error = errors[0]
        field = error.get('loc', ['', ''])[1]
        rel = field_code_dict.get(field, {})
        ctx = error.get('ctx', {})
        if rel:
            ret = ErrorData(code=rel.get('code', 0), msg=rel.get('msg', ''))
        elif ctx:
            ret = ErrorData(code=ctx.get('code', 0), msg=ctx.get('msg', ''))
        else:
            ret = ErrorData(code=error.get('type'), msg=error.get('msg'))
        return ret.to_dict()
    else:
        return ErrorData(code=-99, msg='服务器异常').to_dict()

