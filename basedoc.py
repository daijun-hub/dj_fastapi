# -*- coding: utf-8 -*-
import json
import datetime
import settings
import pickle

from mongoengine.context_managers import switch_collection
from mongoengine import (
    DateTimeField,
    Document,
    BooleanField,
    StringField,
    IntField,
    BinaryField,
    ObjectIdField
)
from commons import logging
from typing import (
    Any,
    Dict,
)

logger = logging.get_logging()


class ObjectDict(Dict[str, Any]):
    """Makes a dictionary behave like an object, with attribute-style access.
    """

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class BaseDocument(Document):
    meta = {'abstract': True}

    @property
    def field_mappings(self):
        """
        获取字段映射关系
        :return:
        """
        return getattr(self, '__mappings')

    def get_db_field(self, clazz_field):
        """
        依据类字段名获取数据库字段
        :param clazz_field: 类字段名
        :return:
        """
        if isinstance(clazz_field, str):
            if clazz_field == 'id':
                return '_id'
            ft = self.field_mappings.get(clazz_field)
            if ft and ft.db_field:
                return ft.db_field
        return clazz_field

    def map_filter_2_field(self, filtered):
        """
        映射查询字段为数据库字段
        :param filtered:
        :return:
        """
        if isinstance(filtered, dict):
            rf_list = []
            for k, v in filtered.items():
                db_field = self.get_db_field(k)
                if not db_field == k:
                    rf_list.append((k, db_field))
            if rf_list:
                for t in rf_list:
                    filtered[t[1]] = filtered.pop(t[0])
        return filtered


class ModelBase(BaseDocument):
    meta = {'abstract': True}

    created_time = DateTimeField(default=datetime.datetime.now)
    creater_id = ObjectIdField()
    update_time = DateTimeField(default=datetime.datetime.max)
    updater_id = ObjectIdField()
    is_delete = BooleanField(default=False)

    @property
    def oid(self):
        return str(self.id)

    def to_dict(self):
        from commons.common_utils import model_object_hook
        d = json.loads(self.to_json(), object_hook=model_object_hook)
        d['id'] = self.oid
        d.pop('_id')
        return ObjectDict(d)

    def __unicode__(self):
        try:
            return self.name
        except AttributeError:
            return self.oid

    def pro_save(self, is_update):
        pass

    def my_delete(self, executer):
        self.is_delete = True

        if isinstance(self, HistoryBase):
            self.history_position = history_position['tail']

        self.my_save(executer, True)

    def _is_update(self, executer):
        """
        judge if this is a request of update or a request or create
        """
        old_id = self.id
        if not old_id:
            # create
            self.created_time = datetime.datetime.now()
            self.creater_id = executer.id
            self.save()
            return False
        else:
            # update
            self.update_time = datetime.datetime.now()
            self.updater_id = executer.id
            return True

    def my_save(self, executer, is_update=None):
        if is_update is None:
            is_update = self._is_update(executer)

        self.pro_save(is_update)

        # class inherbited from HistoryBase
        if isinstance(self, HistoryBase):
            self.save_history(executer, is_update)

        super(ModelBase, self).save()
        self.after_save()

    def after_save(self):
        pass

    @property
    def created_time_str(self):
        if isinstance(self.created_time, datetime.datetime):
            return self.created_time.strftime(settings.DATE_FORMAT)
        else:
            return self.created_time or ''


class History(BaseDocument):
    ref_id = StringField()
    value = BinaryField()
    time = DateTimeField(default=datetime.datetime.now)
    # operator id
    o_id = ObjectIdField()
    # position id:
    #   0: head of history chain, mostly this means create an instance
    #   1: body of history chain, mostly this means update an instance
    #   2: tail of history chain, mostly this means delete an instance
    p_id = IntField()
    ip = StringField()

    meta = {'db_alias': settings.DB_NAME_HIS}


history_position = {
    'head': 0,  # mostly this means create an instance
    'body': 1,  # mostly this means update an instance
    'tail': 2,  # mostly this means delete an instance
}


class HistoryBase(BaseDocument):
    """
    deal things about changing history
    every class inherbited from this class will have a
    property in name of update_time
    """
    meta = {'abstract': True}

    def __init__(self, *args, **values):
        super(HistoryBase, self).__init__(*args, **values)
        self.history_position = history_position['body']

    def _get_cname_and_ref_id(self):
        ''''''
        cname = self._meta["collection"]
        ref_id = str(self.id)
        return cname, ref_id

    def save_history(self, executer, is_update):
        if not is_update:
            self.history_position = history_position['head']
        elif self.history_position != history_position['tail']:
            self.history_position = history_position['body']

        cname, ref_id = self._get_cname_and_ref_id()
        value = pickle.dumps(self)
        self.create_histroy(cname, ref_id, value, self.history_position, executer)

    def get_history(self, count=0):
        cname, ref_id = self._get_cname_and_ref_id()
        cursor = self.get_histories(cname=cname, ref_id=ref_id)
        if isinstance(count, int):
            cursor = cursor[:count]
        return [(h.time, pickle.loads(str(h.value))) for h in cursor]

    def get_history_info(self, count=0):
        cname, ref_id = self._get_cname_and_ref_id()
        cursor = self.get_histories(cname=cname, ref_id=ref_id)
        if isinstance(count, int):
            cursor = cursor[:count]
        return [(h, pickle.loads(str(h.value))) for h in cursor]

    def get_collection_name(self, cname):
        result = 'history_%s' % cname
        result = result.lower()
        return result

    def create_histroy(self, cname, ref_id, value, position, user):
        history = History()
        history.ref_id = ref_id
        history.value = value
        history.p_id = position
        history.o_id = user.id
        history.ip = getattr(user, 'remote_ip', '')

        cn = self.get_collection_name(cname)
        history.switch_collection(cn)

        try:
            history.save()
        except Exception as e:
            log_argus = (cname, ref_id, user.id, e)
            logger.error('[%s][%s][%s]%s' % log_argus)

    def get_histories(self, cname, **kwargs):
        cn = self.get_collection_name(cname)

        with switch_collection(History, cn) as _history:
            cursor = _history.objects.filter(**kwargs)
            result = cursor.order_by('-time')
        return result
