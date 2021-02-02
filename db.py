import db_api
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import operator
from dataclasses_json import dataclass_json

DB_ROOT = Path('db_files')


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        if not os.path.isfile(f"{DB_ROOT}/database.json"):
            with (DB_ROOT / "database.json").open("w") as file:
                json.dump({}, file)

    def create_metadata_if_needed(self):
        with (DB_ROOT / "database.json").open('w') as f:
            json.dump({}, f)

    def read_metadata(self):
        with (DB_ROOT / "database.json").open() as f:
            return json.load(f)

    def write_metadata(self, data):
        with (DB_ROOT / "database.json").open("w") as f:
            json.dump(data, f, indent=4)

    def validate_primary_key(self, fields, key):
        if key not in [field.name for field in fields]:
            raise ValueError

    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str) -> db_api.DBTable:
        self.validate_primary_key(fields, key_field_name)

        try:
            add_table = self.read_metadata()
        except:
            self.create_metadata_if_needed()
            add_table = self.read_metadata()

        add_table[table_name] = {"files": f"{table_name}.json",
                                 "fields": [(field.name, field.type.__name__) for field in fields],
                                 "key_field_name": key_field_name}

        self.write_metadata(add_table)

        with open(f"{DB_ROOT}/{add_table[table_name]['files']}", "w") as f:
            json.dump([], f)

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        try:
            return len(self.read_metadata())
        except:
            self.create_metadata_if_needed()
            return 0

    def get_table(self, table_name: str) -> db_api.DBTable:
        types = {"str": str, "int": int}
        DBtable_data = self.read_metadata()[table_name]
        DBField_data = [db_api.DBField(iter[0], types[iter[1]]) for iter in DBtable_data["fields"]]
        table = DBTable(table_name, DBField_data, DBtable_data["key_field_name"])
        return table

    def delete_table(self, table_name: str) -> None:
        try:
            del_table = self.read_metadata()
            del del_table[table_name]
            self.write_metadata(del_table)
        except:
            None

    def get_tables_names(self) -> List[Any]:
        try:
            return [key for key in self.read_metadata().keys()]
        except:
            return []



@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[db_api.DBField]
    key_field_name: str

    def read_file(self):
        with (DB_ROOT / f'{self.name}.json').open() as f:
            return json.load(f)

    def write_to_file(self, data):
        with (DB_ROOT / f'{self.name}.json').open("w") as f:
            json.dump(data, f, indent=4)

    def count(self) -> int:
        return len(self.read_file())

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.find_record(values[self.key_field_name]):
            raise ValueError

        data = self.read_file()
        data.append(values)
        self.write_to_file(data)

    def find_record(self, key: Any):
        data = self.read_file()

        for record in data:
            if record[self.key_field_name] == key:
                return record
        return None

    def delete_record(self, key: Any) -> None:
        data = self.read_file()
        data.remove(self.find_record(key))
        self.write_to_file(data)

    def cmp(self, arg1, op: str, arg2):
        ops = {
            '<': operator.lt,
            '<=': operator.le,
            '==': operator.eq,
            '!=': operator.ne,
            '>=': operator.ge,
            '>': operator.gt
        }
        operation = ops.get(op)
        return operation(arg1, arg2)

    def filter_record(self, criteria, record):
        for c in criteria:
            if not self.cmp(record[c.field_name], c.operator if c.operator != '=' else '==', c.value):
                return False
        return True

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        data = self.read_file()
        data_after_deletion = [record for record in data if not self.filter_record(criteria, record)]
        self.write_to_file(data_after_deletion)

    def get_record(self, key: Any) -> Dict[str, Any]:
            return self.find_record(key)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data = self.read_file()

        for record in data:
            if record[self.key_field_name] == key:
                for update_key in values.keys():
                    record[update_key] = values[update_key]
                break
        self.write_to_file(data)
