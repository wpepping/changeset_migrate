import sys
import os
import sqlalchemy as db
from sqlalchemy.orm import Session as DBSession
from sqlalchemy import exc as db_exc
import pandas as pd
import hashlib

CHANGESETS = "changesets"
TABLES = "tables"
EXTENSION = ".sql"
DATABASE_SCHEMA = "changeset_migrate"
MIGRATION_HISTORY_TABLE = "migration_history"
INSERT_INTO_MIGRATION_HISTORY_TABLE = "INSERT INTO " + DATABASE_SCHEMA + "." + MIGRATION_HISTORY_TABLE + " (name, type, hash) VALUES ('<<NAME>>', '<<TYPE>>', '<<HASH>>');"
CREATE_TABLE_QUERY_FILE = "migration_history.sql"
ENCODINGS = ["utf-8", "ansi"]

df_migration_history = None
debug_info = False


def migrate(target_folder, source_folder_tables, source_folder_changesets, source_folder_procedures, connection_string, debug = False):
    debug_message("migrate")

    global debug_info
    debug_info = debug

    tables_folder = os.path.join(target_folder, TABLES)
    changesets_folder = os.path.join(target_folder, CHANGESETS)

    create_table_query_path = os.path.join(os.path.dirname(__file__), CREATE_TABLE_QUERY_FILE)

    os.makedirs(tables_folder, exist_ok=True)
    os.makedirs(changesets_folder, exist_ok=True)

    create_statements = get_create_statements_from_folder(source_folder_tables)

    db_connection = db.create_engine(connection_string).connect()
    db_connection.execute(read_file(create_table_query_path))

    try:
        create_statements = validate_changesets(create_statements, target_folder, TABLES, db_connection)
    except ChangesetValidationError as ex:
        report_error("Table create statement " + ex.changeset_name + "' has been changed after it was deployed. Aborting.")

    changesets = get_changesets_from_folder(source_folder_changesets)

    try:
        changesets = validate_changesets(changesets, target_folder, CHANGESETS, db_connection)
    except ChangesetValidationError as ex:
        report_error("Changeset '" + ex.changeset_name + "' has been changed after it was deployed. Aborting.")

    # Open a session to run within a transaction
    db_session = DBSession(db_connection)

    deploy_changesets(create_statements, target_folder, TABLES, db_connection, db_session)
    deploy_changesets(changesets, target_folder, CHANGESETS, db_connection, db_session)
    deploy_procedures_and_functions(source_folder_procedures, db_session)

    db_session.commit()
    db_session.close()


def get_create_statements_from_folder(source_folder):
    result = dict()

    for subdir, dirs, files in os.walk(source_folder):
        for file in files:
            if file[-4:] == EXTENSION:
                changeset_name = os.path.relpath(os.path.join(subdir, file), source_folder).rsplit('.', 1)[0].replace(os.sep, '_')
                contents = read_file(os.path.join(subdir, file)).strip()
                result[changeset_name] = {"contents": contents, "hash": hashlib.md5(contents.encode("utf-8")).hexdigest()}

    return result


def deploy_procedures_and_functions(folder, db_session):
    debug_message("migrate_procedures_and_functions")
    for subdir, dirs, files in os.walk(folder):
        for file in files:
            if file[-4:] == EXTENSION:
                debug_message("Deploy procedure or function " + file[:-4])

                contents = read_file(os.path.join(subdir, file)).strip().replace(":", "\:")
                db_session.execute(contents)


def get_changesets_from_folder(folder):
    debug_message("get_changesets_from_folder")

    result = dict()

    for subdir, dirs, files in os.walk(folder):
        for file in files:
            if file[-4:] == EXTENSION:
                result.update(get_changesets_from_file(os.path.join(subdir, file), result))

    return result


def get_changesets_from_file(file, changesets = {}):
    # Split up file in a dict of separate changesets
    debug_message("get_changesets_from_file")

    name = None
    result = dict()

    file_and_first_line = open_file_and_read_line(file)
    infile = file_and_first_line["file"]
    line = file_and_first_line["line"]

    while line and line.strip() == "":  # skip leading white space
        line = infile.readline()

    if line and line[0:11] != "--changeset":
        report_error("File '" + file + "' does not start with changeset.")

    while line:
        if line[0:11] == "--changeset":
            debug_message("Read changeset: " + line.replace("--changeset ", "").strip())

            # Save previous changeset
            if name:
                result[name] = {"contents": contents.strip(), "hash": hashlib.md5(contents.strip().encode("utf-8")).hexdigest()}

            name = line.replace("--changeset ", "").strip()
            if name == "":
                report_error("Changeset name missing in " + file)
            if name in result or name in changesets:
                report_error("Changeset name '" + name + "' occurs more than once.")

            contents = ""
        else:
            contents = contents + line

        line = infile.readline()

    if name:
        result[name] = {"contents": contents.strip(), "hash": hashlib.md5(contents.strip().encode("utf-8")).hexdigest()}

    return result


def validate_changesets(changesets, target_folder, type, db_connection):
    # Checks if any changesets that already exist as file have changed.
    # Returns dict with only changesets that don't exist.
    # Throws ChangesetValidationError in case any existing changeset has been changed
    debug_message("validate_changesets")

    folder = os.path.join(target_folder, type)
    df_migration_history = get_migration_history_table(db_connection)
    df_migration_history.loc[:,"type_name"] = df_migration_history.loc[:,"type"] + "_" + df_migration_history.loc[:,"name"]

    result = dict()

    for changeset_name in changesets:
        if changeset_name in df_migration_history.loc[:,"name"].values:
            old_hash = df_migration_history.loc[(df_migration_history['name'] == changeset_name) & (df_migration_history['type'] == type)].hash.item()
            new_hash = changesets[changeset_name]["hash"]

            debug_message(type + " " + changeset_name + " - old hash: " + old_hash + " / new hash: " + new_hash)

            if new_hash != old_hash:
                raise ChangesetValidationError(changeset_name=changeset_name)

        else:
            result[changeset_name] = changesets[changeset_name]

    return result


def deploy_changesets(changesets, target_folder, type, db_connection, db_session):
    debug_message("deploy_changesets")

    folder = os.path.join(target_folder, type)
    df_migration_history = get_migration_history_table(db_connection)

    for changeset_name in changesets:
        path = os.path.join(folder, changeset_name + EXTENSION)

        if not type + "_" + changeset_name in df_migration_history.loc[:,"type_name"].values:
            debug_message("Deploy changeset (" + type + "): " + changeset_name)

            try:
                db_session.execute(changesets[changeset_name]["contents"])
            except db_exc.ProgrammingError as e:
                print("CAUSED BY")
                print(changesets[changeset_name]["contents"])
                db_session.rollback()
                report_error(e.args[0])

            db_session.execute(INSERT_INTO_MIGRATION_HISTORY_TABLE.replace("<<NAME>>", changeset_name).replace("<<TYPE>>", type).replace("<<HASH>>", changesets[changeset_name]["hash"]))

            if not os.path.isfile(path):
                with open(path, 'w', encoding='utf-8') as outfile:
                    outfile.write(changesets[changeset_name]["contents"])


def get_migration_history_table(db_connection):
    global df_migration_history
    if df_migration_history is None:
        df_migration_history = pd.read_sql_table(MIGRATION_HISTORY_TABLE, db_connection, schema=DATABASE_SCHEMA)

    return df_migration_history


def report_error(error):
    print("Error: " + error)
    sys.exit(1)


def debug_message(message):
    if debug_info:
        print(message)

def read_file(path):
    for e in ENCODINGS:
        try:
            result = open(path, 'r', encoding=e).read()
        except UnicodeDecodeError:
            continue
        
        return result

def open_file_and_read_line(path):
    for e in ENCODINGS:
        try:
            infile = open(path, 'r', encoding=e)
            line = infile.readline()
        except UnicodeDecodeError:
            continue

        return {"file": infile, "line": line}


class ChangesetValidationError(BaseException):
    def __init__(self, changeset_name, message="Changeset has been changed after it was deployed."):
        self.changeset_name = changeset_name
        self.message = message
        super().__init__(self.message)