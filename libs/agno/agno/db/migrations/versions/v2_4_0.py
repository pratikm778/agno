"""Migration v2.4.0: Multi-ingestion path fields for knowledge table

Changes:
- Add multi-ingestion path columns to knowledge table:
  - original_path, storage_path, relative_path
  - root_id, root_path, root_label
  - source_type, parent_folder
  - link_status, link_checked_at
  - upload_batch_id
- Add indexes on source_type, upload_batch_id, root_id, root_path
- Backfill existing rows with source_type='legacy'
"""

from agno.db.base import AsyncBaseDb, BaseDb
from agno.db.migrations.utils import quote_db_identifier
from agno.utils.log import log_error, log_info

try:
    from sqlalchemy import text
except ImportError:
    raise ImportError("`sqlalchemy` not installed. Please install it using `pip install sqlalchemy`")


# New columns to add to knowledge table
KNOWLEDGE_COLUMNS = [
    ("original_path", "TEXT"),
    ("storage_path", "TEXT"),
    ("relative_path", "TEXT"),
    ("root_id", "VARCHAR(255)"),
    ("root_path", "TEXT"),
    ("root_label", "VARCHAR(255)"),
    ("source_type", "VARCHAR(50)"),
    ("parent_folder", "VARCHAR(255)"),
    ("link_status", "VARCHAR(50)"),
    ("link_checked_at", "BIGINT"),
    ("upload_batch_id", "VARCHAR(255)"),
]

# Columns that need indexes
INDEXED_COLUMNS = ["source_type", "upload_batch_id", "root_id", "root_path"]


def up(db: BaseDb, table_type: str, table_name: str) -> bool:
    """
    Apply multi-ingestion path fields to knowledge table.

    Returns:
        bool: True if any migration was applied, False otherwise.
    """
    if table_type != "knowledge":
        return False

    db_type = type(db).__name__

    try:
        if db_type == "PostgresDb":
            return _migrate_postgres(db, table_name)
        elif db_type == "MySQLDb":
            return _migrate_mysql(db, table_name)
        elif db_type == "SqliteDb":
            return _migrate_sqlite(db, table_name)
        elif db_type == "SingleStoreDb":
            return _migrate_singlestore(db, table_name)
        else:
            log_info(f"{db_type} does not require schema migrations (NoSQL/document store)")
        return False
    except Exception as e:
        log_error(f"Error running migration v2.4.0 for {db_type} on table {table_name}: {e}")
        raise


async def async_up(db: AsyncBaseDb, table_type: str, table_name: str) -> bool:
    """
    Apply multi-ingestion path fields to knowledge table (async).

    Returns:
        bool: True if any migration was applied, False otherwise.
    """
    if table_type != "knowledge":
        return False

    db_type = type(db).__name__

    try:
        if db_type == "AsyncPostgresDb":
            return await _migrate_async_postgres(db, table_name)
        elif db_type == "AsyncSqliteDb":
            return await _migrate_async_sqlite(db, table_name)
        else:
            log_info(f"{db_type} does not require schema migrations (NoSQL/document store)")
        return False
    except Exception as e:
        log_error(f"Error running migration v2.4.0 for {db_type} on table {table_name}: {e}")
        raise


def down(db: BaseDb, table_type: str, table_name: str) -> bool:
    """
    Revert multi-ingestion path fields from knowledge table.

    Returns:
        bool: True if any migration was reverted, False otherwise.
    """
    if table_type != "knowledge":
        return False

    db_type = type(db).__name__

    try:
        if db_type == "PostgresDb":
            return _revert_postgres(db, table_name)
        elif db_type == "MySQLDb":
            return _revert_mysql(db, table_name)
        elif db_type == "SqliteDb":
            return _revert_sqlite(db, table_name)
        elif db_type == "SingleStoreDb":
            return _revert_singlestore(db, table_name)
        else:
            log_info(f"Revert not implemented for {db_type}")
        return False
    except Exception as e:
        log_error(f"Error reverting migration v2.4.0 for {db_type} on table {table_name}: {e}")
        raise


async def async_down(db: AsyncBaseDb, table_type: str, table_name: str) -> bool:
    """
    Revert multi-ingestion path fields from knowledge table (async).

    Returns:
        bool: True if any migration was reverted, False otherwise.
    """
    if table_type != "knowledge":
        return False

    db_type = type(db).__name__

    try:
        if db_type == "AsyncPostgresDb":
            return await _revert_async_postgres(db, table_name)
        elif db_type == "AsyncSqliteDb":
            return await _revert_async_sqlite(db, table_name)
        else:
            log_info(f"Revert not implemented for {db_type}")
        return False
    except Exception as e:
        log_error(f"Error reverting migration v2.4.0 for {db_type} on table {table_name}: {e}")
        raise


def _migrate_postgres(db: BaseDb, table_name: str) -> bool:
    """Migrate PostgreSQL database."""
    db_schema = db.db_schema or "public"  # type: ignore
    db_type = type(db).__name__
    quoted_schema = quote_db_identifier(db_type, db_schema)
    quoted_table = quote_db_identifier(db_type, table_name)

    with db.Session() as sess, sess.begin():  # type: ignore
        # Check if table exists
        table_exists = sess.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_name = :table_name
                )
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        ).scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping migration")
            return False

        # Get existing columns
        check_columns = sess.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table_name
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        ).fetchall()
        existing_columns = {row[0] for row in check_columns}

        applied_changes = False

        # Add new columns
        for col_name, col_type in KNOWLEDGE_COLUMNS:
            if col_name not in existing_columns:
                log_info(f"-- Adding {col_name} column to {table_name}")
                sess.execute(
                    text(
                        f"""
                        ALTER TABLE {quoted_schema}.{quoted_table}
                        ADD COLUMN {col_name} {col_type}
                        """
                    )
                )
                applied_changes = True

        # Add indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            # Check if index exists
            index_exists = sess.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT FROM pg_indexes
                        WHERE schemaname = :schema
                        AND tablename = :table_name
                        AND indexname = :index_name
                    )
                    """
                ),
                {"schema": db_schema, "table_name": table_name, "index_name": index_name},
            ).scalar()

            if not index_exists:
                log_info(f"-- Adding index {index_name}")
                sess.execute(
                    text(
                        f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {quoted_schema}.{quoted_table}({col_name})
                        """
                    )
                )
                applied_changes = True

        # Backfill source_type='legacy' for existing rows
        if "source_type" not in existing_columns or applied_changes:
            log_info(f"-- Backfilling source_type='legacy' for existing rows")
            sess.execute(
                text(
                    f"""
                    UPDATE {quoted_schema}.{quoted_table}
                    SET source_type = 'legacy'
                    WHERE source_type IS NULL
                    """
                )
            )

        sess.commit()
        return applied_changes


async def _migrate_async_postgres(db: AsyncBaseDb, table_name: str) -> bool:
    """Migrate PostgreSQL database (async)."""
    db_schema = db.db_schema or "public"  # type: ignore
    db_type = type(db).__name__
    quoted_schema = quote_db_identifier(db_type, db_schema)
    quoted_table = quote_db_identifier(db_type, table_name)

    async with db.async_session_factory() as sess, sess.begin():  # type: ignore
        # Check if table exists
        result = await sess.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_name = :table_name
                )
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        )
        table_exists = result.scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping migration")
            return False

        # Get existing columns
        result = await sess.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table_name
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        )
        check_columns = result.fetchall()
        existing_columns = {row[0] for row in check_columns}

        applied_changes = False

        # Add new columns
        for col_name, col_type in KNOWLEDGE_COLUMNS:
            if col_name not in existing_columns:
                log_info(f"-- Adding {col_name} column to {table_name}")
                await sess.execute(
                    text(
                        f"""
                        ALTER TABLE {quoted_schema}.{quoted_table}
                        ADD COLUMN {col_name} {col_type}
                        """
                    )
                )
                applied_changes = True

        # Add indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            result = await sess.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT FROM pg_indexes
                        WHERE schemaname = :schema
                        AND tablename = :table_name
                        AND indexname = :index_name
                    )
                    """
                ),
                {"schema": db_schema, "table_name": table_name, "index_name": index_name},
            )
            index_exists = result.scalar()

            if not index_exists:
                log_info(f"-- Adding index {index_name}")
                await sess.execute(
                    text(
                        f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {quoted_schema}.{quoted_table}({col_name})
                        """
                    )
                )
                applied_changes = True

        # Backfill source_type='legacy' for existing rows
        if "source_type" not in existing_columns or applied_changes:
            log_info(f"-- Backfilling source_type='legacy' for existing rows")
            await sess.execute(
                text(
                    f"""
                    UPDATE {quoted_schema}.{quoted_table}
                    SET source_type = 'legacy'
                    WHERE source_type IS NULL
                    """
                )
            )

        await sess.commit()
        return applied_changes


def _migrate_sqlite(db: BaseDb, table_name: str) -> bool:
    """Migrate SQLite database."""
    db_type = type(db).__name__
    quoted_table = quote_db_identifier(db_type, table_name)

    with db.Session() as sess, sess.begin():  # type: ignore
        # Check if table exists
        table_exists = sess.execute(
            text(
                """
                SELECT COUNT(*) FROM sqlite_master
                WHERE type='table' AND name=:table_name
                """
            ),
            {"table_name": table_name},
        ).scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping migration")
            return False

        # Get existing columns
        result = sess.execute(text(f"PRAGMA table_info({quoted_table})"))
        columns_info = result.fetchall()
        existing_columns = {row[1] for row in columns_info}

        applied_changes = False

        # Add new columns
        for col_name, col_type in KNOWLEDGE_COLUMNS:
            if col_name not in existing_columns:
                log_info(f"-- Adding {col_name} column to {table_name}")
                # SQLite uses VARCHAR for TEXT in some cases
                sqlite_type = col_type.replace("VARCHAR(255)", "VARCHAR").replace("VARCHAR(50)", "VARCHAR")
                sess.execute(
                    text(f"ALTER TABLE {quoted_table} ADD COLUMN {col_name} {sqlite_type}")
                )
                applied_changes = True

        # Add indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            sess.execute(
                text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {quoted_table}({col_name})")
            )

        # Backfill source_type='legacy' for existing rows
        if "source_type" not in existing_columns or applied_changes:
            log_info(f"-- Backfilling source_type='legacy' for existing rows")
            sess.execute(
                text(
                    f"""
                    UPDATE {quoted_table}
                    SET source_type = 'legacy'
                    WHERE source_type IS NULL
                    """
                )
            )

        sess.commit()
        return applied_changes


async def _migrate_async_sqlite(db: AsyncBaseDb, table_name: str) -> bool:
    """Migrate SQLite database (async)."""
    db_type = type(db).__name__
    quoted_table = quote_db_identifier(db_type, table_name)

    async with db.async_session_factory() as sess, sess.begin():  # type: ignore
        # Check if table exists
        result = await sess.execute(
            text(
                """
                SELECT COUNT(*) FROM sqlite_master
                WHERE type='table' AND name=:table_name
                """
            ),
            {"table_name": table_name},
        )
        table_exists = result.scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping migration")
            return False

        # Get existing columns
        result = await sess.execute(text(f"PRAGMA table_info({quoted_table})"))
        columns_info = result.fetchall()
        existing_columns = {row[1] for row in columns_info}

        applied_changes = False

        # Add new columns
        for col_name, col_type in KNOWLEDGE_COLUMNS:
            if col_name not in existing_columns:
                log_info(f"-- Adding {col_name} column to {table_name}")
                sqlite_type = col_type.replace("VARCHAR(255)", "VARCHAR").replace("VARCHAR(50)", "VARCHAR")
                await sess.execute(
                    text(f"ALTER TABLE {quoted_table} ADD COLUMN {col_name} {sqlite_type}")
                )
                applied_changes = True

        # Add indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            await sess.execute(
                text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {quoted_table}({col_name})")
            )

        # Backfill source_type='legacy' for existing rows
        if "source_type" not in existing_columns or applied_changes:
            log_info(f"-- Backfilling source_type='legacy' for existing rows")
            await sess.execute(
                text(
                    f"""
                    UPDATE {quoted_table}
                    SET source_type = 'legacy'
                    WHERE source_type IS NULL
                    """
                )
            )

        await sess.commit()
        return applied_changes


def _migrate_mysql(db: BaseDb, table_name: str) -> bool:
    """Migrate MySQL database."""
    db_schema = db.db_schema or "agno"  # type: ignore
    db_type = type(db).__name__
    quoted_schema = quote_db_identifier(db_type, db_schema)
    quoted_table = quote_db_identifier(db_type, table_name)

    with db.Session() as sess, sess.begin():  # type: ignore
        # Check if table exists
        table_exists = sess.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = :schema
                    AND TABLE_NAME = :table_name
                )
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        ).scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping migration")
            return False

        # Get existing columns
        check_columns = sess.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                AND TABLE_NAME = :table_name
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        ).fetchall()
        existing_columns = {row[0] for row in check_columns}

        applied_changes = False

        # Add new columns
        for col_name, col_type in KNOWLEDGE_COLUMNS:
            if col_name not in existing_columns:
                log_info(f"-- Adding {col_name} column to {table_name}")
                sess.execute(
                    text(
                        f"""
                        ALTER TABLE {quoted_schema}.{quoted_table}
                        ADD COLUMN `{col_name}` {col_type}
                        """
                    )
                )
                applied_changes = True

        # Add indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            index_exists = sess.execute(
                text(
                    """
                    SELECT COUNT(1) FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = :schema
                    AND TABLE_NAME = :table_name
                    AND INDEX_NAME = :index_name
                    """
                ),
                {"schema": db_schema, "table_name": table_name, "index_name": index_name},
            ).scalar()

            if not index_exists:
                log_info(f"-- Adding index {index_name}")
                sess.execute(
                    text(
                        f"""
                        ALTER TABLE {quoted_schema}.{quoted_table}
                        ADD INDEX `{index_name}` (`{col_name}`)
                        """
                    )
                )
                applied_changes = True

        # Backfill source_type='legacy' for existing rows
        if "source_type" not in existing_columns or applied_changes:
            log_info(f"-- Backfilling source_type='legacy' for existing rows")
            sess.execute(
                text(
                    f"""
                    UPDATE {quoted_schema}.{quoted_table}
                    SET `source_type` = 'legacy'
                    WHERE `source_type` IS NULL
                    """
                )
            )

        sess.commit()
        return applied_changes


def _migrate_singlestore(db: BaseDb, table_name: str) -> bool:
    """Migrate SingleStore database."""
    # SingleStore uses MySQL-compatible syntax
    return _migrate_mysql(db, table_name)


def _revert_postgres(db: BaseDb, table_name: str) -> bool:
    """Revert PostgreSQL migration."""
    db_schema = db.db_schema or "public"  # type: ignore
    db_type = type(db).__name__
    quoted_schema = quote_db_identifier(db_type, db_schema)
    quoted_table = quote_db_identifier(db_type, table_name)

    with db.Session() as sess, sess.begin():  # type: ignore
        table_exists = sess.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_name = :table_name
                )
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        ).scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping revert")
            return False

        # Drop indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            sess.execute(text(f"DROP INDEX IF EXISTS {quoted_schema}.{index_name}"))

        # Drop columns
        for col_name, _ in KNOWLEDGE_COLUMNS:
            sess.execute(
                text(
                    f"""
                    ALTER TABLE {quoted_schema}.{quoted_table}
                    DROP COLUMN IF EXISTS {col_name}
                    """
                )
            )

        sess.commit()
        return True


async def _revert_async_postgres(db: AsyncBaseDb, table_name: str) -> bool:
    """Revert PostgreSQL migration (async)."""
    db_schema = db.db_schema or "public"  # type: ignore
    db_type = type(db).__name__
    quoted_schema = quote_db_identifier(db_type, db_schema)
    quoted_table = quote_db_identifier(db_type, table_name)

    async with db.async_session_factory() as sess, sess.begin():  # type: ignore
        result = await sess.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_name = :table_name
                )
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        )
        table_exists = result.scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping revert")
            return False

        # Drop indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            await sess.execute(text(f"DROP INDEX IF EXISTS {quoted_schema}.{index_name}"))

        # Drop columns
        for col_name, _ in KNOWLEDGE_COLUMNS:
            await sess.execute(
                text(
                    f"""
                    ALTER TABLE {quoted_schema}.{quoted_table}
                    DROP COLUMN IF EXISTS {col_name}
                    """
                )
            )

        await sess.commit()
        return True


def _revert_mysql(db: BaseDb, table_name: str) -> bool:
    """Revert MySQL migration."""
    db_schema = db.db_schema or "agno"  # type: ignore
    db_type = type(db).__name__
    quoted_schema = quote_db_identifier(db_type, db_schema)
    quoted_table = quote_db_identifier(db_type, table_name)

    with db.Session() as sess, sess.begin():  # type: ignore
        table_exists = sess.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = :schema
                    AND TABLE_NAME = :table_name
                )
                """
            ),
            {"schema": db_schema, "table_name": table_name},
        ).scalar()

        if not table_exists:
            log_info(f"Table {table_name} does not exist, skipping revert")
            return False

        # Drop indexes
        for col_name in INDEXED_COLUMNS:
            index_name = f"idx_{table_name}_{col_name}"
            index_exists = sess.execute(
                text(
                    """
                    SELECT COUNT(1) FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = :schema
                    AND TABLE_NAME = :table_name
                    AND INDEX_NAME = :index_name
                    """
                ),
                {"schema": db_schema, "table_name": table_name, "index_name": index_name},
            ).scalar()

            if index_exists:
                sess.execute(
                    text(f"ALTER TABLE {quoted_schema}.{quoted_table} DROP INDEX `{index_name}`")
                )

        # Drop columns
        for col_name, _ in KNOWLEDGE_COLUMNS:
            sess.execute(
                text(
                    f"""
                    ALTER TABLE {quoted_schema}.{quoted_table}
                    DROP COLUMN IF EXISTS `{col_name}`
                    """
                )
            )

        sess.commit()
        return True


def _revert_sqlite(db: BaseDb, table_name: str) -> bool:
    """Revert SQLite migration."""
    from agno.utils.log import log_warning
    log_warning(f"-- SQLite does not support DROP COLUMN easily. Manual migration may be required for {table_name}.")
    return False


async def _revert_async_sqlite(db: AsyncBaseDb, table_name: str) -> bool:
    """Revert SQLite migration (async)."""
    from agno.utils.log import log_warning
    log_warning(f"-- SQLite does not support DROP COLUMN easily. Manual migration may be required for {table_name}.")
    return False


def _revert_singlestore(db: BaseDb, table_name: str) -> bool:
    """Revert SingleStore migration."""
    return _revert_mysql(db, table_name)
