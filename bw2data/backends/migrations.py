import shutil
from pathlib import Path

from peewee import SqliteDatabase
from playhouse.migrate import SqliteMigrator, migrate

from bw2data.logs import stdout_feedback_logger
from bw2data.backends.schema import Scenario


def add_scenario_table_and_foreign_key(base_data_dir: Path, db: SqliteDatabase) -> None:
    """Migration to add Scenario table and foreign key to ExchangeDataset.
    
    This migration:
    1. Creates the new 'scenario' table with id, name, and data columns
    2. Adds a 'scenario_id' foreign key column to the 'exchangedataset' table
    """
    src_filepath = base_data_dir / "lci" / "databases.db"
    backup_filepath = base_data_dir / "lci" / "databases.backup-scenario.db"
    
    # Create backup
    shutil.copy(src_filepath, backup_filepath)

    MIGRATION_WARNING = """
Adding Scenario table and foreign key to ExchangeDataset.
A backup copy of this database '{}' was made at '{}'.
If you have problems, file an issue, restore the backup data, and use a stable version of Brightway.
""".format(
        src_filepath, backup_filepath
    )
    stdout_feedback_logger.warning(MIGRATION_WARNING)

    # Create migrator
    migrator = SqliteMigrator(db)
    
    # Check if scenario table already exists
    existing_tables = {table.name for table in db.get_tables()}
    
    if 'scenario' not in existing_tables:
        # Create the scenario table
        migrate(
            migrator.create_table(
                'scenario',
                [
                    ('id', 'TEXT', False, None, None, False),
                    ('name', 'TEXT', False, None, None, False),
                    ('data', 'TEXT', True, None, None, False),
                ]
            )
        )
        stdout_feedback_logger.info("Created 'scenario' table")
    
    # Check if scenario_id column already exists in exchangedataset table
    existing_columns = {col.name for col in db.get_columns("exchangedataset")}
    
    if 'scenario_id' not in existing_columns:
        # Add foreign key column to exchangedataset table
        migrate(
            migrator.add_column('exchangedataset', 'scenario_id', 'TEXT', null=True)
        )
        stdout_feedback_logger.info("Added 'scenario_id' foreign key column to 'exchangedataset' table")
    
    stdout_feedback_logger.info("Migration completed successfully")


def check_scenario_migration_needed(db: SqliteDatabase) -> bool:
    """Check if the scenario migration is needed."""
    existing_tables = {table.name for table in db.get_tables()}
    if 'scenario' not in existing_tables:
        return True
    
    existing_columns = {col.name for col in db.get_columns("exchangedataset")}
    if 'scenario_id' not in existing_columns:
        return True
    
    return False 