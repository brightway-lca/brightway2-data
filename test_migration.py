#!/usr/bin/env python3
"""
Test script for the Scenario migration.
This script creates a test database and verifies that the migration works correctly.
"""

import tempfile
import shutil
from pathlib import Path

from peewee import SqliteDatabase

from bw2data.backends.migrations import add_scenario_table_and_foreign_key, check_scenario_migration_needed
from bw2data.backends.schema import ActivityDataset, ExchangeDataset, Scenario


def test_migration():
    """Test the scenario migration."""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create the lci directory structure
        lci_dir = temp_path / "lci"
        lci_dir.mkdir()
        
        # Create a test database
        db_path = lci_dir / "databases.db"
        db = SqliteDatabase(db_path, pragmas={'foreign_keys': 'on'})
        
        # Bind models to database
        ActivityDataset.bind(db, bind_refs=False, bind_backrefs=False)
        ExchangeDataset.bind(db, bind_refs=False, bind_backrefs=False)
        Scenario.bind(db, bind_refs=False, bind_backrefs=False)
        
        # Connect and create tables (without Scenario initially)
        db.connect()
        db.create_tables([ActivityDataset, ExchangeDataset])
        
        print("Initial database state:")
        print(f"Tables: {[table.name for table in db.get_tables()]}")
        print(f"ExchangeDataset columns: {[col.name for col in db.get_columns('exchangedataset')]}")
        
        # Check if migration is needed
        migration_needed = check_scenario_migration_needed(db)
        print(f"\nMigration needed: {migration_needed}")
        
        if migration_needed:
            # Run migration
            print("\nRunning migration...")
            add_scenario_table_and_foreign_key(temp_path, db)
        
        print("\nAfter migration:")
        print(f"Tables: {[table.name for table in db.get_tables()]}")
        print(f"ExchangeDataset columns: {[col.name for col in db.get_columns('exchangedataset')]}")
        
        # Check if migration is still needed
        migration_needed_after = check_scenario_migration_needed(db)
        print(f"\nMigration needed after: {migration_needed_after}")
        
        # Test creating Scenario and ExchangeDataset with foreign key
        try:
            # Create a scenario
            scenario = Scenario.create(
                id="test-scenario-1",
                name="Test Scenario",
                data={"description": "A test scenario"}
            )
            print(f"\nCreated scenario: {scenario.id}")
            
            # Create an activity
            activity = ActivityDataset.create(
                id="test-activity-1",
                code="TEST001",
                database="test_db",
                data={"name": "Test Activity"}
            )
            print(f"Created activity: {activity.id}")
            
            # Create an exchange with scenario
            exchange = ExchangeDataset.create(
                id="test-exchange-1",
                input_code="INPUT001",
                input_database="input_db",
                output_code="OUTPUT001", 
                output_database="output_db",
                type="technosphere",
                scenario=scenario
            )
            print(f"Created exchange with scenario: {exchange.id}")
            
            # Verify the foreign key relationship
            retrieved_exchange = ExchangeDataset.get(ExchangeDataset.id == "test-exchange-1")
            print(f"Exchange scenario: {retrieved_exchange.scenario.name}")
            
            print("\n✅ Migration test completed successfully!")
            
        except Exception as e:
            print(f"\n❌ Error during test: {e}")
            raise


if __name__ == "__main__":
    test_migration() 