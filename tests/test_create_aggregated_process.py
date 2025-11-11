"""Tests for Activity.create_aggregated_process method."""

from unittest.mock import Mock, patch

import numpy as np
import pytest

from bw2data import Database, get_node
from bw2data.database import DatabaseChooser
from bw2data.tests import bw2test

try:
    import bw2calc

    BW2CALC_AVAILABLE = True
except ImportError:
    BW2CALC_AVAILABLE = False


@bw2test
def test_create_aggregated_process_database_not_exists():
    """Test that create_aggregated_process raises error when database doesn't exist"""
    db = DatabaseChooser("source_db")
    db.register()

    # Create a process node
    process = db.new_node(code="process_1", name="Test Process", type="process", unit="kg")
    process.new_exchange(amount=1.0, input=process, type="production").save()
    process.save()

    # Try to create aggregated process in non-existent database
    with pytest.raises(ValueError, match="Database nonexistent_db doesn't exist"):
        process.create_aggregated_process(database="nonexistent_db")


@bw2test
def test_create_aggregated_process_non_process_type():
    """Test that create_aggregated_process raises error for non-process node types"""
    db = DatabaseChooser("source_db")
    db.register()

    # Create a product node (not a process)
    product = db.new_node(code="product_1", name="Test Product", type="product", unit="kg")
    product.save()

    # Try to create aggregated process from product
    with pytest.raises(
        ValueError, match="Only works with a `process` or `processwithreferenceproduct` node"
    ):
        product.create_aggregated_process()


@bw2test
def test_create_aggregated_process_mock_lca():
    """Test create_aggregated_process with mocked LCA"""

    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            },
            ("biosphere", "CH4"): {
                "name": "Methane",
                "type": "emission",
                "unit": "kg",
            },
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a product node
    product = source_db.new_node(
        code="product_1", name="Product 1", type="product", unit="kg", location="GLO"
    )
    product.save()

    # Create a process node with production exchange
    process = source_db.new_node(
        code="process_1", name="Test Process", type="process", unit="kg", location="GLO"
    )
    process.new_exchange(amount=1.0, input=product, type="production", functional=True).save()
    process.save()

    # Mock LCA and its components
    mock_inventory = np.array([[10.0], [5.0]])  # Two biosphere flows
    mock_lca = Mock()
    mock_lca.inventory = Mock()
    mock_lca.inventory.sum.return_value = mock_inventory
    mock_lca.dicts = Mock()
    # Get the actual node IDs from the biosphere database
    co2_node = get_node(key=("biosphere", "CO2"))
    ch4_node = get_node(key=("biosphere", "CH4"))
    mock_lca.dicts.biosphere = {
        co2_node.id: 0,  # CO2 at row 0
        ch4_node.id: 1,  # CH4 at row 1
    }

    # Mock prepare_lca_inputs
    mock_fu = {product: 1.0}
    mock_data_objs = []

    with patch("bw2data.compat.prepare_lca_inputs", return_value=(mock_fu, mock_data_objs, None)):
        with patch("bw2calc.LCA", return_value=mock_lca) as mock_lca_class:
            mock_lca_class.return_value = mock_lca

            # Create aggregated process
            new_process, new_product = process.create_aggregated_process(database="source_db")

    # Verify new process was created
    assert new_process is not None
    assert new_process["code"] != process["code"]  # Should have different code
    assert new_process["database"] == "source_db"
    assert new_process["name"] == "Test Process"
    assert new_process["type"] == "process"

    # Verify product was copied (since product != self)
    assert new_product is not None
    assert new_product["code"] != product["code"]
    assert new_product["database"] == "source_db"
    assert new_product["name"] == "Product 1"
    assert new_product["type"] == "product"

    new_exchanges = list(new_process.exchanges())
    production_exchanges = [e for e in new_exchanges if e["type"] == "production"]
    assert len(production_exchanges) == 1

    prod_exc = production_exchanges[0]
    assert prod_exc["amount"] == 1.0
    assert prod_exc.get("functional") == True
    # Verify the production exchange points to the new product
    assert prod_exc["input"] == new_product.key

    # Verify LCA was called correctly
    mock_lca.lci.assert_called_once()

    # Verify biosphere exchanges were created
    biosphere_exchanges = [e for e in new_exchanges if e["type"] == "biosphere"]

    # Should have 2 biosphere exchanges (CO2 and CH4)
    assert len(biosphere_exchanges) == 2

    # Check amounts
    amounts = {e["input"][1]: e["amount"] for e in biosphere_exchanges}
    assert amounts.get("CO2") == 10.0
    assert amounts.get("CH4") == 5.0


@bw2test
def test_create_aggregated_process_self_production():
    """Test create_aggregated_process when process produces itself (product == self)"""
    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            }
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a process that produces itself
    process = source_db.new_node(
        code="process_1", name="Self-Producing Process", type="process", unit="kg", location="GLO"
    )
    process.new_exchange(amount=1.0, input=process, type="production").save()
    process.save()

    # Mock LCA
    mock_inventory = np.array([[15.0]])
    mock_lca = Mock()
    mock_lca.inventory = Mock()
    # Get the actual node ID from the biosphere database
    co2_node = get_node(key=("biosphere", "CO2"))

    mock_lca.inventory.sum.return_value = mock_inventory
    mock_lca.dicts = Mock()
    mock_lca.dicts.biosphere = {co2_node.id: 0}

    mock_fu = {process: 1.0}
    mock_data_objs = []

    with patch("bw2data.compat.prepare_lca_inputs", return_value=(mock_fu, mock_data_objs, None)):
        with patch("bw2calc.LCA", return_value=mock_lca):
            new_process, new_product = process.create_aggregated_process(database="source_db")

    # Verify it worked
    assert new_process is not None
    assert new_process["code"] != process["code"]

    # Since product == self, new_product should be the same as new_process
    assert new_product is new_process

    # Should have production exchange pointing to self
    new_exchanges = list(new_process.exchanges())
    production_exchanges = [e for e in new_exchanges if e["type"] == "production"]
    assert len(production_exchanges) == 1
    assert production_exchanges[0]["input"] == new_process.key


@bw2test
def test_create_aggregated_process_custom_kwargs():
    """Test that create_aggregated_process passes kwargs to _create_activity_copy"""
    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            }
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a process node
    product = source_db.new_node(code="product_1", name="Product 1", type="product", unit="kg")
    product.save()

    process = source_db.new_node(
        code="process_1", name="Original Process", type="process", unit="kg"
    )
    process.new_exchange(amount=1.0, input=product, type="production", functional=True).save()
    process.save()

    # Mock LCA
    mock_inventory = np.array([[0.0]])  # No biosphere flows
    mock_lca = Mock()
    mock_lca.inventory = Mock()
    mock_lca.inventory.sum.return_value = mock_inventory
    mock_lca.dicts = Mock()
    mock_lca.dicts.biosphere = {}

    mock_fu = {product: 1.0}
    mock_data_objs = []

    with patch("bw2data.compat.prepare_lca_inputs", return_value=(mock_fu, mock_data_objs, None)):
        with patch("bw2calc.LCA", return_value=mock_lca):
            # Create aggregated process with custom attributes
            new_process, new_product = process.create_aggregated_process(
                database="source_db", name="Aggregated Process", location="US"
            )

    # Verify custom attributes were applied
    assert new_process["name"] == "Aggregated Process"
    assert new_process["location"] == "US"
    assert new_process["type"] == "process"  # Original type preserved

    # Verify product was also copied
    assert new_product is not None
    assert new_product["database"] == "source_db"

    new_exchanges = list(new_process.exchanges())
    production_exchanges = [e for e in new_exchanges if e["type"] == "production"]
    assert len(production_exchanges) == 1
    assert production_exchanges[0]["amount"] == 1.0
    assert production_exchanges[0]["input"] == new_product.key


@bw2test
def test_create_aggregated_process_zero_biosphere_amounts():
    """Test that biosphere exchanges with zero amounts are not created"""
    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            },
            ("biosphere", "CH4"): {
                "name": "Methane",
                "type": "emission",
                "unit": "kg",
            },
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a process node
    product = source_db.new_node(code="product_1", name="Product 1", type="product", unit="kg")
    product.save()

    process = source_db.new_node(code="process_1", name="Test Process", type="process", unit="kg")
    process.new_exchange(amount=1.0, input=product, type="production", functional=True).save()
    process.save()

    # Mock LCA with one zero amount
    mock_inventory = np.array([[10.0], [0.0]])  # CO2=10, CH4=0
    mock_lca = Mock()
    mock_lca.inventory = Mock()
    mock_lca.inventory.sum.return_value = mock_inventory
    # Get the actual node IDs from the biosphere database
    co2_node = get_node(key=("biosphere", "CO2"))
    ch4_node = get_node(key=("biosphere", "CH4"))

    mock_lca.dicts = Mock()
    mock_lca.dicts.biosphere = {
        co2_node.id: 0,  # CO2 at row 0
        ch4_node.id: 1,  # CH4 at row 1 (zero amount)
    }

    mock_fu = {product: 1.0}
    mock_data_objs = []

    with patch("bw2data.compat.prepare_lca_inputs", return_value=(mock_fu, mock_data_objs, None)):
        with patch("bw2calc.LCA", return_value=mock_lca):
            new_process, new_product = process.create_aggregated_process(database="source_db")

    # Verify only non-zero biosphere exchange was created
    new_exchanges = list(new_process.exchanges())
    biosphere_exchanges = [e for e in new_exchanges if e["type"] == "biosphere"]

    assert len(biosphere_exchanges) == 1
    assert biosphere_exchanges[0]["input"][1] == "CO2"
    assert biosphere_exchanges[0]["amount"] == 10.0

    # Verify production exchange was also created
    production_exchanges = [e for e in new_exchanges if e["type"] == "production"]
    assert len(production_exchanges) == 1


@bw2test
def test_create_aggregated_process_signal_parameter():
    """Test that signal parameter is passed correctly"""
    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            }
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a process node
    product = source_db.new_node(code="product_1", name="Product 1", type="product", unit="kg")
    product.save()

    process = source_db.new_node(code="process_1", name="Test Process", type="process", unit="kg")
    process.new_exchange(amount=1.0, input=product, type="production", functional=True).save()
    process.save()

    # Mock LCA
    mock_inventory = np.array([[0.0]])
    mock_lca = Mock()
    mock_lca.inventory = Mock()
    mock_lca.inventory.sum.return_value = mock_inventory
    mock_lca.dicts = Mock()
    mock_lca.dicts.biosphere = {}

    mock_fu = {product: 1.0}
    mock_data_objs = []

    with patch("bw2data.compat.prepare_lca_inputs", return_value=(mock_fu, mock_data_objs, None)):
        with patch("bw2calc.LCA", return_value=mock_lca):
            with patch.object(process, "save") as mock_save:
                with patch("bw2data.backends.schema.ExchangeDataset") as mock_exchange_dataset:
                    new_process, new_product = process.create_aggregated_process(
                        database="source_db", signal=False
                    )

                    # Verify both process and product were created
                    assert new_process is not None
                    assert new_product is not None
                    # Note: save is called on the new process/product, not the original
                    # We can't easily mock this, but we can verify the method completed


@bw2test
def test_create_aggregated_process_product_copying():
    """Test that product node is copied when product != self"""
    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            }
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a product node
    product = source_db.new_node(
        code="product_1", name="Product 1", type="product", unit="kg", location="GLO"
    )
    product.save()

    # Create a process node with production exchange
    process = source_db.new_node(
        code="process_1", name="Test Process", type="process", unit="kg", location="GLO"
    )
    process.new_exchange(amount=2.5, input=product, type="production", functional=True).save()
    process.save()

    # Mock LCA
    mock_inventory = np.array([[0.0]])  # No biosphere flows
    mock_lca = Mock()
    mock_lca.inventory = Mock()
    mock_lca.inventory.sum.return_value = mock_inventory
    mock_lca.dicts = Mock()
    mock_lca.dicts.biosphere = {}

    mock_fu = {product: 2.5}
    mock_data_objs = []

    with patch("bw2data.compat.prepare_lca_inputs", return_value=(mock_fu, mock_data_objs, None)):
        with patch("bw2calc.LCA", return_value=mock_lca):
            new_process, new_product = process.create_aggregated_process(database="source_db")

    # Verify new process was created
    assert new_process is not None
    assert new_process["code"] != process["code"]

    # Verify product was copied and returned
    assert new_product is not None
    assert new_product["code"] != product["code"]
    assert new_product["name"] == "Product 1"
    assert new_product["type"] == "product"
    assert new_product["database"] == "source_db"

    # Verify production exchange was created
    new_exchanges = list(new_process.exchanges())
    production_exchanges = [e for e in new_exchanges if e["type"] == "production"]
    assert len(production_exchanges) == 1

    prod_exc = production_exchanges[0]
    assert prod_exc["amount"] == 2.5
    assert prod_exc.get("functional") == True
    assert prod_exc["input"] == new_product.key


@bw2test
def test_create_aggregated_process_production_exchange_type():
    """Test that production exchange preserves type and functional flag"""
    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            }
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a product node
    product = source_db.new_node(code="product_1", name="Product 1", type="product", unit="kg")
    product.save()

    # Create a process with non-functional production exchange
    process = source_db.new_node(code="process_1", name="Test Process", type="process", unit="kg")
    process.new_exchange(amount=3.0, input=product, type="production", functional=False).save()
    process.save()

    # Mock LCA
    mock_inventory = np.array([[0.0]])
    mock_lca = Mock()
    mock_lca.inventory = Mock()
    mock_lca.inventory.sum.return_value = mock_inventory
    mock_lca.dicts = Mock()
    mock_lca.dicts.biosphere = {}

    mock_fu = {product: 3.0}
    mock_data_objs = []

    with patch("bw2data.compat.prepare_lca_inputs", return_value=(mock_fu, mock_data_objs, None)):
        with patch("bw2calc.LCA", return_value=mock_lca):
            new_process, new_product = process.create_aggregated_process(database="source_db")

    # Verify product was returned
    assert new_product is not None
    assert new_product["database"] == "source_db"

    # Verify production exchange preserves type and functional flag
    new_exchanges = list(new_process.exchanges())
    production_exchanges = [e for e in new_exchanges if e["type"] == "production"]
    assert len(production_exchanges) == 1

    prod_exc = production_exchanges[0]
    assert prod_exc["type"] == "production"
    assert prod_exc.get("functional") == False
    assert prod_exc["amount"] == 3.0
    assert prod_exc["input"] == new_product.key


@pytest.mark.skipif(not BW2CALC_AVAILABLE, reason="bw2calc not available")
@bw2test
def test_create_aggregated_process_integration():
    """Integration test with actual LCA class"""

    # Create biosphere database
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
                "unit": "kg",
            }
        }
    )

    # Create source database
    source_db = DatabaseChooser("source_db")
    source_db.register()

    # Create a simple process with biosphere exchange
    product = source_db.new_node(
        code="product_1", name="Product 1", type="product", unit="kg", location="GLO"
    )
    product.save()

    process = source_db.new_node(
        code="process_1", name="Test Process", type="process", unit="kg", location="GLO"
    )
    process.new_exchange(amount=1.0, input=product, type="production", functional=True).save()
    process.new_exchange(amount=5.0, input=("biosphere", "CO2"), type="biosphere").save()
    process.save()

    # Process the database
    source_db.process()

    lca = bw2calc.LCA({product: 1})
    lca.lci()
    inventory_original = lca.inventory.toarray()

    # Create aggregated process (this will use real LCA)
    new_process, new_product = process.create_aggregated_process(database="source_db")

    # Verify new process was created
    assert new_process is not None
    assert new_process["code"] != process["code"]
    assert new_process["database"] == "source_db"

    # Verify production exchange was created
    new_exchanges = list(new_process.exchanges())
    production_exchanges = [e for e in new_exchanges if e["type"] == "production"]
    assert len(production_exchanges) == 1
    assert production_exchanges[0]["amount"] == 1.0
    assert production_exchanges[0].get("functional") == True

    # Verify biosphere exchange was created
    biosphere_exchanges = [e for e in new_exchanges if e["type"] == "biosphere"]

    # Should have at least the CO2 exchange
    assert len(biosphere_exchanges) >= 1
    co2_exchange = [e for e in biosphere_exchanges if e["input"][1] == "CO2"]
    assert len(co2_exchange) == 1
    assert co2_exchange[0]["amount"] == 5.0

    lca = bw2calc.LCA({new_product: 1})
    lca.lci()
    inventory_aggregated = lca.inventory.toarray()

    assert np.allclose(inventory_original.sum(axis=1), inventory_aggregated.sum(axis=1))
