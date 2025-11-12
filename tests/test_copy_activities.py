import pytest

from bw2data.database import DatabaseChooser
from bw2data.errors import UnknownObject
from bw2data.tests import bw2test


@bw2test
def test_copy_activities_basic():
    """Test basic copying of a single activity with exchanges"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    # Create source activity with exchanges
    source_db.write(
        {
            ("source_db", "A"): {
                "name": "Activity A",
                "type": "process",
                "exchanges": [
                    {"input": ("source_db", "A"), "amount": 1.0, "type": "production"},
                    {"input": ("source_db", "B"), "amount": 2.0, "type": "technosphere"},
                ],
            },
            ("source_db", "B"): {"name": "Activity B", "type": "process"},
        }
    )

    activity_a = source_db.get("A")

    # Copy activity
    result = source_db.copy_activities([activity_a], "target_db")

    assert len(result) == 1
    new_activity = result[0]
    assert new_activity["database"] == "target_db"
    assert new_activity["code"] == activity_a["code"]
    assert new_activity["name"] == "Activity A"

    # Check exchanges were copied
    new_exchanges = list(new_activity.exchanges())
    assert len(new_exchanges) == 2

    # Check that exchange pointing to copied activity (A) now points to new database
    production_exc = [e for e in new_exchanges if e["type"] == "production"][0]
    assert production_exc["input"] == ("target_db", "A")
    assert production_exc["output"] == ("target_db", "A")

    # Check that exchange pointing to non-copied activity (B) still points to source database
    techno_exc = [e for e in new_exchanges if e["type"] == "technosphere"][0]
    assert techno_exc["input"] == ("source_db", "B")
    assert techno_exc["output"] == ("target_db", "A")

    # Verify original activity's exchanges are preserved in source database
    activity_a = source_db.get("A")
    original_exchanges = list(activity_a.exchanges())
    assert len(original_exchanges) == 2
    original_production = [e for e in original_exchanges if e["type"] == "production"][0]
    assert original_production["input"] == ("source_db", "A")
    assert original_production["output"] == ("source_db", "A")
    original_techno = [e for e in original_exchanges if e["type"] == "technosphere"][0]
    assert original_techno["input"] == ("source_db", "B")
    assert original_techno["output"] == ("source_db", "A")


@bw2test
def test_copy_activities_multiple():
    """Test copying multiple activities with edges between them"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    # Create activities with exchanges between them
    source_db.write(
        {
            ("source_db", "A"): {
                "name": "Activity A",
                "type": "process",
                "exchanges": [
                    {"input": ("source_db", "A"), "amount": 1.0, "type": "production"},
                    {"input": ("source_db", "B"), "amount": 2.0, "type": "technosphere"},
                ],
            },
            ("source_db", "B"): {
                "name": "Activity B",
                "type": "process",
                "exchanges": [
                    {"input": ("source_db", "B"), "amount": 1.0, "type": "production"},
                    {"input": ("source_db", "A"), "amount": 1.5, "type": "technosphere"},
                    {"input": ("source_db", "C"), "amount": 3.0, "type": "technosphere"},
                ],
            },
            ("source_db", "C"): {"name": "Activity C", "type": "process"},
        }
    )

    activity_a = source_db.get("A")
    activity_b = source_db.get("B")

    # Copy both activities
    result = source_db.copy_activities([activity_a, activity_b], "target_db")

    assert len(result) == 2

    # Check that edges between copied activities point to new database
    new_a = [a for a in result if a["code"] == "A"][0]
    new_b = [a for a in result if a["code"] == "B"][0]

    # Activity A's exchange to B should point to new database
    a_exchanges = list(new_a.exchanges())
    a_to_b = [e for e in a_exchanges if e["input"][1] == "B"][0]
    assert a_to_b["input"] == ("target_db", "B")

    # Activity B's exchange to A should point to new database
    b_exchanges = list(new_b.exchanges())
    b_to_a = [e for e in b_exchanges if e["input"][1] == "A"][0]
    assert b_to_a["input"] == ("target_db", "A")

    # Activity B's exchange to C (not copied) should point to source database
    b_to_c = [e for e in b_exchanges if e["input"][1] == "C"][0]
    assert b_to_c["input"] == ("source_db", "C")

    # Verify original activities' exchanges are preserved in source database
    original_a_exchanges = list(activity_a.exchanges())
    assert len(original_a_exchanges) == 2
    original_a_to_b = [e for e in original_a_exchanges if e["input"][1] == "B"][0]
    assert original_a_to_b["input"] == ("source_db", "B")
    assert original_a_to_b["output"] == ("source_db", "A")

    original_b_exchanges = list(activity_b.exchanges())
    assert len(original_b_exchanges) == 3
    original_b_to_a = [e for e in original_b_exchanges if e["input"][1] == "A"][0]
    assert original_b_to_a["input"] == ("source_db", "A")
    assert original_b_to_a["output"] == ("source_db", "B")
    original_b_to_c = [e for e in original_b_exchanges if e["input"][1] == "C"][0]
    assert original_b_to_c["input"] == ("source_db", "C")
    assert original_b_to_c["output"] == ("source_db", "B")


@bw2test
def test_copy_activities_functional_edge_to_product():
    """Test that product nodes connected via functional edges are automatically copied"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    # Create process with functional edge to product, and also reference to another product without functional edge
    source_db.write(
        {
            ("source_db", "process_A"): {
                "name": "Process A",
                "type": "process",
                "exchanges": [
                    {
                        "input": ("source_db", "product_X"),
                        "amount": 1.0,
                        "type": "production",
                        "functional": True,
                    },
                    {"input": ("source_db", "product_Y"), "amount": 2.0, "type": "production"},
                    {"input": ("source_db", "process_A"), "amount": 1.0, "type": "production"},
                ],
            },
            ("source_db", "product_X"): {
                "name": "Product X",
                "type": "product",
            },
            ("source_db", "product_Y"): {
                "name": "Product Y",
                "type": "product",
            },
        }
    )

    process_a = source_db.get("process_A")

    # Copy process - only product_X should be automatically copied (has functional edge)
    result = source_db.copy_activities([process_a], "target_db")

    # Should return both the process and product_X (product_Y should NOT be copied)
    assert len(result) == 2
    result_codes = [a["code"] for a in result]
    assert "process_A" in result_codes
    assert "product_X" in result_codes
    assert "product_Y" not in result_codes

    # Check that product_X was copied to target database
    target_product_x = target_db.get("product_X")
    assert target_product_x is not None
    assert target_product_x["database"] == "target_db"
    assert target_product_x["name"] == "Product X"
    assert target_product_x["type"] == "product"

    # Check that product_Y was NOT copied to target database
    with pytest.raises(UnknownObject):
        target_db.get("product_Y")

    assert source_db.get("product_Y")

    # Check that functional edge in copied process points to copied product
    new_process = [a for a in result if a["code"] == "process_A"][0]
    new_exchanges = list(new_process.exchanges())
    functional_exc = [e for e in new_exchanges if e.get("functional")][0]
    assert functional_exc["input"] == ("target_db", "product_X")
    assert functional_exc["type"] == "production"

    # Check that non-functional edge to product_Y still points to source database
    techno_exc = [e for e in new_exchanges if e["input"][1] == "product_Y"][0]
    assert techno_exc["input"] == ("source_db", "product_Y")
    assert techno_exc.get("functional") is None

    # Verify original process's exchanges are preserved in source database
    original_exchanges = list(process_a.exchanges())
    assert len(original_exchanges) == 3
    original_functional = [e for e in original_exchanges if e.get("functional")][0]
    assert original_functional["input"] == ("source_db", "product_X")
    assert original_functional["output"] == ("source_db", "process_A")


@bw2test
def test_copy_activities_no_exchanges():
    """Test copying activity with no exchanges"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    source_db.write(
        {
            ("source_db", "A"): {
                "name": "Activity A",
                "type": "process",
            }
        }
    )

    activity_a = source_db.get("A")

    result = source_db.copy_activities([activity_a], "target_db")

    assert len(result) == 1
    new_activity = result[0]
    assert new_activity["database"] == "target_db"
    assert new_activity["name"] == "Activity A"

    # Should have no exchanges
    new_exchanges = list(new_activity.exchanges())
    assert len(new_exchanges) == 0


@bw2test
def test_copy_activities_biosphere_exchanges():
    """Test copying activity with biosphere exchanges"""
    from bw2data import Database

    # Create biosphere database first
    biosphere_db = Database("biosphere")
    biosphere_db.write(
        {
            ("biosphere", "CO2"): {
                "name": "Carbon dioxide",
                "type": "emission",
            }
        }
    )

    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    source_db.write(
        {
            ("source_db", "A"): {
                "name": "Activity A",
                "type": "process",
                "exchanges": [
                    {"input": ("source_db", "A"), "amount": 1.0, "type": "production"},
                    {"input": ("biosphere", "CO2"), "amount": 10.0, "type": "biosphere"},
                ],
            },
        }
    )

    activity_a = source_db.get("A")

    result = source_db.copy_activities([activity_a], "target_db")

    assert len(result) == 1
    new_activity = result[0]

    # Biosphere exchange should still point to biosphere database
    new_exchanges = list(new_activity.exchanges())
    biosphere_exc = [e for e in new_exchanges if e["type"] == "biosphere"][0]
    assert biosphere_exc["input"] == ("biosphere", "CO2")

    # Verify original activity's exchanges are preserved in source database
    original_exchanges = list(activity_a.exchanges())
    assert len(original_exchanges) == 2
    original_biosphere = [e for e in original_exchanges if e["type"] == "biosphere"][0]
    assert original_biosphere["input"] == ("biosphere", "CO2")
    assert original_biosphere["output"] == ("source_db", "A")


@bw2test
def test_copy_activities_target_database_not_exists():
    """Test error when target database doesn't exist"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    source_db.write(
        {
            ("source_db", "A"): {"name": "Activity A", "type": "process"},
        }
    )

    activity_a = source_db.get("A")

    with pytest.raises(ValueError, match="Target database 'nonexistent' does not exist"):
        source_db.copy_activities([activity_a], "nonexistent")


@bw2test
def test_copy_activities_target_same_as_source():
    """Test error when target database is same as source"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    source_db.write(
        {
            ("source_db", "A"): {"name": "Activity A", "type": "process"},
        }
    )

    activity_a = source_db.get("A")

    with pytest.raises(
        ValueError, match="Target database 'source_db' must be different from the source database"
    ):
        source_db.copy_activities([activity_a], "source_db")


@bw2test
def test_copy_activities_from_target_database():
    """Test error when activities to be copied are from the target database"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    # Create activity in source database
    source_db.write(
        {
            ("source_db", "A"): {"name": "Activity A", "type": "process"},
        }
    )

    # Create activity in target database
    target_db.write(
        {
            ("target_db", "B"): {"name": "Activity B", "type": "process"},
        }
    )

    activity_a = source_db.get("A")
    activity_b = target_db.get("B")

    # Try to copy activity from target database - should raise error
    with pytest.raises(ValueError, match="Input activities or associated products can't"):
        source_db.copy_activities([activity_b], "target_db")

    # Try to copy mix of activities from source and target - should raise error
    with pytest.raises(ValueError, match="Input activities or associated products can't"):
        source_db.copy_activities([activity_a, activity_b], "target_db")


@bw2test
def test_copy_activities_non_process_no_product_copy():
    """Test that non-process activities don't trigger product node copying"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    source_db.write(
        {
            ("source_db", "product_A"): {
                "name": "Product A",
                "type": "product",
                "exchanges": [
                    {
                        "input": ("source_db", "product_B"),
                        "amount": 1.0,
                        "type": "processwithreferenceproduct",
                    },
                ],
            },
            ("source_db", "product_B"): {
                "name": "Product B",
                "type": "product",
            },
        }
    )

    product_a = source_db.get("product_A")

    result = source_db.copy_activities([product_a], "target_db")

    assert len(result) == 1
    # Product B should NOT be copied because product_A is not a process
    with pytest.raises(UnknownObject):
        target_db.get("product_B")


@bw2test
def test_copy_activities_multiple_processes_same_product():
    """Test that product node is only copied once even if multiple processes reference it"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    source_db.write(
        {
            ("source_db", "process_A"): {
                "name": "Process A",
                "type": "process",
                "exchanges": [
                    {
                        "input": ("source_db", "product_X"),
                        "amount": 1.0,
                        "type": "production",
                        "functional": True,
                    },
                ],
            },
            ("source_db", "process_B"): {
                "name": "Process B",
                "type": "process",
                "exchanges": [
                    {
                        "input": ("source_db", "product_X"),
                        "amount": 1.0,
                        "type": "production",
                        "functional": True,
                    },
                ],
            },
            ("source_db", "product_X"): {
                "name": "Product X",
                "type": "product",
            },
        }
    )

    process_a = source_db.get("process_A")
    process_b = source_db.get("process_B")

    # Copy both processes
    result = source_db.copy_activities([process_a, process_b], "target_db")

    # Should return both processes and the product (product is automatically copied)
    assert len(result) == 3
    result_codes = [a["code"] for a in result]
    assert "process_A" in result_codes
    assert "process_B" in result_codes
    assert "product_X" in result_codes

    # Product should be copied only once
    target_product = target_db.get("product_X")
    assert target_product is not None

    # Both processes should have functional edges pointing to the same copied product
    new_a = [a for a in result if a["code"] == "process_A"][0]
    new_b = [a for a in result if a["code"] == "process_B"][0]

    # Verify product is in the result
    new_product = [a for a in result if a["code"] == "product_X"][0]
    assert new_product["database"] == "target_db"

    a_exc = list(new_a.exchanges())[0]
    b_exc = list(new_b.exchanges())[0]

    assert a_exc["input"] == ("target_db", "product_X")
    assert b_exc["input"] == ("target_db", "product_X")


@bw2test
def test_copy_activities_multiple_with_products_and_references():
    """Test copying multiple activities with multiple products to copy, and references that shouldn't be copied"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    # Create multiple processes with functional edges to products
    # Also include activities and products that are referenced but shouldn't be copied
    source_db.write(
        {
            ("source_db", "process_A"): {
                "name": "Process A",
                "type": "process",
                "exchanges": [
                    {
                        "input": ("source_db", "product_X"),
                        "amount": 1.0,
                        "type": "production",
                        "functional": True,
                    },
                    {"input": ("source_db", "process_B"), "amount": 1.0, "type": "technosphere"},
                    {"input": ("source_db", "process_C"), "amount": 2.0, "type": "technosphere"},
                    {"input": ("source_db", "product_Z"), "amount": 0.5, "type": "technosphere"},
                ],
            },
            ("source_db", "process_B"): {
                "name": "Process B",
                "type": "process",
                "exchanges": [
                    {
                        "input": ("source_db", "product_Y"),
                        "amount": 1.0,
                        "type": "production",
                        "functional": True,
                    },
                    {"input": ("source_db", "process_A"), "amount": 1.5, "type": "technosphere"},
                    {"input": ("source_db", "process_D"), "amount": 3.0, "type": "technosphere"},
                ],
            },
            ("source_db", "product_X"): {
                "name": "Product X",
                "type": "product",
            },
            ("source_db", "product_Y"): {
                "name": "Product Y",
                "type": "product",
            },
            ("source_db", "product_Z"): {
                "name": "Product Z",
                "type": "product",
            },
            ("source_db", "process_C"): {
                "name": "Process C",
                "type": "process",
            },
            ("source_db", "process_D"): {
                "name": "Process D",
                "type": "process",
            },
        }
    )

    process_a = source_db.get("process_A")
    process_b = source_db.get("process_B")

    # Copy both processes - product_X and product_Y should be automatically copied
    # product_Z, process_C, and process_D should NOT be copied (no functional edges or not in input list)
    result = source_db.copy_activities([process_a, process_b], "target_db")

    # Should return both processes and the two products (product_X and product_Y)
    assert len(result) == 4
    result_codes = [a["code"] for a in result]
    assert "process_A" in result_codes
    assert "process_B" in result_codes
    assert "product_X" in result_codes
    assert "product_Y" in result_codes
    assert "product_Z" not in result_codes

    # Check that product_X and product_Y were copied to target database
    target_product_x = target_db.get("product_X")
    assert target_product_x is not None
    assert target_product_x["database"] == "target_db"
    assert target_product_x["name"] == "Product X"

    target_product_y = target_db.get("product_Y")
    assert target_product_y is not None
    assert target_product_y["database"] == "target_db"
    assert target_product_y["name"] == "Product Y"

    # Check that product_Z was NOT copied (no functional edge)
    with pytest.raises(UnknownObject):
        target_db.get("product_Z")

    # Check that process_C and process_D were NOT copied (not in input list)
    with pytest.raises(UnknownObject):
        target_db.get("process_C")
    with pytest.raises(UnknownObject):
        target_db.get("process_D")

    # Check edge resolution in copied activities
    new_a = [a for a in result if a["code"] == "process_A"][0]
    new_b = [a for a in result if a["code"] == "process_B"][0]

    a_exchanges = list(new_a.exchanges())
    b_exchanges = list(new_b.exchanges())

    # Process A's functional edge to product_X should point to target database
    a_to_product_x = [e for e in a_exchanges if e.get("functional")][0]
    assert a_to_product_x["input"] == ("target_db", "product_X")

    # Process A's edge to process_B should point to target database (both were copied)
    a_to_b = [e for e in a_exchanges if e["input"][1] == "process_B"][0]
    assert a_to_b["input"] == ("target_db", "process_B")

    # Process A's edge to process_C should point to source database (not copied)
    a_to_c = [e for e in a_exchanges if e["input"][1] == "process_C"][0]
    assert a_to_c["input"] == ("source_db", "process_C")

    # Process A's edge to product_Z should point to source database (not copied, no functional edge)
    a_to_product_z = [e for e in a_exchanges if e["input"][1] == "product_Z"][0]
    assert a_to_product_z["input"] == ("source_db", "product_Z")
    assert a_to_product_z.get("functional") is None

    # Process B's functional edge to product_Y should point to target database
    b_to_product_y = [e for e in b_exchanges if e.get("functional")][0]
    assert b_to_product_y["input"] == ("target_db", "product_Y")

    # Process B's edge to process_A should point to target database (both were copied)
    b_to_a = [e for e in b_exchanges if e["input"][1] == "process_A"][0]
    assert b_to_a["input"] == ("target_db", "process_A")

    # Process B's edge to process_D should point to source database (not copied)
    b_to_d = [e for e in b_exchanges if e["input"][1] == "process_D"][0]
    assert b_to_d["input"] == ("source_db", "process_D")

    # Verify original activities' exchanges are preserved in source database
    original_a_exchanges = list(process_a.exchanges())
    assert len(original_a_exchanges) == 4
    original_a_functional = [e for e in original_a_exchanges if e.get("functional")][0]
    assert original_a_functional["input"] == ("source_db", "product_X")
    assert original_a_functional["output"] == ("source_db", "process_A")

    original_b_exchanges = list(process_b.exchanges())
    assert len(original_b_exchanges) == 3
    original_b_functional = [e for e in original_b_exchanges if e.get("functional")][0]
    assert original_b_functional["input"] == ("source_db", "product_Y")
    assert original_b_functional["output"] == ("source_db", "process_B")


@bw2test
def test_copy_activities_processwithreferenceproduct():
    """Test copying activities with type processwithreferenceproduct and verify edge behavior"""
    source_db = DatabaseChooser("source_db")
    source_db.register()

    target_db = DatabaseChooser("target_db")
    target_db.register()

    # Create activities with type processwithreferenceproduct
    source_db.write(
        {
            ("source_db", "A"): {
                "name": "Activity A",
                "type": "processwithreferenceproduct",
                "exchanges": [
                    {"input": ("source_db", "A"), "amount": 1.0, "type": "production"},
                    {"input": ("source_db", "B"), "amount": 2.0, "type": "technosphere"},
                    {
                        "input": ("source_db", "product_X"),
                        "amount": 1.0,
                        "type": "production",
                        "functional": True,
                    },
                ],
            },
            ("source_db", "B"): {
                "name": "Activity B",
                "type": "processwithreferenceproduct",
                "exchanges": [
                    {"input": ("source_db", "B"), "amount": 1.0, "type": "production"},
                    {"input": ("source_db", "A"), "amount": 1.5, "type": "technosphere"},
                    {"input": ("source_db", "C"), "amount": 3.0, "type": "technosphere"},
                ],
            },
            ("source_db", "C"): {
                "name": "Activity C",
                "type": "processwithreferenceproduct",
            },
            ("source_db", "product_X"): {
                "name": "Product X",
                "type": "product",
            },
        }
    )

    activity_a = source_db.get("A")
    activity_b = source_db.get("B")

    # Copy both activities
    result = source_db.copy_activities([activity_a, activity_b], "target_db")

    # Should return both activities and product_X (automatically copied due to functional edge)
    assert len(result) == 3
    result_codes = [a["code"] for a in result]
    assert "A" in result_codes
    assert "B" in result_codes
    assert "product_X" in result_codes

    # Check that edges between copied activities point to new database
    new_a = [a for a in result if a["code"] == "A"][0]
    new_b = [a for a in result if a["code"] == "B"][0]

    # Activity A's exchange to B should point to new database
    a_exchanges = list(new_a.exchanges())
    a_to_b = [e for e in a_exchanges if e["input"][1] == "B"][0]
    assert a_to_b["input"] == ("target_db", "B")
    assert a_to_b["output"] == ("target_db", "A")

    # Activity A's functional edge to product_X should point to new database
    a_to_product_x = [e for e in a_exchanges if e.get("functional")][0]
    assert a_to_product_x["input"] == ("target_db", "product_X")
    assert a_to_product_x["output"] == ("target_db", "A")

    # Activity B's exchange to A should point to new database
    b_exchanges = list(new_b.exchanges())
    b_to_a = [e for e in b_exchanges if e["input"][1] == "A"][0]
    assert b_to_a["input"] == ("target_db", "A")
    assert b_to_a["output"] == ("target_db", "B")

    # Activity B's exchange to C (not copied) should point to source database
    b_to_c = [e for e in b_exchanges if e["input"][1] == "C"][0]
    assert b_to_c["input"] == ("source_db", "C")
    assert b_to_c["output"] == ("target_db", "B")

    # Verify original activities' exchanges are preserved in source database
    original_a_exchanges = list(activity_a.exchanges())
    assert len(original_a_exchanges) == 3
    original_a_to_b = [e for e in original_a_exchanges if e["input"][1] == "B"][0]
    assert original_a_to_b["input"] == ("source_db", "B")
    assert original_a_to_b["output"] == ("source_db", "A")
    original_a_functional = [e for e in original_a_exchanges if e.get("functional")][0]
    assert original_a_functional["input"] == ("source_db", "product_X")
    assert original_a_functional["output"] == ("source_db", "A")

    original_b_exchanges = list(activity_b.exchanges())
    assert len(original_b_exchanges) == 3
    original_b_to_a = [e for e in original_b_exchanges if e["input"][1] == "A"][0]
    assert original_b_to_a["input"] == ("source_db", "A")
    assert original_b_to_a["output"] == ("source_db", "B")
    original_b_to_c = [e for e in original_b_exchanges if e["input"][1] == "C"][0]
    assert original_b_to_c["input"] == ("source_db", "C")
    assert original_b_to_c["output"] == ("source_db", "B")
