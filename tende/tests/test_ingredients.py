import pytest
from fastapi import status
from uuid import uuid4

def test_create_ingredient(test_client, sample_ingredient):
    """Test creating a new ingredient."""
    response = test_client.post("/ingredients/", json=sample_ingredient)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == sample_ingredient["name"]
    assert data["unit"] == sample_ingredient["unit"]
    assert float(data["cost_per_unit"]) == sample_ingredient["cost_per_unit"]
    assert float(data["density"]) == sample_ingredient["density"]

def test_create_ingredient_duplicate_name(test_client, sample_ingredient):
    """Test creating an ingredient with a duplicate name."""
    # First create an ingredient
    test_client.post("/ingredients/", json=sample_ingredient)
    
    # Try to create another with the same name
    response = test_client.post("/ingredients/", json=sample_ingredient)
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "Database constraint violation" in response.json()["detail"]

def test_get_ingredients(test_client, sample_ingredient):
    """Test retrieving a list of ingredients."""
    # Create a test ingredient
    test_client.post("/ingredients/", json=sample_ingredient)
    
    # Get ingredients
    response = test_client.get("/ingredients/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) > 0
    assert any(ing["name"] == sample_ingredient["name"] for ing in data)

def test_get_ingredient_by_id(test_client, sample_ingredient):
    """Test retrieving a specific ingredient by ID."""
    # Create a test ingredient
    create_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = create_response.json()["id"]
    
    # Get the ingredient
    response = test_client.get(f"/ingredients/{ingredient_id}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == ingredient_id
    assert data["name"] == sample_ingredient["name"]

def test_get_nonexistent_ingredient(test_client):
    """Test retrieving a non-existent ingredient."""
    response = test_client.get(f"/ingredients/{uuid4()}")
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_update_ingredient(test_client, sample_ingredient):
    """Test updating an ingredient."""
    # Create a test ingredient
    create_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = create_response.json()["id"]
    
    # Update the ingredient
    update_data = {
        "name": "Updated Ingredient",
        "unit": "g",
        "cost_per_unit": 15.75,
        "density": 1.2
    }
    response = test_client.patch(f"/ingredients/{ingredient_id}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["unit"] == update_data["unit"]
    assert float(data["cost_per_unit"]) == update_data["cost_per_unit"]
    assert float(data["density"]) == update_data["density"]

def test_delete_ingredient(test_client, sample_ingredient):
    """Test deleting an ingredient."""
    # Create a test ingredient
    create_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = create_response.json()["id"]
    
    # Delete the ingredient
    response = test_client.delete(f"/ingredients/{ingredient_id}")
    assert response.status_code == status.HTTP_204_NO_CONTENT
    
    # Verify it's deleted
    get_response = test_client.get(f"/ingredients/{ingredient_id}")
    assert get_response.status_code == status.HTTP_404_NOT_FOUND

def test_delete_ingredient_used_in_formula(test_client, sample_ingredient, sample_formula):
    """Test deleting an ingredient that's used in a formula."""
    # Create an ingredient
    ingredient_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = ingredient_response.json()["id"]
    
    # Create a formula using the ingredient
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {ingredient_id: 100.0}
    test_client.post("/formulas/", json=formula_data)
    
    # Try to delete the ingredient
    response = test_client.delete(f"/ingredients/{ingredient_id}")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "used in formulas" in response.json()["detail"] 