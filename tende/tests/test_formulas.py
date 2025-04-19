import pytest
from fastapi import status
from uuid import uuid4

def test_create_formula(test_client, sample_formula, sample_ingredient):
    """Test creating a new formula."""
    # First create an ingredient
    ingredient_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = ingredient_response.json()["id"]
    
    # Create formula with the ingredient
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {ingredient_id: 100.0}
    
    response = test_client.post("/formulas/", json=formula_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == formula_data["name"]
    assert data["description"] == formula_data["description"]
    assert data["ingredients"][ingredient_id] == 100.0

def test_create_formula_duplicate_name(test_client, sample_formula, sample_ingredient):
    """Test creating a formula with a duplicate name."""
    # Create an ingredient
    ingredient_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = ingredient_response.json()["id"]
    
    # Create first formula
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {ingredient_id: 100.0}
    test_client.post("/formulas/", json=formula_data)
    
    # Try to create another with the same name
    response = test_client.post("/formulas/", json=formula_data)
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "Database constraint violation" in response.json()["detail"]

def test_get_formulas(test_client, sample_formula, sample_ingredient):
    """Test retrieving a list of formulas."""
    # Create an ingredient and formula
    ingredient_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = ingredient_response.json()["id"]
    
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {ingredient_id: 100.0}
    test_client.post("/formulas/", json=formula_data)
    
    # Get formulas
    response = test_client.get("/formulas/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) > 0
    assert any(formula["name"] == formula_data["name"] for formula in data)

def test_get_formula_by_id(test_client, sample_formula, sample_ingredient):
    """Test retrieving a specific formula by ID."""
    # Create an ingredient and formula
    ingredient_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = ingredient_response.json()["id"]
    
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {ingredient_id: 100.0}
    create_response = test_client.post("/formulas/", json=formula_data)
    formula_id = create_response.json()["id"]
    
    # Get the formula
    response = test_client.get(f"/formulas/{formula_id}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == formula_id
    assert data["name"] == formula_data["name"]
    assert data["ingredients"][ingredient_id] == 100.0

def test_get_nonexistent_formula(test_client):
    """Test retrieving a non-existent formula."""
    response = test_client.get(f"/formulas/{uuid4()}")
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_update_formula(test_client, sample_formula, sample_ingredient):
    """Test updating a formula."""
    # Create an ingredient and formula
    ingredient_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = ingredient_response.json()["id"]
    
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {ingredient_id: 100.0}
    create_response = test_client.post("/formulas/", json=formula_data)
    formula_id = create_response.json()["id"]
    
    # Update the formula
    update_data = {
        "name": "Updated Formula",
        "description": "Updated description",
        "ingredients": {ingredient_id: 200.0}
    }
    response = test_client.patch(f"/formulas/{formula_id}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["description"] == update_data["description"]
    assert data["ingredients"][ingredient_id] == 200.0

def test_delete_formula(test_client, sample_formula, sample_ingredient):
    """Test deleting a formula."""
    # Create an ingredient and formula
    ingredient_response = test_client.post("/ingredients/", json=sample_ingredient)
    ingredient_id = ingredient_response.json()["id"]
    
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {ingredient_id: 100.0}
    create_response = test_client.post("/formulas/", json=formula_data)
    formula_id = create_response.json()["id"]
    
    # Delete the formula
    response = test_client.delete(f"/formulas/{formula_id}")
    assert response.status_code == status.HTTP_204_NO_CONTENT
    
    # Verify it's deleted
    get_response = test_client.get(f"/formulas/{formula_id}")
    assert get_response.status_code == status.HTTP_404_NOT_FOUND

def test_create_formula_with_nonexistent_ingredient(test_client, sample_formula):
    """Test creating a formula with a non-existent ingredient."""
    formula_data = sample_formula.copy()
    formula_data["ingredients"] = {str(uuid4()): 100.0}
    
    response = test_client.post("/formulas/", json=formula_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Invalid ingredient ID" in response.json()["detail"] 