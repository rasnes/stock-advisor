package load

import (
	"github.com/rasnes/stock-advisor/EtL/config"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"os"
	"testing"
)

func setupTestDB(t *testing.T) *DuckDB {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := &config.Config{
		DuckDB: config.DuckDBConfig{
			Path: ":memory:",
		},
	}

	db, err := NewDuckDB(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create DuckDB instance: %v", err)
	}

	return db
}

func TestNewDuckDB(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	assert.NotNil(t, db.DB)
}

func TestLoadCSVWithQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a test table
	createTableQuery := "CREATE TABLE test (id INTEGER, name STRING);"
	err := db.RunQuery(createTableQuery)
	assert.NoError(t, err)

	// Test data
	csvData := []byte("id,name\n1,Alice\n2,Bob")
	queryTemplate := "COPY test FROM '{{.CsvFile}}' (FORMAT CSV, HEADER);"
	params := map[string]any{}

	// Execute the templated query
	res, err := db.LoadCSVWithQuery(csvData, queryTemplate, params)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	// Verify the data was loaded correctly
	results, err := db.GetQueryResults("SELECT * FROM test ORDER BY id;")
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{
		"id":   {"1", "2"},
		"name": {"Alice", "Bob"},
	}, results)
}

func TestLoadCSVWithEmptyData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test with empty CSV data
	err := db.LoadCSV([]byte{}, "test", false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received empty CSV data")
}

func TestLoadCSVWithNoneResponse(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test with "None%" response
	err := db.LoadCSV([]byte("None%"), "test", false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received 'None%' response from API")
}

func TestLoadCSV(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a test table
	createTableQuery := "CREATE TABLE test (id INTEGER, name STRING);"
	err := db.RunQuery(createTableQuery)
	assert.NoError(t, err)

	// Load CSV data into the test table
	csvData := []byte("id,name\n1,Alice\n2,Bob")
	err = db.LoadCSV(csvData, "test", false)
	assert.NoError(t, err)

	// Verify the data was loaded correctly
	query := "SELECT * FROM test;"
	results, err := db.GetQueryResults(query)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{
		"id":   {"1", "2"},
		"name": {"Alice", "Bob"},
	}, results)
}

func TestRunQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a test table
	createTableQuery := "CREATE TABLE test (id INTEGER, name STRING);"
	err := db.RunQuery(createTableQuery)
	assert.NoError(t, err)

	// Insert data into the test table
	insertQuery := "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');"
	err = db.RunQuery(insertQuery)
	assert.NoError(t, err)

	// Verify the data was inserted correctly
	query := "SELECT * FROM test;"
	results, err := db.GetQueryResults(query)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{
		"id":   {"1", "2"},
		"name": {"Alice", "Bob"},
	}, results)
}

func TestRunQueryFile(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a temporary query file
	query := "CREATE TABLE test (id INTEGER, name STRING);"
	tmpFile, err := os.CreateTemp("", "query.sql")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(query)
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	// Run the query from the file
	err = db.RunQueryFile(tmpFile.Name())
	assert.NoError(t, err)

	// Verify the table was created
	query = "SELECT * FROM test;"
	results, err := db.GetQueryResults(query)
	assert.NoError(t, err)

	// Check that the columns are present but no rows exist
	expectedResults := map[string][]string{
		"id":   {},
		"name": {},
	}
	assert.Equal(t, expectedResults, results)
}

func TestGetQueryResults(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a test table and insert data
	createTableQuery := "CREATE TABLE test (id INTEGER, name STRING);"
	err := db.RunQuery(createTableQuery)
	assert.NoError(t, err)

	insertQuery := "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');"
	err = db.RunQuery(insertQuery)
	assert.NoError(t, err)

	// Get query results
	query := "SELECT * FROM test;"
	results, err := db.GetQueryResults(query)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{
		"id":   {"1", "2"},
		"name": {"Alice", "Bob"},
	}, results)
}

func TestGetQueryResultsFromFile(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a test table and insert data
	createTableQuery := "CREATE TABLE test (id INTEGER, name STRING);"
	err := db.RunQuery(createTableQuery)
	assert.NoError(t, err)

	insertQuery := "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');"
	err = db.RunQuery(insertQuery)
	assert.NoError(t, err)

	// Create a temporary query file
	query := "SELECT * FROM test;"
	tmpFile, err := os.CreateTemp("", "query.sql")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(query)
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	// Get query results from the file
	results, err := db.GetQueryResultsFromFile(tmpFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{
		"id":   {"1", "2"},
		"name": {"Alice", "Bob"},
	}, results)
}
