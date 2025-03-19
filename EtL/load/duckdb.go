package load

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"text/template"

	_ "github.com/marcboeker/go-duckdb/v2"
	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/rasnes/stock-advisor/EtL/config"
	"github.com/rasnes/stock-advisor/EtL/constants"
)

type DuckDB struct {
	Logger    *slog.Logger
	DB        *sql.DB
	Connector *duckdb.Connector
	Conn      driver.Conn
	Appender  *duckdb.Appender
	DBType    string
}

func NewDuckDB(config *config.Config, logger *slog.Logger) (*DuckDB, error) {
	var path string
	var dbType string
	if strings.HasPrefix(config.DuckDB.Path, "md:") {
		motherduckToken := os.Getenv("MOTHERDUCK_TOKEN")
		if motherduckToken == "" {
			return nil, fmt.Errorf("MOTHERDUCK_TOKEN env variable is not set")
		}
		path = fmt.Sprintf("%s?motherduck_token=%s", config.DuckDB.Path, motherduckToken)
		dbType = ":md:"
	} else if config.DuckDB.Path == "" || config.DuckDB.Path == ":memory:" {
		path = ""
		dbType = ":memory:"
	} else {
		path = config.DuckDB.Path
		dbType = path
	}

	var connInitFn func(driver.ExecerContext) error
	if len(config.DuckDB.ConnInitFnQueries) == 0 {
		connInitFn = nil
	} else {
		connInitFn = func(exec driver.ExecerContext) error {
			for _, path := range config.DuckDB.ConnInitFnQueries {
				query, err := readQuery(path)
				if err != nil {
					return err
				}

				// Execute the query read from the file
				_, err = exec.ExecContext(context.Background(), string(query), nil)
				if err != nil {
					return fmt.Errorf("failed to execute query from file %s: %w", path, err)
				}
			}
			return nil
		}
		logger.Debug(fmt.Sprintf("Connection initialization queries: %v", config.DuckDB.ConnInitFnQueries))
	}

	connector, err := duckdb.NewConnector(path, connInitFn)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)

	switch dbType {
	case ":memory:":
		logger.Info("Connected to DuckDB in-memory database")
	case ":md:":
		logger.Info("Connected to MotherDuck database")
	default:
		logger.Info(fmt.Sprintf("Connected to local DuckDB database at %s", dbType))
	}

	return &DuckDB{
		Logger:    logger,
		DB:        db,
		Connector: connector,
		DBType:    dbType,
	}, nil
}

func readQuery(path string) ([]byte, error) {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	// Read the content of the file
	query, err := io.ReadAll(file)
	if err != nil {
		file.Close() // Ensure the file is closed if reading fails
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	// Close the file after reading its content
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("failed to close file %s: %w", path, err)
	}
	return query, nil
}

func (db *DuckDB) Close() {
	db.DB.Close()
	db.Connector.Close()
}

// LoadCSVWithQuery loads CSV data using a templated SQL query.
// The query template should use {{.CsvFile}} where the temporary CSV filename should be inserted.
func (db *DuckDB) LoadCSVWithQuery(csv []byte, queryTemplate string, params map[string]any) (sql.Result, error) {
	// Create a temporary file
	tmpFile, err := createTmpFile(csv)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Add the temporary file path to the template parameters
	if params == nil {
		params = make(map[string]any)
	}
	params["CsvFile"] = tmpFile.Name()

	// Parse and execute the template
	tmpl, err := template.New("sql").Parse(queryTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query template: %w", err)
	}

	var queryBuffer bytes.Buffer
	if err := tmpl.Execute(&queryBuffer, params); err != nil {
		return nil, fmt.Errorf("failed to execute query template: %w", err)
	}

	res, err := db.DB.ExecContext(context.Background(), queryBuffer.String())
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return res, nil
}

// LoadCSV loads CSV data into a table in DuckDB
// If insert is true, 'insert or replace' semantics are used,
// else the 'copy' command is used to load the data (which truncates the table).
func (db *DuckDB) LoadCSV(csv []byte, table string, insert bool) error {
	// Create a temporary file
	tmpFile, err := createTmpFile(csv)
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	if err := db.LoadTmpFile(tmpFile, table, insert); err != nil {
		return err
	}

	return nil
}

// LoadTmpFile loads a temporary file into a table in DuckDB
// If insert is true, 'insert or replace' semantics are used,
// else the write-truncate semantics are used.
func (db *DuckDB) LoadTmpFile(tmpFile *os.File, table string, insert bool) error {
	// Use the COPY statement or INSERT OR REPLACE to read the data from the temporary file into DuckDB
	var query string
	if insert {
		query = fmt.Sprintf("INSERT OR REPLACE INTO %s SELECT * FROM read_csv('%s', delim=',', quote='\"', escape='\"', header=true);", table, tmpFile.Name())
	} else {
		query = fmt.Sprintf("TRUNCATE TABLE %s; COPY %s FROM '%s' (FORMAT CSV, DELIMITER ',', QUOTE '\"', ESCAPE '\"', HEADER);", table, table, tmpFile.Name())
	}

	db.Logger.Debug("Executing DuckDB query", "query", query)

	if _, err := db.DB.ExecContext(context.Background(), query); err != nil {
		return fmt.Errorf("failed to execute COPY or INSERT OR REPLACE INTO statement: %w", err)
	}

	return nil
}

func createTmpFile(csv []byte) (*os.File, error) {
	// Validate CSV content
	if len(csv) == 0 {
		return nil, fmt.Errorf("received empty CSV data")
	}

	// Check for "None%" response which indicates no data available
	if string(csv) == "None%" {
		return nil, fmt.Errorf("received 'None%%' response from API, indicating no data available")
	}

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", constants.TmpCSVFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}

	// Write the CSV data to the temporary file
	if _, err := tmpFile.Write(csv); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("failed to write to temporary file: %w", err)
	}

	// Close the file to flush the data
	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temporary file: %w", err)
	}

	return tmpFile, nil
}

func (db *DuckDB) RunQuery(query string) error {
	_, err := db.DB.ExecContext(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	return nil
}

func (db *DuckDB) RunQueryFile(path string) error {
	query, err := readQuery(path)
	if err != nil {
		return err
	}

	return db.RunQuery(string(query))
}

func (db *DuckDB) GetQueryResultsFromFile(path string) (map[string][]string, error) {
	query, err := readQuery(path)
	if err != nil {
		return nil, err
	}

	return db.GetQueryResults(string(query))
}

// GetQueryResults executes a query and returns the results as a map of column names to slices of values
func (db *DuckDB) GetQueryResults(query string) (map[string][]string, error) {
	// Execute the query
	rows, err := db.DB.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Initialize a map to hold slices for each column
	results := make(map[string][]string)
	for _, col := range columns {
		results[col] = []string{}
	}

	// Iterate over the rows
	for rows.Next() {
		// Create a slice to hold the column values
		values := make([]interface{}, len(columns))
		// Create a slice of pointers to the column values
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Append the values to the corresponding slices in the results map
		for i, col := range columns {
			// Convert the value to a string
			valueStr := fmt.Sprintf("%v", values[i])
			results[col] = append(results[col], valueStr)
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	return results, nil
}
