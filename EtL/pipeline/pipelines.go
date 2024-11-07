package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/constants"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/utils"
	"github.com/sourcegraph/conc/iter"
)

type Pipeline struct {
	DuckDB       *load.DuckDB
	TiingoClient *extract.TiingoClient
	Logger       *slog.Logger
	sqlDir       string
	timeProvider utils.TimeProvider
	InTest       bool
}

func NewPipeline(config *config.Config, logger *slog.Logger, timeProvider utils.TimeProvider) (*Pipeline, error) {
	db, err := load.NewDuckDB(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error creating DB database: %v", err)
	}

	httpClient, err := extract.NewTiingoClient(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error creating Tiingo HTTP client: %v", err)
	}

	// Determine SQL directory based on working directory
	sqlDir := "sql"
	if _, err := os.Stat(sqlDir); os.IsNotExist(err) {
		// If sql/ doesn't exist in current directory, try parent
		sqlDir = filepath.Join("..", "sql")
		if _, err := os.Stat(sqlDir); os.IsNotExist(err) {
			return nil, fmt.Errorf("cannot find SQL directory in either current or parent directory")
		}
	}

	return &Pipeline{
		DuckDB:       db,
		TiingoClient: httpClient,
		Logger:       logger,
		sqlDir:       sqlDir,
		timeProvider: timeProvider,
	}, nil
}

func (p *Pipeline) Close() {
	p.DuckDB.Close()
}

func (p *Pipeline) DailyEndOfDay() (int, error) {
	err := p.supportedTickers()
	if err != nil {
		return 0, fmt.Errorf("error getting supported tickers: %v", err)
	}

	lastTradingDay, err := p.TiingoClient.GetLastTradingDay()
	if err != nil {
		return 0, fmt.Errorf("error getting ticker data from last trading day: %v", err)
	}

	if err := p.DuckDB.LoadCSV(lastTradingDay, "last_trading_day", false); err != nil {
		return 0, fmt.Errorf("error loading last_trading_day into DB: %v", err)
	}

	if err := p.DuckDB.RunQueryFile(p.getSQLPath("insert__daily_adjusted.sql")); err != nil {
		return 0, fmt.Errorf("error inserting last trading day into daily_adjusted: %v", err)
	}

	res, err := p.DuckDB.GetQueryResultsFromFile(p.getSQLPath("query__selected_backfill.sql"))
	if err != nil {
		return 0, fmt.Errorf("error getting backfill results: %v", err)
	}

	tickers, ok := res["ticker"]
	if !ok {
		return 0, errors.New("ticker key not found in selected_backfill.sql results")
	}
	if len(tickers) == 0 {
		return 0, nil
	}

	nTickers, err := p.BackfillEndOfDay(tickers)
	if err != nil {
		return nTickers, fmt.Errorf("error backfilling tickers: %v", err)
	}

	return len(tickers), nil
}

func (p *Pipeline) selectedFundamentals() ([]string, error) {
	query := "select ticker from fundamentals.selected_fundamentals"
	if !p.InTest && os.Getenv("APP_ENV") != "prod" {
		query += " using sample 20"
	}
	query += " order by ticker;"

	res, err := p.DuckDB.GetQueryResults(query)
	if err != nil {
		return nil, fmt.Errorf("error getting fundamentals.selected_fundamentals results: %w", err)
	}

	tickers, ok := res["ticker"]
	if !ok {
		return nil, fmt.Errorf("ticker key not found in fundamentals.selected_fundamentals results")
	}
	if len(tickers) == 0 {
		return nil, fmt.Errorf("no tickers found in fundamentals.selected_fundamentals results")
	}

	return tickers, nil
}

type csvPerTicker func(ticker string) (csv []byte, err error)

func fetchCSVs(tickers []string, fetch csvPerTicker) ([]byte, []string, error) {
	// TODO: This part should probably have more tailored error handling
	// Like some HTTP error codes should be ignored (I might not have access).
	// BUT: it seems the API sends 400 Bad Request with body: None if no access,
	// which is the same as if the request were incorrect. Not optimal.
	// UPDATE: with 20+ years of historical data avaiable, one gets 200 OK with body: None
	// if data does not exists.
	// Remaining question: what is the HTTP code on 3 year subscription and requesting >3 years?
	// If it is still 200 but with body: None, I should probably just default to query data from 1995-01-01.

	mapper := iter.Mapper[string, []byte]{
		MaxGoroutines: 20,
	}

	// Map over tickers concurrently, fetching CSV data for each
	csvs, err := mapper.MapErr(tickers, func(ticker *string) ([]byte, error) {
		body, err := fetch(*ticker)
		if err != nil {
			return nil, fmt.Errorf("error fetching data for ticker %s: %w", *ticker, err)
		}

		// Handle empty responses by returning nil
		if string(body) == "None" {
			return nil, nil
		}

		csv, err := load.AddTickerColumn(body, *ticker)
		if err != nil {
			return nil, fmt.Errorf("error adding ticker column to CSV for ticker %s: %w", *ticker, err)
		}

		return csv, nil
	})

	// Track empty responses
	emptyResponses := make([]string, 0)
	validCSVs := make([][]byte, 0)

	// Process results, separating valid CSVs and empty responses
	for i, csv := range csvs {
		if csv == nil {
			emptyResponses = append(emptyResponses, tickers[i])
		} else {
			validCSVs = append(validCSVs, csv)
		}
	}

	// If we got an error during fetching, return it
	if err != nil {
		return nil, emptyResponses, err
	}

	// Concatenate valid CSVs
	finalCsv, err := load.ConcatCSVs(validCSVs)
	if err != nil {
		return nil, emptyResponses, fmt.Errorf("error concatenating CSVs: %w", err)
	}

	return finalCsv, emptyResponses, nil
}

// fetchFundamentalsData handles fetching and loading fundamentals data (daily or statements)
// for the specified tickers into DuckDB. If batchSize > 0, processes tickers in batches
// to manage memory usage.
func (p *Pipeline) fetchFundamentalsData(tickers []string, half bool, fetchFn csvPerTicker, tableName string, batchSize int) (int, error) {
	// Make sure we have the latest supported tickers
	err := p.supportedTickers()
	if err != nil {
		return 0, fmt.Errorf("error getting supported tickers: %v", err)
	}

	// Make sure we have the latest fundamentals metadata
	_, err = p.UpdateMetadata()
	if err != nil {
		return 0, fmt.Errorf("error updating metadata: %v", err)
	}

	if len(tickers) == 0 {
		tickersFromQuery, err := p.selectedFundamentals()
		if err != nil {
			return 0, fmt.Errorf("error getting selected fundamentals: %w", err)
		}

		// Below is a simple workaround for Tiingo's 10k requests per hour.
		// In Github Actions two cron jobs are scheduled one hour apart, to make sure we can fetch data for all tickers.
		// Take the modulo of the current hour to determine which half of the tickers to process.
		// This is a simple way to split the tickers into two halves, each of which could be scheduled on separate clock hours.
		if half {
			tickersFromQuery = utils.HalfOfSlice(
				tickersFromQuery,
				p.timeProvider.Now().Hour()%2 == 0,
			)
		}

		tickers = tickersFromQuery
	}

	upperCaseTickers := make([]string, 0)
	for _, ticker := range tickers {
		upperCaseTickers = append(upperCaseTickers, strings.ToUpper(ticker))
	}

	// Parse table name for logging
	dataType := strings.TrimPrefix(tableName, "fundamentals.")
	dataType = strings.ReplaceAll(dataType, "_", " ")

	totalEmptyResponses := make([]string, 0)
	totalProcessed := 0

	// Process all tickers at once if batchSize is 0
	if batchSize == 0 {
		finalCsv, emptyResponses, err := fetchCSVs(upperCaseTickers, fetchFn)
		if err != nil {
			return 0, fmt.Errorf("error fetching %s data: %w", dataType, err)
		}

		if err := p.DuckDB.LoadCSV(finalCsv, tableName, true); err != nil {
			return 0, fmt.Errorf("error loading %s data to DB: %w", dataType, err)
		}
		totalEmptyResponses = emptyResponses
		totalProcessed = len(upperCaseTickers) - len(emptyResponses)
	} else {
		// Process tickers in batches
		for i := 0; i < len(upperCaseTickers); i += batchSize {
			end := i + batchSize
			if end > len(upperCaseTickers) {
				end = len(upperCaseTickers)
			}
			batch := upperCaseTickers[i:end]

			finalCsv, emptyResponses, err := fetchCSVs(batch, fetchFn)
			if err != nil {
				return totalProcessed, fmt.Errorf("error fetching %s data for batch %d-%d: %w", dataType, i, end-1, err)
			}

			if err := p.DuckDB.LoadCSV(finalCsv, tableName, true); err != nil {
				return totalProcessed, fmt.Errorf("error loading %s data to DB for batch %d-%d: %w", dataType, i, end-1, err)
			}

			totalEmptyResponses = append(totalEmptyResponses, emptyResponses...)
			batchProcessed := len(batch) - len(emptyResponses)
			totalProcessed += batchProcessed

			p.Logger.Info(fmt.Sprintf("Successfully processed batch of %s data", dataType),
				"batch", fmt.Sprintf("%d-%d", i, end-1),
				"processed", batchProcessed,
				"empty_responses", len(emptyResponses))
		}
	}

	p.Logger.Info(fmt.Sprintf("Total number of empty responses: %d", len(totalEmptyResponses)))

	return totalProcessed, nil
}

// filterOutSkippedTickers removes any tickers that should be skipped from the input slice
// The comparison is case insensitive
func filterOutSkippedTickers(tickers []string, skipTickers []string) []string {
	// Convert skip tickers to uppercase for case-insensitive comparison
	upperSkipTickers := make([]string, len(skipTickers))
	for i, t := range skipTickers {
		upperSkipTickers[i] = strings.ToUpper(t)
	}
	
	return slices.DeleteFunc(tickers, func(ticker string) bool {
		return slices.Contains(upperSkipTickers, strings.ToUpper(ticker))
	})
}

func (p *Pipeline) DailyFundamentals(tickers []string, half bool, batchSize int, skipTickers []string) (int, error) {
	if len(skipTickers) > 0 {
		tickers = filterOutSkippedTickers(tickers, skipTickers)
	}
	return p.fetchFundamentalsData(tickers, half, p.TiingoClient.GetDailyFundamentals, "fundamentals.daily", batchSize)
}

func (p *Pipeline) Statements(tickers []string, half bool, batchSize int, skipTickers []string) (int, error) {
	if len(skipTickers) > 0 {
		tickers = filterOutSkippedTickers(tickers, skipTickers)
	}
	return p.fetchFundamentalsData(tickers, half, p.TiingoClient.GetStatements, "fundamentals.statements", batchSize)
}

func (p *Pipeline) UpdateMetadata() (int, error) {
	err := p.supportedTickers()
	if err != nil {
		return 0, fmt.Errorf("error getting supported tickers: %v", err)
	}

	// Get fundamentals metadata for all tickers from Tiingo API
	metadata, err := p.TiingoClient.GetMeta("")
	if err != nil {
		return 0, fmt.Errorf("error fetching metadata from Tiingo: %w", err)
	}

	insertMetaFile := p.getSQLPath("insert__fundamentals_meta.sql")
	templateContent, err := os.ReadFile(insertMetaFile)
	if err != nil {
		return 0, fmt.Errorf("error reading %s file: %w", insertMetaFile, err)
	}

	sqlParams := map[string]any{
		"CsvFile": constants.TmpCSVFile,
	}

	// Load metadata into DuckDB
	res, err := p.DuckDB.LoadCSVWithQuery(metadata, string(templateContent), sqlParams)
	if err != nil {
		return 0, fmt.Errorf("error loading metadata into DB: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

func (p *Pipeline) BackfillEndOfDay(tickers []string) (int, error) {
	var errorList []error
	for i, ticker := range tickers {
		history, err := p.TiingoClient.GetHistory(ticker)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("error fetching history for ticker %s: %w", ticker, err))
			continue
		}

		historyWithTicker, err := load.AddTickerColumn(history, ticker)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("error adding ticker column to history for ticker %s: %w", ticker, err))
			continue
		}

		if err := p.DuckDB.LoadCSV(historyWithTicker, "daily_adjusted", true); err != nil {
			errorList = append(errorList, fmt.Errorf("error loading history to DB for ticker %s: %w", ticker, err))
			continue
		}

		if i > 0 && i%20 == 0 {
			if len(errorList) > 0 {
				p.Logger.Info(fmt.Sprintf("Successfully backfilled %d tickers; failed on %d tickers", i-len(errorList), len(errorList)))
			} else {
				p.Logger.Info(fmt.Sprintf("Successfully backfilled %d tickers", i))
			}
		}
	}

	if len(errorList) > 0 {
		return len(tickers) - len(errorList), errors.Join(errorList...)
	}

	return len(tickers), nil
}

// Add this helper method
func (p *Pipeline) getSQLPath(filename string) string {
	return filepath.Join(p.sqlDir, filename)
}

func (p *Pipeline) supportedTickers() error {
	zipSupportedTickers, err := p.TiingoClient.GetSupportedTickers()
	if err != nil {
		return fmt.Errorf("error getting supported_tickers.zip: %v", err)
	}

	csvSupportedTickers, err := extract.UnzipSingleCSV(zipSupportedTickers)
	if err != nil {
		return fmt.Errorf("error unzipping supported_tickers.zip: %v", err)
	}

	if err := p.DuckDB.LoadCSV(csvSupportedTickers, "supported_tickers", false); err != nil {
		return fmt.Errorf("error loading supported_tickers.csv into DB: %v", err)
	}

	return nil
}
