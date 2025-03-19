package pipeline

import (
	"archive/zip"
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rasnes/stock-advisor/EtL/config"
	"github.com/rasnes/stock-advisor/EtL/utils"
	"github.com/stretchr/testify/assert"
)

func setupTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify token is present
		token := r.URL.Query().Get("token")
		if token == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "/docs/tiingo/daily/supported_tickers.zip":
			// Create a minimal zip file with supported tickers CSV
			w.Header().Set("Content-Type", "application/zip")
			csvContent := `ticker,exchange,assetType,priceCurrency,startDate,endDate
aapl,NASDAQ,Stock,USD,2018-08-22,2024-01-01
MSFT,NASDAQ,Stock,USD,1975-08-22,2024-01-01
ENRON,NASDAQ,Stock,USD,1990-08-22,2005-01-01
000001,SHE,Stock,CNY,2007-01-04,2024-01-01
TQQQ,NASDAQ,ETF,USD,2010-02-11,2024-01-01
ETFGONE,NYSE,ETF,USD,2010-02-11,2010-01-01
TSLA,NASDAQ,Stock,USD,2010-06-29,2024-01-01
AMZN,NASDAQ,Stock,USD,1997-05-15,2024-01-01
`
			_, _ = w.Write(createTestZip(csvContent))

		case "/tiingo/daily/prices":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `ticker,date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor
aapl,2024-01-01,191.5,192.0,190.5,191.0,1100000,191.5,192.0,190.5,191.0,1100000,0.0,1.0
msft,2024-01-01,192.5,193.0,192.0,192.2,1200000,192.5,193.0,192.0,192.2,1200000,0.0,1.0
tsla,2024-01-01,191.5,192.0,190.5,191.0,1100000,191.5,192.0,190.5,191.0,1100000,0.0,0.9
AMZN,2024-01-01,192.5,193.0,192.0,192.2,1200000,192.5,193.0,192.0,192.2,1200000,0.1,1.0
`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/daily/AMZN/prices":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,close,adjClose,adjVolume
2024-01-01,191.5,191.5,1100000
2023-01-01,130.5,130.5,1200000
2021-01-01,191.5,191.5,1100000
`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/daily/TSLA/prices":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,close,adjClose,adjVolume
2024-01-01,376.8,376.8,850000
2023-01-01,300.2,300.2,900000
2022-01-01,300.2,300.2,900000
`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/fundamentals/meta":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `permaTicker,ticker,name,isActive,isADR,sector,industry,sicCode,sicSector,sicIndustry,reportingCurrency,location,companyWebsite,secFilingWebsite,statementLastUpdated,dailyLastUpdated,dataProviderPermaTicker
US000000000038,aapl,Apple Inc,True,False,Technology,Consumer Electronics,3571,Manufacturing,Electronic Computers,usd,"California, USA",http://www.apple.com,https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193,2024-11-02 01:01:16,2024-11-05 02:10:59,199059
US000000000042,msft,Microsoft Corporation,True,False,Technology,Software Development,7372,Services,Software Development,usd,"Washington, USA",http://www.microsoft.com,https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000789019,2024-11-02 00:15:22,2024-11-05 02:15:33,199060
US000000000091,tsla,Tesla Inc,True,False,Consumer Cyclical,Auto Manufacturers,3711,Manufacturing,Motor Vehicles,usd,"Texas, USA",http://www.tesla.com,https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001318605,2024-11-01 23:45:11,2024-11-05 02:05:44,199061
CN000000000001,000001,Ping An Bank Co Ltd,True,False,Financial Services,Banks,6021,Finance,National Banks,cny,"Shenzhen, China",http://www.pingan.cn,http://www.szse.cn,2024-11-02 03:30:15,2024-11-05 04:22:18,199062
CN000000000002,600000,Shanghai Pudong Development Bank,True,False,Financial Services,Banks,6021,Finance,National Banks,cny,"Shanghai, China",http://www.spdb.com.cn,http://www.sse.com.cn,2024-11-02 03:15:44,2024-11-05 04:18:55,199063`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/fundamentals/AAPL/daily":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,marketCap,enterpriseVal,peRatio,pbRatio,trailingPEG1Y
2024-01-01,2500000000000.0,2550000000000.0,25.5,12.3,1.5
2024-01-02,2520000000000.0,2570000000000.0,25.7,12.4,1.52
2024-01-03,2480000000000.0,2530000000000.0,25.3,12.2,1.48`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/fundamentals/MSFT/daily":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,marketCap,enterpriseVal,peRatio,pbRatio,trailingPEG1Y
2024-01-01,3000000000000.0,3050000000000.0,32.5,15.8,1.8
2024-01-02,3020000000000.0,3070000000000.0,32.7,15.9,1.82
2024-01-03,2980000000000.0,3030000000000.0,32.3,15.7,1.78`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/fundamentals/TSLA/daily":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,marketCap,enterpriseVal,peRatio,pbRatio,trailingPEG1Y
2024-01-01,2000000000000.0,2050000000000.0,20.5,10.8,1.2
2024-01-02,2020000000000.0,2070000000000.0,20.7,10.9,1.22
2024-01-03,1980000000000.0,2030000000000.0,20.3,10.7,1.18`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/fundamentals/AAPL/statements":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,year,quarter,statementType,dataCode,value
2024-09-28,2024,4,balanceSheet,totalAssets,364980000000.0
2024-09-28,2024,4,balanceSheet,acctRec,66243000000.0
2024-09-28,2024,4,incomeStatement,revenue,94930000000.0
2024-09-28,2024,4,incomeStatement,netinc,14736000000.0
2024-09-28,2024,4,cashFlow,freeCashFlow,23903000000.0
2024-09-28,2024,4,cashFlow,capex,-2908000000.0
2024-09-28,2024,4,overview,roa,0.270226599025453
2024-06-30,2024,3,balanceSheet,totalAssets,355800000000.0
2024-06-30,2024,3,balanceSheet,acctRec,64100000000.0
2024-06-30,2024,3,incomeStatement,revenue,89100000000.0
2024-06-30,2024,3,incomeStatement,netinc,13800000000.0
2024-06-30,2024,3,cashFlow,freeCashFlow,22500000000.0
2024-06-30,2024,3,cashFlow,capex,-2700000000.0
2024-06-30,2024,3,overview,roa,0.265`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/fundamentals/MSFT/statements":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,year,quarter,statementType,dataCode,value
2024-09-28,2024,4,balanceSheet,totalAssets,450000000000.0
2024-09-28,2024,4,incomeStatement,revenue,105000000000.0
2024-09-28,2024,4,cashFlow,freeCashFlow,25000000000.0
2024-06-30,2024,3,balanceSheet,totalAssets,440000000000.0
2024-06-30,2024,3,incomeStatement,revenue,100000000000.0
2024-06-30,2024,3,cashFlow,freeCashFlow,24000000000.0`
			_, _ = w.Write([]byte(csvContent))
		case "/tiingo/fundamentals/TSLA/statements":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,year,quarter,statementType,dataCode,value
2024-09-28,2024,4,balanceSheet,totalAssets,120000000000.0
2024-09-28,2024,4,incomeStatement,revenue,23000000000.0
2024-09-28,2024,4,cashFlow,freeCashFlow,8000000000.0
2024-06-30,2024,3,balanceSheet,totalAssets,115000000000.0
2024-06-30,2024,3,incomeStatement,revenue,21000000000.0
2024-06-30,2024,3,cashFlow,freeCashFlow,7500000000.0`
			_, _ = w.Write([]byte(csvContent))

		case "/tiingo/fundamentals/NODAILY/daily":
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write([]byte("None"))

		case "/tiingo/fundamentals/NODATA/daily":
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write([]byte("None"))

		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("Not found"))
		}
	}))
}

// TODO: add test where columns in daily are randomised. The API says it may change order of columns.

func createTestZip(csvContent string) []byte {
	// Create a buffer to write our zip to
	buf := new(bytes.Buffer)

	// Create a new zip archive
	w := zip.NewWriter(buf)

	// Create a new file inside the zip
	f, err := w.Create("supported_tickers.csv")
	if err != nil {
		panic(err)
	}

	// Write the CSV content to the file
	_, err = f.Write([]byte(csvContent))
	if err != nil {
		panic(err)
	}

	// Close the zip writer
	err = w.Close()
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func setupTestConfig(t *testing.T) *config.Config {
	// Read the base config file
	baseConfig, err := os.Open("../config.base.yaml")
	assert.NoError(t, err)
	defer baseConfig.Close()

	// Create new config with base config and no env config
	cfg, err := config.NewConfig(baseConfig, nil, "test")
	assert.NoError(t, err)

	// Override the DuckDB path to use in-memory database
	cfg.DuckDB.Path = ":memory:"

	// Update SQL file paths for test environment
	var updatedQueries []string
	for _, query := range cfg.DuckDB.ConnInitFnQueries {
		// Handle both relative and absolute paths
		if strings.HasPrefix(query, "./sql/") {
			query = strings.TrimPrefix(query, "./")
		}
		if strings.HasPrefix(query, "../sql/") {
			query = strings.TrimPrefix(query, "../")
		}
		// Always use the parent directory in tests
		updatedQueries = append(updatedQueries, filepath.Join("..", query))
	}

	// Add all SQL files from the test directory
	testSQLFiles, err := filepath.Glob("../sql/test/*.sql")
	if err != nil {
		t.Fatalf("Failed to glob test SQL files: %v", err)
	}

	// Sort the files to ensure consistent ordering
	sort.Strings(testSQLFiles)

	cfg.DuckDB.ConnInitFnQueries = append(updatedQueries, testSQLFiles...)

	// Add required configuration for fundamentals statements
	cfg.Tiingo.Fundamentals.Statements.StartDate = "2024-01-01"
	cfg.Tiingo.Fundamentals.Daily.StartDate = "2024-01-01"

	return cfg
}

func setupTestPipeline(t *testing.T, server *httptest.Server, timeProvider utils.TimeProvider) (*Pipeline, func()) {
	// Setup environment
	os.Setenv("TIINGO_TOKEN", "test-token")

	// Setup logger
	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

	// Setup config
	cfg := setupTestConfig(t)

	// Create pipeline
	pipeline, err := NewPipeline(cfg, logger, timeProvider)
	assert.NoError(t, err)

	// Override the base URL to use our test server and set the time provider
	pipeline.TiingoClient.BaseURL = server.URL
	pipeline.TiingoClient.InTest = true
	pipeline.timeProvider = timeProvider
	pipeline.InTest = true

	// Cleanup function
	cleanup := func() {
		pipeline.Close()
		os.Unsetenv("TIINGO_TOKEN")
	}

	return pipeline, cleanup
}

func TestPipeline_UpdateMetadata(t *testing.T) {
	// Setup test server
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// Run the metadata update
	count, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)
	assert.Equal(t, 5, count, "Expected 5 rows to be inserted into fundamentals.meta")

	// Verify the data in DuckDB
	// First verify total count
	rowsTotal, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM fundamentals.meta;")
	assert.NoError(t, err)
	assert.Equal(t, []string{"5"}, rowsTotal["count"], "Expected 5 total rows in fundamentals.meta")

	// Then verify US tickers specifically through the view
	rowsUS, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM fundamentals.selected_fundamentals;")
	assert.NoError(t, err)
	assert.Equal(t, []string{"3"}, rowsUS["count"], "Expected 3 US tickers in selected_fundamentals view")

	// Verify specific fields for a known ticker
	appleData, err := pipeline.DuckDB.GetQueryResults(`
		SELECT permaTicker, name, sector, industry, location
		FROM fundamentals.meta
		WHERE ticker = 'aapl';
	`)
	assert.NoError(t, err)
	assert.Equal(t, []string{"US000000000038"}, appleData["permaTicker"])
	assert.Equal(t, []string{"Apple Inc"}, appleData["name"])
	assert.Equal(t, []string{"Technology"}, appleData["sector"])
	assert.Equal(t, []string{"Consumer Electronics"}, appleData["industry"])
	assert.Equal(t, []string{"California, USA"}, appleData["location"])
}

// MockTimeProvider implements TimeProvider for testing with fixed hour
type MockTimeProvider struct {
	hour int
}

func (m MockTimeProvider) Now() time.Time {
	return time.Date(2024, 1, 1, m.hour, 0, 0, 0, time.UTC)
}

func TestPipeline_DailyFundamentals(t *testing.T) {
	tests := []struct {
		name        string
		tickers     []string
		half        bool
		hour        int
		wantCount   int
		wantTickers []string
	}{
		{
			name:        "specific tickers - no half selection",
			tickers:     []string{"AAPL", "MSFT"},
			half:        false,
			hour:        14,
			wantCount:   2,
			wantTickers: []string{"AAPL", "MSFT"},
		},
		{
			name:        "even hour gets first half",
			tickers:     nil,
			half:        true,
			hour:        14, // 2pm
			wantCount:   1,
			wantTickers: []string{"AAPL"}, // First half of ["AAPL", "MSFT", "TSLA"]
		},
		{
			name:        "odd hour gets second half",
			tickers:     nil,
			half:        true,
			hour:        15, // 3pm
			wantCount:   2,
			wantTickers: []string{"MSFT", "TSLA"}, // Second half of ["AAPL", "MSFT", "TSLA"]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test server
			server := setupTestServer()
			defer server.Close()

			// Setup pipeline with mock time provider
			pipeline, cleanup := setupTestPipeline(t, server, &MockTimeProvider{hour: tt.hour})
			defer cleanup()

			// First populate meta table if we're testing automatic ticker selection
			if tt.tickers == nil {
				_, err := pipeline.UpdateMetadata()
				assert.NoError(t, err)
			}

			// Run the test
			count, err := pipeline.DailyFundamentals(tt.tickers, tt.half, 0, nil, false, 0)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCount, count)

			// Verify the correct tickers were processed
			rows, err := pipeline.DuckDB.GetQueryResults(`
                SELECT DISTINCT ticker
                FROM fundamentals.daily
                ORDER BY ticker;
            `)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantTickers, rows["ticker"])
		})
	}
}

func TestPipeline_Statements(t *testing.T) {
	tests := []struct {
		name        string
		tickers     []string
		half        bool
		hour        int
		wantCount   int
		wantTickers []string
	}{
		{
			name:        "specific tickers - no half selection",
			tickers:     []string{"MSFT"},
			half:        false,
			hour:        14,
			wantCount:   1,
			wantTickers: []string{"MSFT"},
		},
		{
			name:        "even hour gets first half",
			tickers:     nil,
			half:        true,
			hour:        14, // 2pm
			wantCount:   1,
			wantTickers: []string{"AAPL"}, // First half of ["AAPL", "MSFT", "TSLA"]
		},
		{
			name:        "odd hour gets second half",
			tickers:     nil,
			half:        true,
			hour:        15, // 3pm
			wantCount:   2,
			wantTickers: []string{"MSFT", "TSLA"}, // Second half of ["AAPL", "MSFT", "TSLA"]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test server
			server := setupTestServer()
			defer server.Close()

			// Setup pipeline with mock time provider
			pipeline, cleanup := setupTestPipeline(t, server, &MockTimeProvider{hour: tt.hour})
			defer cleanup()

			// First populate meta table if we're testing automatic ticker selection
			if tt.tickers == nil {
				_, err := pipeline.UpdateMetadata()
				assert.NoError(t, err)
			}

			// Run the test
			count, err := pipeline.Statements(tt.tickers, tt.half, 0, nil, false, 0)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCount, count)

			// Verify the correct tickers were processed
			rows, err := pipeline.DuckDB.GetQueryResults(`
                SELECT DISTINCT ticker
                FROM fundamentals.statements
                ORDER BY ticker;
            `)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantTickers, rows["ticker"])
		})
	}
}

func TestFilterOutSkippedTickers(t *testing.T) {
	tests := []struct {
		name        string
		tickers     []string
		skipTickers []string
		want        []string
	}{
		{
			name:        "no tickers to skip",
			tickers:     []string{"AAPL", "MSFT", "GOOGL"},
			skipTickers: []string{},
			want:        []string{"AAPL", "MSFT", "GOOGL"},
		},
		{
			name:        "skip one ticker",
			tickers:     []string{"AAPL", "MSFT", "GOOGL"},
			skipTickers: []string{"msft"}, // lowercase skip ticker
			want:        []string{"AAPL", "GOOGL"},
		},
		{
			name:        "skip multiple tickers - mixed case",
			tickers:     []string{"AAPL", "MSFT", "GOOGL", "TSLA"},
			skipTickers: []string{"msft", "TSla"}, // mixed case skip tickers
			want:        []string{"AAPL", "GOOGL"},
		},
		{
			name:        "skip non-existent ticker",
			tickers:     []string{"AAPL", "MSFT"},
			skipTickers: []string{"INVALID"},
			want:        []string{"AAPL", "MSFT"},
		},
		{
			name:        "empty input tickers",
			tickers:     []string{},
			skipTickers: []string{"MSFT"},
			want:        []string{},
		},
		{
			name:        "skip all tickers",
			tickers:     []string{"AAPL", "MSFT"},
			skipTickers: []string{"AAPL", "MSFT"},
			want:        []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterOutSkippedTickers(tt.tickers, tt.skipTickers)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPipeline_DailyFundamentals_BatchProcessing(t *testing.T) {
	// Setup test server
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// First populate meta table
	_, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)

	// Test with batch size of 2
	count, err := pipeline.DailyFundamentals([]string{"AAPL", "MSFT", "TSLA"}, false, 2, nil, false, 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify all data was loaded despite batching
	rows, err := pipeline.DuckDB.GetQueryResults(`
        SELECT DISTINCT ticker
        FROM fundamentals.daily
        ORDER BY ticker;
    `)
	assert.NoError(t, err)
	assert.Equal(t, []string{"AAPL", "MSFT", "TSLA"}, rows["ticker"])
}

func TestPipeline_DailyFundamentals_SkipTickers(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// First populate meta table
	_, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)

	// Test skipping specific tickers
	count, err := pipeline.DailyFundamentals(
		[]string{"AAPL", "MSFT", "TSLA"},
		false,
		0,
		[]string{"msft", "TSla"}, // Test case-insensitive skipping
		false,
		0,
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify only non-skipped ticker was processed
	rows, err := pipeline.DuckDB.GetQueryResults(`
        SELECT DISTINCT ticker
        FROM fundamentals.daily
        ORDER BY ticker;
    `)
	assert.NoError(t, err)
	assert.Equal(t, []string{"AAPL"}, rows["ticker"])
}

func TestPipeline_DailyFundamentals_SkipExisting(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// First populate meta table
	_, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)

	// First insertion
	_, err = pipeline.DailyFundamentals([]string{"AAPL"}, false, 0, nil, false, 0)
	assert.NoError(t, err)

	// Second insertion with skipExisting=true
	count, err := pipeline.DailyFundamentals(
		[]string{"AAPL", "MSFT", "TSLA"},
		false,
		0,
		nil,
		true,
		0,
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // Should only process MSFT and TSLA

	// Verify all tickers are present
	rows, err := pipeline.DuckDB.GetQueryResults(`
        SELECT DISTINCT ticker
        FROM fundamentals.daily
        ORDER BY ticker;
    `)
	assert.NoError(t, err)
	assert.Equal(t, []string{"AAPL", "MSFT", "TSLA"}, rows["ticker"])
}

func TestPipeline_DailyFundamentals_Lookback(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// First populate meta table
	_, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)

	// Test with lookback period
	count, err := pipeline.DailyFundamentals(nil, false, 0, nil, false, 20000)
	assert.NoError(t, err)
	assert.Greater(t, count, 0)

	// Verify data was loaded
	rows, err := pipeline.DuckDB.GetQueryResults(`
        SELECT DISTINCT ticker
        FROM fundamentals.daily
        ORDER BY ticker;
    `)
	assert.NoError(t, err)
	assert.NotEmpty(t, rows["ticker"])
}

func TestPipeline_Statements_BatchAndSkip(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// First populate meta table
	_, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)

	// Test combination of batch processing and skipping
	count, err := pipeline.Statements(
		[]string{"AAPL", "MSFT", "TSLA"},
		false,
		2,                // batch size
		[]string{"MSFT"}, // skip MSFT
		false,
		0,
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // Should process AAPL and TSLA in batches

	// Verify correct tickers were processed
	rows, err := pipeline.DuckDB.GetQueryResults(`
        SELECT DISTINCT ticker
        FROM fundamentals.statements
        ORDER BY ticker;
    `)
	assert.NoError(t, err)
	assert.Equal(t, []string{"AAPL", "TSLA"}, rows["ticker"])
}

func TestPipeline_DailyFundamentals_AllNoneResponses(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// First populate meta table
	_, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)

	// Test with tickers that will return "None"
	tickers := []string{"NODAILY", "NODATA"}
	count, err := pipeline.DailyFundamentals(tickers, false, 1, nil, false, 0) // batch size of 1 to ensure multiple requests
	assert.NoError(t, err)
	assert.Equal(t, 0, count) // Should process 0 tickers since all returned "None"

	// Verify no data was loaded
	rows, err := pipeline.DuckDB.GetQueryResults(`
        SELECT COUNT(*) as count
        FROM fundamentals.daily;
    `)
	assert.NoError(t, err)
	assert.Equal(t, []string{"0"}, rows["count"])

	// Verify we can still successfully process other tickers after receiving "None" responses
	count, err = pipeline.DailyFundamentals([]string{"AAPL"}, false, 0, nil, false, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify only AAPL data was loaded
	rows, err = pipeline.DuckDB.GetQueryResults(`
        SELECT DISTINCT ticker
        FROM fundamentals.daily
        ORDER BY ticker;
    `)
	assert.NoError(t, err)
	assert.Equal(t, []string{"AAPL"}, rows["ticker"])
}

func TestPipeline_Statements_SkipExistingAndLookback(t *testing.T) {
	server := setupTestServer()
	defer server.Close()

	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// First populate meta table
	_, err := pipeline.UpdateMetadata()
	assert.NoError(t, err)

	// First insertion
	_, err = pipeline.Statements([]string{"AAPL"}, false, 0, nil, false, 0)
	assert.NoError(t, err)

	// Second insertion with skipExisting and lookback
	count, err := pipeline.Statements(
		nil, // use selected_fundamentals
		false,
		0,
		nil,
		true,  // skip existing
		20000, // lookback days
	)
	assert.NoError(t, err)
	assert.Greater(t, count, 0)

	// Verify AAPL wasn't processed again
	rows, err := pipeline.DuckDB.GetQueryResults(`
        SELECT COUNT(*) as count
        FROM (
            SELECT ticker, date
            FROM fundamentals.statements
            WHERE ticker = 'AAPL'
            GROUP BY ticker, date
        );
    `)
	assert.NoError(t, err)
	assert.Equal(t, []string{"2"}, rows["count"]) // Should have 2 dates for AAPL (from test server response)
}

func TestPipeline_DailyEndOfDay(t *testing.T) {
	// Configure expected variables (see response from test server)
	expectedInitRowsLastTradingDay := 7
	expectedInitRowsDailyAdjusted := 6
	expectedPostRowsLastTradingDay := 4
	expectedPostRowsSelectedLastTradingDay := 4
	expectedPostRowsSelectedUSTickers := 6
	expectedBackfillRows := 4 // 4 in addition to the 2 with same date as in LastTradingDay

	// Setup test server
	server := setupTestServer()
	defer server.Close()

	// Setup pipeline with no time provider (not needed for this test)
	pipeline, cleanup := setupTestPipeline(t, server, nil)
	defer cleanup()

	// Asserting that existing mock data in the database is as expected
	rowsLastTradingDayPre, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM main.last_trading_day;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedInitRowsLastTradingDay)}, rowsLastTradingDayPre["count"], fmt.Sprintf("Expected %d rows in last_trading_day table", expectedInitRowsLastTradingDay))
	rowsDailyAdjustedPre, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM main.daily_adjusted;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedInitRowsDailyAdjusted)}, rowsDailyAdjustedPre["count"], fmt.Sprintf("Expected %d rows in daily_adjusted table", expectedInitRowsDailyAdjusted))

	// Override the base URL to use our test server
	pipeline.TiingoClient.BaseURL = server.URL
	pipeline.TiingoClient.InTest = true

	// Run the pipeline
	count, err := pipeline.DailyEndOfDay() // Count here is only if backfill happens, not if no backfills are needed.
	assert.NoError(t, err)
	nBackfills := 2 // We expect 2 backfills since TSLA and AMZN has divCash or splitFactor in non-normal values
	assert.Equal(t, nBackfills, count)

	// Verify the data in DuckDB
	// Verify selected_us_tickers table
	rowsSelectedUSTickers, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM main.selected_us_tickers;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsSelectedUSTickers)}, rowsSelectedUSTickers["count"], fmt.Sprintf("Expected %d rows in selected_us_tickers table", expectedPostRowsSelectedUSTickers))
	// Verify that the last_trading_day table has been overwritten
	rowsLastTradingDayPost, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM last_trading_day;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsLastTradingDay)}, rowsLastTradingDayPost["count"], fmt.Sprintf("Expected %d rows in last_trading_day table", expectedPostRowsLastTradingDay))
	// Verify that selected_last_trading_day view returns the expected number of rows
	rowsSelectedLastTradingDay, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM selected_last_trading_day;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsSelectedLastTradingDay)}, rowsSelectedLastTradingDay["count"], fmt.Sprintf("Expected %d rows in selected_last_trading_day view", expectedPostRowsSelectedLastTradingDay))
	// Verify that the daily_adjusted table has been updated with newly inserted rows
	rowsDailyAdjustedPost, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM daily_adjusted;")
	assert.NoError(t, err)
	expectedPostRowsDailyAdjusted := expectedInitRowsDailyAdjusted + expectedPostRowsSelectedLastTradingDay + expectedBackfillRows
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsDailyAdjusted)}, rowsDailyAdjustedPost["count"], fmt.Sprintf("Expected %d rows in daily_adjusted table", expectedPostRowsDailyAdjusted))
}
