package extract

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
	"github.com/rasnes/stock-advisor/EtL/config"
)

type TiingoClient struct {
	HTTPClient   *retryablehttp.Client
	Logger       *slog.Logger
	TiingoConfig *config.TiingoConfig
	tiingoToken  string
	BaseURL      string
	InTest       bool
}

func NewTiingoClient(config *config.Config, logger *slog.Logger) (*TiingoClient, error) {
	tiingoToken := os.Getenv("TIINGO_TOKEN")
	if tiingoToken == "" {
		return nil, fmt.Errorf("TIINGO_TOKEN env variable is not set")
	}

	client := &TiingoClient{
		HTTPClient:   retryablehttp.NewClient(),
		Logger:       logger,
		TiingoConfig: &config.Tiingo,
		tiingoToken:  tiingoToken,
		BaseURL:      "https://api.tiingo.com",
	}

	client.HTTPClient.RetryWaitMin = config.Extract.Backoff.RetryWaitMin
	client.HTTPClient.RetryWaitMax = config.Extract.Backoff.RetryWaitMax
	client.HTTPClient.RetryMax = config.Extract.Backoff.RetryMax
	client.HTTPClient.Logger = logger

	return client, nil
}

// GetSupportedTickers fetches the supported tickers from the Tiingo API and returns the zip file downloaded
func (c *TiingoClient) GetSupportedTickers() ([]byte, error) {
	var baseURL string
	if !c.InTest {
		baseURL = "https://apimedia.tiingo.com"
	} else {
		baseURL = c.BaseURL
	}

	url, err := c.addTiingoConfigToURL(
		c.TiingoConfig.Eod,
		fmt.Sprintf("%s/docs/tiingo/daily/supported_tickers.zip", baseURL),
		false,
	)
	if err != nil {
		return nil, err
	}
	return c.FetchData(url, "supported_tickers.zip")
}

// GetLastTradingDay fetches prices for all tickers on the last completed training day
func (c *TiingoClient) GetLastTradingDay() ([]byte, error) {
	url, err := c.addTiingoConfigToURL(
		c.TiingoConfig.Eod,
		fmt.Sprintf("%s/tiingo/daily/prices", c.BaseURL),
		false,
	)
	if err != nil {
		return nil, err
	}
	return c.FetchData(url, fmt.Sprintf("last_trading_day.%s", c.TiingoConfig.Eod.Format))
}

// GetHistory fetches the historical EoD prices for a ticker, from c.TiingoStartDate to the present
func (c *TiingoClient) GetHistory(ticker string) ([]byte, error) {
	url, err := c.addTiingoConfigToURL(
		c.TiingoConfig.Eod,
		fmt.Sprintf("%s/tiingo/daily/%s/prices", c.BaseURL, ticker),
		true,
	)
	if err != nil {
		return nil, err
	}
	return c.FetchData(url, fmt.Sprintf("history for ticker %s", ticker))
}

// GetStatements fetches the financial statements for a ticker
// https://www.tiingo.com/documentation/fundamentals section 2.6.3
func (c *TiingoClient) GetStatements(ticker string) ([]byte, error) {
	url, err := c.addTiingoConfigToURL(
		c.TiingoConfig.Fundamentals.Statements,
		fmt.Sprintf("%s/tiingo/fundamentals/%s/statements", c.BaseURL, ticker),
		true,
	)
	if err != nil {
		return nil, err
	}
	return c.FetchData(url, fmt.Sprintf("statements for ticker %s", ticker))
}

// GetMeta fetches the meta information for a ticker.
// `tickers` is a comma separated list of tickers, e.g. "AAPL,GOOGL"
// If `tickers` is zero value, it fetches the meta information for all tickers.
// https://www.tiingo.com/documentation/fundamentals section 2.6.5
func (c *TiingoClient) GetMeta(tickers string) ([]byte, error) {
	metaURL := fmt.Sprintf("%s/tiingo/fundamentals/meta", c.BaseURL)
	if tickers != "" {
		parsedURL, _ := url.Parse(metaURL)
		query := parsedURL.Query()
		query.Set("tickers", tickers)
		parsedURL.RawQuery = query.Encode()
		metaURL = parsedURL.String()
	}

	url, err := c.addTiingoConfigToURL(
		c.TiingoConfig.Fundamentals.Meta,
		metaURL,
		false,
	)
	if err != nil {
		return nil, err
	}
	return c.FetchData(url, fmt.Sprintf("meta.%s", c.TiingoConfig.Fundamentals.Meta.Format))
}

// GetDailyFundamentals fetches the daily fundamentals for a ticker
// https://www.tiingo.com/documentation/fundamentals section 2.6.4
func (c *TiingoClient) GetDailyFundamentals(ticker string) ([]byte, error) {
	url, err := c.addTiingoConfigToURL(
		c.TiingoConfig.Fundamentals.Daily,
		fmt.Sprintf("%s/tiingo/fundamentals/%s/daily", c.BaseURL, ticker),
		true,
	)
	if err != nil {
		return nil, err
	}

	return c.FetchData(url, fmt.Sprintf("daily fundamentals for ticker %s", ticker))
}

// FetchData handles the common logic of making the HTTP request and checking the response status
func (c *TiingoClient) FetchData(url, description string) ([]byte, error) {
	body, resp, err := c.get(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch the `%s` file, status: %s, body: %s", description, resp.Status, string(body))
	}

	return body, nil
}

// addTiingoConfigToURL adds the Tiingo token, format, startDate and columns to the URL
func (c *TiingoClient) addTiingoConfigToURL(apiConfig config.TiingoAPIConfig, rawURL string, history bool) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}

	query := parsedURL.Query()
	query.Set("token", c.tiingoToken)
	query.Set("format", apiConfig.Format)
	if apiConfig.Columns != "" {
		query.Set("columns", apiConfig.Columns)
	}
	if history {
		if apiConfig.StartDate == "" {
			return "", fmt.Errorf("startDate is required for historical data")
		}
		var startDate string
		if strings.Contains(apiConfig.StartDate, "today") {
			startDate, err = parseTodayString(apiConfig.StartDate)
			if err != nil {
				return "", fmt.Errorf("failed to parse startDate: %w", err)
			}
		} else {
			startDate = apiConfig.StartDate
		}

		query.Set("startDate", startDate)
	}
	parsedURL.RawQuery = query.Encode()

	return parsedURL.String(), nil
}

// get fetches the URL and returns the body and response
func (c *TiingoClient) get(url string) (body []byte, resp *http.Response, err error) {
	resp, err = c.HTTPClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return body, resp, nil
}

// parseTodayString converts a string in the format "today" or "today-<duration>" into an ISO 8601 date string.
// The duration part supports any valid time.ParseDuration format (e.g., "24h", "7h30m", "1h30m10s").
//
// Examples:
//   - "today" returns current date
//   - "today-24h" returns yesterday's date
//   - "today-168h" returns date from 7 days ago
//   - "today-30m" returns today's date (as it's less than a day)
//
// Returns:
//   - string: ISO 8601 formatted date (YYYY-MM-DD)
//   - error: if the input format is invalid or duration parsing fails
func parseTodayString(todayString string) (string, error) {
	// Handle the "today" case
	if todayString == "today" {
		return time.Now().Format("2006-01-02"), nil
	}

	// Split the string by "-"
	parts := strings.Split(todayString, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid today string format: %s", todayString)
	}

	if parts[0] != "today" {
		return "", fmt.Errorf("string must start with 'today': %s", todayString)
	}

	duration, err := time.ParseDuration(parts[1])
	if err != nil {
		return "", fmt.Errorf("failed to parse duration: %w", err)
	}

	today := time.Now().Add(-duration)
	return today.Format("2006-01-02"), nil
}
