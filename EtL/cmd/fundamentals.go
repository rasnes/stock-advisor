package cmd

import (
	"fmt"
	"strings"

	"github.com/rasnes/stock-advisor/EtL/pipeline"
	"github.com/rasnes/stock-advisor/EtL/utils"
	"github.com/spf13/cobra"
)

var fundamentalsCmd = &cobra.Command{
	Use:   "fundamentals",
	Short: "Manage fundamentals data operations",
}

func newFundamentalsDailyCmd() *cobra.Command {
	var (
		tickers        string
		skipTickers    string
		halfOnly       bool
		dailyBatchSize int
		skipExisting   bool
		lookback       int
	)

	cmd := &cobra.Command{
		Use:   "daily [--tickers TICKER1,TICKER2,...] [--skipTickers TICKER1,TICKER2,...] [--halfOnly] [--lookback DAYS] [--batchSize SIZE] [--skipExisting]",
		Short: "Updates daily fundamentals data for selected tickers",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Validate that halfOnly is only used when tickers is not provided
			if halfOnly && tickers != "" {
				return fmt.Errorf("--halfOnly can only be used when --tickers is not provided")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, log, err := initializeConfigAndLogger()
			if err != nil {
				return err
			}

			pipeline, err := pipeline.NewPipeline(cfg, log, utils.RealTimeProvider{})
			if err != nil {
				return fmt.Errorf("error creating pipeline: %w", err)
			}
			defer pipeline.Close()

			var tickerSlice []string
			if tickers != "" {
				tickerSlice = strings.Split(strings.ToUpper(tickers), ",")
			}

			var skipTickerSlice []string
			if skipTickers != "" {
				skipTickerSlice = strings.Split(skipTickers, ",")
			}

			rowsAffected, err := pipeline.DailyFundamentals(tickerSlice, halfOnly, dailyBatchSize, skipTickerSlice, skipExisting, lookback)
			if err != nil {
				return fmt.Errorf("error updating daily fundamentals: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated daily fundamentals for %d tickers", rowsAffected))

			return nil
		},
	}

	cmd.Flags().StringVar(&tickers, "tickers", "", "Comma-separated list of tickers (e.g., AAPL,MSFT,GOOGL)")
	cmd.Flags().StringVar(&skipTickers, "skipTickers", "", "Comma-separated list of tickers to skip (e.g., BAD1,BAD2)")
	cmd.Flags().BoolVar(&halfOnly, "halfOnly", false, "Process only half of the tickers based on current hour (even=first half, odd=second half)")
	cmd.Flags().IntVar(&dailyBatchSize, "batchSize", 0, "Process tickers in batches of this size (0 means process all at once)")
	cmd.Flags().BoolVar(&skipExisting, "skipExisting", false, "Skip tickers that already exist in the database")
	cmd.Flags().IntVar(&lookback, "lookback", 0, "Number of days to look back for updates (0 means no filter)")
	return cmd
}

func newMetadataCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "metadata",
		Short: "Updates fundamentals metadata for all tickers",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, log, err := initializeConfigAndLogger()
			if err != nil {
				return err
			}

			pipeline, err := pipeline.NewPipeline(cfg, log, nil)
			if err != nil {
				return fmt.Errorf("error creating pipeline: %w", err)
			}
			defer pipeline.Close()

			rowsAffected, err := pipeline.UpdateMetadata()
			if err != nil {
				return fmt.Errorf("error updating metadata: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated metadata for %d tickers", rowsAffected))

			return nil
		},
	}
}

func newStatementsCmd() *cobra.Command {
	var (
		tickers             string
		skipTickers         string
		halfOnly            bool
		statementsBatchSize int
		skipExisting        bool
		lookback            int
	)

	cmd := &cobra.Command{
		Use:   "statements [--tickers TICKER1,TICKER2,...] [--skipTickers TICKER1,TICKER2,...] [--halfOnly] [--lookback DAYS] [--batchSize SIZE] [--skipExisting]",
		Short: "Updates financial statements data for selected tickers",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Validate that halfOnly is only used when tickers is not provided
			if halfOnly && tickers != "" {
				return fmt.Errorf("--halfOnly can only be used when --tickers is not provided")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, log, err := initializeConfigAndLogger()
			if err != nil {
				return err
			}

			pipeline, err := pipeline.NewPipeline(cfg, log, utils.RealTimeProvider{})
			if err != nil {
				return fmt.Errorf("error creating pipeline: %w", err)
			}
			defer pipeline.Close()

			var tickerSlice []string
			if tickers != "" {
				tickerSlice = strings.Split(strings.ToUpper(tickers), ",")
			}

			var skipTickerSlice []string
			if skipTickers != "" {
				skipTickerSlice = strings.Split(skipTickers, ",")
			}

			rowsAffected, err := pipeline.Statements(tickerSlice, halfOnly, statementsBatchSize, skipTickerSlice, skipExisting, lookback)
			if err != nil {
				return fmt.Errorf("error updating statements: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated statements for %d tickers", rowsAffected))

			return nil
		},
	}

	cmd.Flags().StringVar(&tickers, "tickers", "", "Comma-separated list of tickers (e.g., AAPL,MSFT,GOOGL)")
	cmd.Flags().StringVar(&skipTickers, "skipTickers", "", "Comma-separated list of tickers to skip (e.g., BAD1,BAD2)")
	cmd.Flags().BoolVar(&halfOnly, "halfOnly", false, "Process only half of the tickers based on current hour (even=first half, odd=second half)")
	cmd.Flags().IntVar(&statementsBatchSize, "batchSize", 0, "Process tickers in batches of this size (0 means process all at once)")
	cmd.Flags().BoolVar(&skipExisting, "skipExisting", false, "Skip tickers that already exist in the database")
	cmd.Flags().IntVar(&lookback, "lookback", 0, "Number of days to look back for updates (0 means no filter)")
	return cmd
}

func init() {
	fundamentalsCmd.AddCommand(newMetadataCmd())
	fundamentalsCmd.AddCommand(newFundamentalsDailyCmd())
	fundamentalsCmd.AddCommand(newStatementsCmd())
}
