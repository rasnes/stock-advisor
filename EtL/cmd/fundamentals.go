package cmd

import (
	"fmt"
	"os"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/pipeline"
	"github.com/spf13/cobra"
)

var fundamentalsCmd = &cobra.Command{
	Use:   "fundamentals",
	Short: "Manage fundamentals data operations",
}

func newFundamentalsDailyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "daily",
		Short: "Updates daily fundamentals data for selected tickers",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, log, err := initializeConfigAndLogger()
			if err != nil {
				return err
			}

			db, err := load.NewDuckDB(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating DB connection: %w", err)
			}
			defer db.Close()

			client, err := extract.NewTiingoClient(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating HTTP client: %w", err)
			}

			rowsAffected, err := pipeline.DailyFundamentals(db, client, log, "")
			if err != nil {
				return fmt.Errorf("error updating daily fundamentals: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated daily fundamentals for %d tickers", rowsAffected))

			return nil
		},
	}
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

			db, err := load.NewDuckDB(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating DB connection: %w", err)
			}
			defer db.Close()

			client, err := extract.NewTiingoClient(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating HTTP client: %w", err)
			}

			// Read the SQL template file
			sqlTemplate, err := os.ReadFile("../sql/insert__fundamentals_meta.sql")
			if err != nil {
				return fmt.Errorf("error reading SQL template file: %w", err)
			}

			rowsAffected, err := pipeline.UpdateMetadata(db, client, log, string(sqlTemplate))
			if err != nil {
				return fmt.Errorf("error updating metadata: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated metadata for %d tickers", rowsAffected))

			return nil
		},
	}
}

func init() {
	fundamentalsCmd.AddCommand(newMetadataCmd())
	fundamentalsCmd.AddCommand(newFundamentalsDailyCmd())
}
