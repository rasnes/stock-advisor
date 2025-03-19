package cmd

import (
	"fmt"
	"strings"

	"github.com/rasnes/stock-advisor/EtL/pipeline"
	"github.com/spf13/cobra"
)

func newBackfillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backfill [tickers]",
		Short: "Backfills historical data for specified tickers",
		Args:  cobra.MinimumNArgs(1), // Requires at least one ticker symbol
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

			tickers := strings.Split(strings.ToUpper(args[0]), ",") // Convert tickers to uppercase
			nSuccess, err := pipeline.BackfillEndOfDay(tickers)
			if err != nil {
				return fmt.Errorf("error backfilling tickers: %w", err)
			}
			log.Info(fmt.Sprintf("Backfilled %d tickers", nSuccess))
			return nil
		},
	}

	return cmd
}
