package cmd

import (
	"fmt"

	"github.com/rasnes/stock-advisor/EtL/pipeline"
	"github.com/spf13/cobra"
)

func newDailyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "daily",
		Short: "Runs the daily end-of-day ETL pipeline",
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

			nTickers, err := pipeline.DailyEndOfDay()
			if err != nil {
				if nTickers > 0 {
					log.Error(fmt.Sprintf("Error running pipeline: %v. Backfilled %d tickers", err, nTickers))
				} else {
					log.Error(fmt.Sprintf("Error running pipeline: %v", err))
				}
				return err
			}
			log.Info(fmt.Sprintf("Batch job completed without errors. Backfilled %d tickers", nTickers))
			return nil
		},
	}
}
