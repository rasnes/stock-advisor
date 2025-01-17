package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

var docsCmd = &cobra.Command{
	Use:   "docs",
	Short: "Generate documentation for all commands",
	Long: `Generate comprehensive documentation for all ETL commands.
This will display the full usage information, including all subcommands and flags.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var w io.Writer = os.Stdout
		if len(args) > 0 {
			f, err := os.Create(args[0])
			if err != nil {
				return err
			}
			defer f.Close()
			w = f
		}

		return generateFullDocs(w)
	},
}

func generateFullDocs(w io.Writer) error {
	fmt.Fprintf(w, "# ETL CLI Documentation\n\n")

	// Document root command
	fmt.Fprintf(w, "## Overview\n\n")
	fmt.Fprintf(w, "%s\n\n", rootCmd.Long)

	// Document EOD commands
	fmt.Fprintf(w, "## End-of-Day (EOD) Commands\n\n")

	// Find the daily and backfill commands by name to ensure correct order
	var dailyCmd, backfillCmd *cobra.Command
	for _, cmd := range endOfDayCmd.Commands() {
		switch cmd.Name() {
		case "daily":
			dailyCmd = cmd
		case "backfill":
			backfillCmd = cmd
		}
	}

	if dailyCmd != nil {
		fmt.Fprintf(w, "### eod daily\n\n")
		fmt.Fprintf(w, "```\n%s\n```\n\n", dailyCmd.UsageString())
	}

	if backfillCmd != nil {
		fmt.Fprintf(w, "### eod backfill\n\n")
		fmt.Fprintf(w, "```\n%s\n```\n\n", backfillCmd.UsageString())
	}

	// Document Fundamentals commands
	fmt.Fprintf(w, "## Fundamentals Commands\n\n")
	for _, subcmd := range fundamentalsCmd.Commands() {
		fmt.Fprintf(w, "### fundamentals %s\n\n", subcmd.Name())
		fmt.Fprintf(w, "```\n%s\n```\n\n", subcmd.UsageString())
	}

	return nil
}
