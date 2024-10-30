package cmd

import (
	"fmt"
	"os"

	"log/slog"

	"github.com/joho/godotenv"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/logger"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "etl",
	Short: "etl cli for different etl tasks",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(endOfDayCmd)
	rootCmd.AddCommand(fundamentalsCmd)
	endOfDayCmd.AddCommand(newDailyCmd())
	endOfDayCmd.AddCommand(newBackfillCmd())
}

func isRunningOnGitHubActions() bool {
	return os.Getenv("GITHUB_ACTIONS") == "true"
}

func initializeConfigAndLogger() (*config.Config, *slog.Logger, error) {
	log := logger.NewLogger()
	if !isRunningOnGitHubActions() {
		err := godotenv.Load()
		if err != nil {
			log.Error("Error loading .env file")
			return nil, nil, err
		}
	}

	// 1. Open the base configuration file
	baseConfigFile, err := os.Open("config.base.yaml") // Update with your base config file path
	if err != nil {
		log.Error(fmt.Sprintf("Error opening base config file: %v", err))
		return nil, nil, err
	}
	defer baseConfigFile.Close()

	// 2. Prepare environment-specific config reader (if needed)
	env := os.Getenv("APP_ENV")
	var envConfigFile *os.File
	envConfigFilename := fmt.Sprintf("config.%s.yaml", env)
	if _, err := os.Stat(envConfigFilename); err == nil {
		// Environment-specific config file exists
		envConfigFile, err = os.Open(envConfigFilename)
		if err != nil {
			log.Error(fmt.Sprintf("Error opening environment config file: %v", err))
			return nil, nil, err
		}
		defer envConfigFile.Close()
	}

	// 3. Create the config
	cfg, err := config.NewConfig(baseConfigFile, envConfigFile, env)
	if err != nil {
		log.Error(fmt.Sprintf("Error reading config: %v", err))
		return nil, nil, err
	}

	return cfg, log, nil
}
