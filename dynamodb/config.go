package dynamodb

import "fmt"

type Config struct {
	DdbTableName string `yaml:"ddb_table_name"`
	AwsAccessKey string `yaml:"aws_access_key"`
	AwsSecretKey string `yaml:"aws_secret_key"`
	AwsRegion    string `yaml:"aws_region"`
}

type LocalConfig struct {
	DdbTableName string `yaml:"ddb_table_name"`
	Endpoint     string `yaml:"endpoint"`
	AwsRegion    string `yaml:"aws_region"`
}

func (cfg Config) HasError() error {
	if cfg.DdbTableName == "" {
		return fmt.Errorf("Invalid config, missing DdbTableName: %v", cfg)
	}
	return nil
}

func (cfg LocalConfig) HasError() error {
	if cfg.DdbTableName == "" {
		return fmt.Errorf("Invalid config, missing DdbTableName: %v", cfg)
	}
	if cfg.Endpoint == "" {
		return fmt.Errorf("Invalid config, missing Endpoint: %v", cfg)
	}
	return nil
}
