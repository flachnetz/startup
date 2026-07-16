package startup_history

import (
	"context"

	"github.com/flachnetz/startup/v2/lib/history"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_postgres"
)

type HistoryOptions struct {
	Inputs struct {
		HistoryEventCreator history.EventCreator `validate:"required"`
	}

	// Athena read fallback for history records that have aged out of the local
	// table. Left empty (the default) it is disabled and reads hit the local
	// table only; set database, table and output location to enable it.
	AthenaDatabase       string `long:"history-athena-database" env:"HISTORY_ATHENA_DATABASE" description:"Athena database holding aged-out history records"`
	AthenaTable          string `long:"history-athena-table" env:"HISTORY_ATHENA_TABLE" description:"Athena table for history records"`
	AthenaWorkGroup      string `long:"history-athena-workgroup" env:"HISTORY_ATHENA_WORKGROUP" description:"Athena workgroup"`
	AthenaOutputLocation string `long:"history-athena-output-location" env:"HISTORY_ATHENA_OUTPUT_LOCATION" description:"Athena query result S3 output location"`
	AthenaRegion         string `long:"history-athena-region" env:"HISTORY_ATHENA_REGION" description:"AWS region for Athena"`
}

func (o HistoryOptions) athenaConfig() *history.AthenaConfig {
	if o.AthenaDatabase == "" || o.AthenaTable == "" || o.AthenaOutputLocation == "" {
		return nil
	}
	return &history.AthenaConfig{
		Database:       o.AthenaDatabase,
		Table:          o.AthenaTable,
		WorkGroup:      o.AthenaWorkGroup,
		OutputLocation: o.AthenaOutputLocation,
		Region:         o.AthenaRegion,
	}
}

func (o HistoryOptions) Initialize(
	ctx context.Context,
	base startup_base.BaseOptions,
	postgresOptions *startup_postgres.PostgresOptions,
) {
	err := history.InitializeGlobal(ctx, history.Options{
		DB:           postgresOptions.Connection(),
		ServiceId:    base.ServiceName,
		HistoryTable: base.TableName("history"),
		EventCreator: o.Inputs.HistoryEventCreator,
		Athena:       o.athenaConfig(),
	})

	startup_base.FatalOnError(err, "Setup history tracking")
}
