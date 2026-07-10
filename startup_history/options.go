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
	})

	startup_base.FatalOnError(err, "Setup history tracking")
}
