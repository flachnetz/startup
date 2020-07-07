package startup_postgres

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"testing"
)

func TestWithTransactionContext_Commit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	dbx := sqlx.NewDb(db, "pgx")

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE products SET key=1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = WithTransactionAutoCommitContext(context.Background(), dbx, func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE products SET key=1`)
		return err
	})

	g := NewGomegaWithT(t)
	g.Expect(mock.ExpectationsWereMet()).To(BeNil())
	g.Expect(err).To(BeNil())
}

func TestWithTransactionContext_Rollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	dbx := sqlx.NewDb(db, "pgx")

	testError := errors.New("the test error")

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE products SET key=1").WillReturnError(testError)
	mock.ExpectRollback()

	err = WithTransactionAutoCommitContext(context.Background(), dbx, func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE products SET key=1`)
		return err
	})

	g := NewGomegaWithT(t)
	g.Expect(mock.ExpectationsWereMet()).To(BeNil())
	g.Expect(errors.Cause(err)).To(Equal(testError))
}

func TestWithTransactionContext_RollbackPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	dbx := sqlx.NewDb(db, "pgx")

	testError := errors.New("the test error")

	mock.ExpectBegin()
	mock.ExpectRollback()

	err = WithTransactionAutoCommitContext(context.Background(), dbx, func(ctx context.Context, tx *sqlx.Tx) error {
		panic(testError)
	})

	g := NewGomegaWithT(t)
	g.Expect(mock.ExpectationsWereMet()).To(BeNil())
	g.Expect(errors.Cause(err)).To(Equal(testError))
}

func TestWithTransactionContext_RollbackPanic2(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	dbx := sqlx.NewDb(db, "pgx")

	mock.ExpectBegin()
	mock.ExpectRollback()

	err = WithTransactionAutoCommitContext(context.Background(), dbx, func(ctx context.Context, tx *sqlx.Tx) error {
		panic("the panic value")
	})

	g := NewGomegaWithT(t)
	g.Expect(mock.ExpectationsWereMet()).To(BeNil())
	g.Expect(errors.Cause(err).Error()).To(Equal(`panic("the panic value")`))
}
