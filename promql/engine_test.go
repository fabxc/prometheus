package promql

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/prometheus/prometheus/storage/local"
)

func TestQueryTimeout(t *testing.T) {
	*defaultQueryTimeout = 5 * time.Millisecond
	defer func() {
		// Restore default query timeout
		*defaultQueryTimeout = 2 * time.Minute
	}()

	storage, closer := local.NewTestStorage(t, 1)
	defer closer.Close()

	engine := NewEngine(storage)
	defer engine.Stop()

	query, err := engine.Query("foo = bar")
	if err != nil {
		t.Fatalf("error parsing query: %s", err)
	}

	// Timeouts are not exact but checked in designated places. For example between
	// invoking handlers. Thus, we reigster two handlers that take some time to ensure we check
	// after exceeding the timeout.
	// Should the implementation of this area change, the test might have to be adjusted.
	engine.RegisterRecordHandler("test", func(context.Context, *RecordStmt) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	engine.RegisterRecordHandler("test2", func(context.Context, *RecordStmt) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	query.Exec()
	res := query.Result()
	if res.Err == nil {
		t.Fatalf("expected timeout error but got none")
	}
	if res.Err != nil && res.Err != context.DeadlineExceeded {
		t.Fatalf("expected timeout error but got: %s", res.Err)
	}
}

func TestQueryCancel(t *testing.T) {
	storage, closer := local.NewTestStorage(t, 1)
	defer closer.Close()

	engine := NewEngine(storage)
	defer engine.Stop()

	query1, err := engine.Query("foo = bar")
	if err != nil {
		t.Fatalf("error parsing query: %s", err)
	}
	query2, err := engine.Query("foo = baz")
	if err != nil {
		t.Fatalf("error parsing query: %s", err)
	}

	// As for timeouts, cancellation is only checked at designated points. We ensure
	// that we reach one of those points using the same method.
	engine.RegisterRecordHandler("test1", func(context.Context, *RecordStmt) error { return nil })
	engine.RegisterRecordHandler("test2", func(context.Context, *RecordStmt) error { return nil })

	// Cancel query after starting it.
	query1.Exec()
	query1.Cancel()

	res := query1.Result()
	if res.Err == nil {
		t.Fatalf("expected cancellation error for query1 but got none")
	}
	if res.Err != nil && res.Err != context.Canceled {
		t.Fatalf("expected cancellation error for query1 but got: %s", res.Err)
	}

	// Canceling query before starting it must have no effect.
	query2.Cancel()
	query2.Exec()

	res = query2.Result()
	if res.Err != nil {
		t.Fatalf("unexpeceted error on executing query2: %s", res.Err)
	}
}

func TestEngineShutdown(t *testing.T) {
	storage, closer := local.NewTestStorage(t, 1)
	defer closer.Close()

	engine := NewEngine(storage)

	query1, err := engine.Query("foo = bar")
	if err != nil {
		t.Fatalf("error parsing query: %s", err)
	}
	query2, err := engine.Query("foo = baz")
	if err != nil {
		t.Fatalf("error parsing query: %s", err)
	}

	handlerExecutions := 0

	// Shutdown engine on first handler execution. Should handler execution ever become
	// concurrent this test has to be adjusted accordingly.
	engine.RegisterRecordHandler("test", func(context.Context, *RecordStmt) error {
		handlerExecutions++
		engine.Stop()
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	engine.RegisterRecordHandler("test2", func(context.Context, *RecordStmt) error {
		handlerExecutions++
		engine.Stop()
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	// Stopping the engine should cancel the base context. While setting up queries is
	// still possible their context is canceled from the beginning and execution should
	// terminate immediately.

	query1.Exec()
	res := query1.Result()
	if res.Err == nil {
		t.Fatalf("expected error on shutdown during query but got none")
	}
	if handlerExecutions != 1 {
		t.Fatalf("expected only one handler to be executed before query cancellation but got %d executons", handlerExecutions)
	}

	query2.Exec()
	res2 := query2.Result()
	if res2.Err == nil {
		t.Fatalf("expected error on querying shutdown engine but got none")
	}
	if handlerExecutions != 1 {
		t.Fatalf("expected no handler execution for query after engine shutdown")
	}

}

func TestAlertHandler(t *testing.T) {
	storage, closer := local.NewTestStorage(t, 1)
	defer closer.Close()

	engine := NewEngine(storage)
	defer engine.Stop()

	qs := `ALERT Foo IF bar FOR 5m WITH {a="b"} SUMMARY "sum" DESCRIPTION "desc"`

	doQuery := func(expectFailure bool) *AlertStmt {
		query, err := engine.Query(qs)
		if err != nil {
			t.Fatalf("error parsing query: %s", err)
		}
		query.Exec()
		res := query.Result()
		if expectFailure && res.Err == nil {
			t.Fatalf("expected error but got none.")
		}
		if res.Err != nil && !expectFailure {
			t.Fatalf("error on executing alert query: %s", res.Err)
		}
		// That this alert statement is correct is tested elsewhere.
		return query.Statements()[0].(*AlertStmt)
	}

	// We expect an error if nothing is registered to handle the query.
	alertStmt := doQuery(true)

	receivedCalls := 0

	// Ensure that we receive the correct statement.
	engine.RegisterAlertHandler("test", func(ctx context.Context, as *AlertStmt) error {
		if !reflect.DeepEqual(alertStmt, as) {
			t.Errorf("received alert statement did not match input: %q", qs)
			t.Fatalf("no match\n\nexpected:\n%s\ngot: \n%s\n", Tree(alertStmt), Tree(as))
		}
		receivedCalls++
		return nil
	})

	for i := 0; i < 10; i++ {
		doQuery(false)
		if receivedCalls != i+1 {
			t.Fatalf("alert handler was not called on query execution")
		}
	}

	engine.UnregisterAlertHandler("test")

	// We must receive no further calls after unregistering.
	doQuery(true)
	if receivedCalls != 10 {
		t.Fatalf("received calls after unregistering alert handler")
	}
}

func TestRecordHandler(t *testing.T) {
	storage, closer := local.NewTestStorage(t, 1)
	defer closer.Close()

	engine := NewEngine(storage)
	defer engine.Stop()

	qs := `foo = bar`

	doQuery := func(expectFailure bool) *RecordStmt {
		query, err := engine.Query(qs)
		if err != nil {
			t.Fatalf("error parsing query: %s", err)
		}
		query.Exec()
		res := query.Result()
		if expectFailure && res.Err == nil {
			t.Fatalf("expected error but got none.")
		}
		if res.Err != nil && !expectFailure {
			t.Fatalf("error on executing record query: %s", res.Err)
		}
		return query.Statements()[0].(*RecordStmt)
	}

	// We expect an error if nothing is registered to handle the query.
	recordStmt := doQuery(true)

	receivedCalls := 0

	// Ensure that we receive the correct statement.
	engine.RegisterRecordHandler("test", func(ctx context.Context, rs *RecordStmt) error {
		if !reflect.DeepEqual(recordStmt, rs) {
			t.Errorf("received record statement did not match input: %q", qs)
			t.Fatalf("no match\n\nexpected:\n%s\ngot: \n%s\n", Tree(recordStmt), Tree(rs))
		}
		receivedCalls++
		return nil
	})

	for i := 0; i < 10; i++ {
		doQuery(false)
		if receivedCalls != i+1 {
			t.Fatalf("record handler was not called on query execution")
		}
	}

	engine.UnregisterRecordHandler("test")

	// We must receive no further calls after unregistering.
	doQuery(true)
	if receivedCalls != 10 {
		t.Fatalf("received calls after unregistering record handler")
	}
}
