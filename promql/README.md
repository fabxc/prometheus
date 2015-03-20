# PromQL overhaul

This package is the result of refactoring the rules/ and rules/ast/ package.
It involves introducing a custom lexer/parser and centralizing query execution
in a query engine.

Sidenote: The approach of collecting behavior for all nodes (walks, typechecks,
evaluation, etc.) in one place rather than spreading it across all the nodes' methods was
taken by golang's parser, text/template, and InfluxDB.
I find it has high advantages in terms of maintainability and flexibility.

Engine, lexer, and parser all have a panic-driven control flow (adapted from text/template).
Panics caused by non-runtime errors are recovered and the error is returned at the respective
entry point. Downstream of the entry-point there is genereally no way to deal with errors
and they are usually fatal to the entire parsing or evaluation. Obviously enabling the
common `err != nil` scheme is still possible for sub-systems (as the `Analyzer` shows).

I also do have dark memories about promising to keep changes small - so consider this a
concept. Should it be the right direction I'll do my best to split it into digestable
chunks.

## Parser and AST

Lexer and parser should both be well tested. The parser allows keywords
for label names, handles operator precedence correctly, and supports different number notations
including hex and octal.
Adding new functionality might require more LOC than in yacc but is very straightforward and
explicit so that it is less prone to unexpected behavior. Being able to test the lexer/parser,
as easy as it is now, is also a big advantage.

The AST is slightly more relaxed regarding typing. In total, however, correct
typing should now be easier to track. By nature parsing and evaluating queries
involves lots of type-checks at runtime. Having them now in one place (parser's type-checking stage)
should make things easier to maintain.
Golang and InfluxDB use the same approach. It also greatly simplifies things should there
ever be new value types (e.g. duration literals).
In general, it also works more natural with the parser. 

Using the lexed token types directly in the AST was also chosen in Golang and text/template
and avoids constant mapping between enumerations and having repetitive String methods
for them all.
In go, every enumeration needs a switch-case with a default error either way. After
parsing an expression the default error can only happen if there is a bug in the first
place. The overhead of harder readibility, maintainability, and general code bloat does not
seem justified only for a (non-critical) bug to show a few microseconds earlier.

Looking at the actual code should reveal that it is not as much of an issue as this
whole explanation might let you think.

### Functions

Type constraints are now a bit stronger for functions - summed up, giving up a bit here, gaining
a bit there... it boils down to minor differences that are not all that important
in practice.

An evaluator is passed instead of a plain timestamp. This makes argument argument evaluations
cleaner.

## Engine

The engine handles the complete control flow of a query from the input of a raw query string
to the eventual result. Querying parts of the code now do not need to know the components
involved in correctly executing the query (i.e., storage, rule manager, whatever
the future brings...).
Gathering of statistics, timeouts and cancellation are completely internal to the engine.
The clearer separation should allow arbitrary changes for all stages without touching
any code outside of promql. Contexts will make it easy to parallelize query execution (and
cancel it from any goroutine on error) should it turn out useful.

The generic query interface is:

	type Query interface {
		// Exec starts evaluating the query and returns immediately.
		// The returned channel blocks until the evaluation is done and receives
		// false if an error occurred, true otherwise.
		Exec() (done <-chan bool)
		// Result returns the result of the evaluated query. It blocks until the
		// result is available.
		Result() Result
		// Statements returns the parsed statements of the query.
		Statements() Statements
		// Stats returns statistics about the lifetime of the query.
		Stats() *stats.TimerGroup
		// Cancel signals that a running query execution should be aborted.
		Cancel()
	}

For per-query timeouts this would naturally be extended by `ExecWithTimeout`.

The resulting control flow for any query could look like this (currently it is a bit more specific
about the query strings' content, see below why):

	q, err := engine.Query("my awesome query")
	if err != nil {
		// Parsing errors.
	}
	q.Exec()
	res := q.Result()
	if res.Err != nil {
		// Finally also evaluation errors here, or timeout, or cancellation...
	}

	// Or alternatively...
	vec, err := q.Result().Vector()
	if err != nil {
		// Relayed res.Err if any, or error of wrong result type.
	}

As the above sidenote suggests expression evaluation is now gathered in a single
evaluator object. The same approach was taken in text/template and by InfluxDB. It
makes it easy to jump between evaluation implementations without type definitions
and unrelated methods interfering.
It also provides good flexibility for future optimizations, however they may look like.

The engine handles expression evaluation but the rules manager is still external. The rule 
manager registers handlers with the engine that are called on receiving new rule statements.
I consider it an easy and flexible way of breaking dependencies and the approach is heavily
used in camlistore (Brad Fitzpatrick) - so it cannot be completely off.

_Had some thoughts on QL here. Brian already noted it is out of scope. Actually, it wasn't 
relevant to the changes and rather distracting from the actual refactoring. Basically, by
design it should be very easy to extend the QL with more statements if ever desired.
The engine processes statements, so evaluating an expression is its own statement - just 
not one that can be derived from parsing an input._

## Roadmap

* Per-query timeout
* Limiting number of concurrent queries
* Reevaluating which query stats we actually want to collect. Currently the `stats/` package
  defines way more timers than being used.
* Agreeing on metrics to export from the engine.

### Metrics

Candidates for metrics include:

* Summaries on the query stats
* Counter for evaluated/failed queries
* Gauge of running queries

Possible labels for query metrics:

* Query type (alert, eval, record)
* Evaluation type (instant, range)
* Result status (`error = none | parsing | timeout | evaluation`)
* Labels informing about operations used in evaluated expressions.

## Change overview

* `analyzer.go`: Adjusted to new AST, no changes to behavior.
* `engine.go`: New `Engine` that handles query processing. Added `evaluator` that includes
  mostly unchanged evaluation methods from nodes.
* `functions.go`: Adjusted to new AST/types. No changes in behavior of function
  implementations.
* `lex.go`: New custom lexer implementation.
* `parse.go`: New custom parser implementation.
* `printer.go`: `String()` and `DotGraph()` methods for AST nodes.
* `promql_test.go`: Port of end-to-end query evaluation tests (from `rules_test.go`)
* `setup_test.go`: Setup portion for `promql_test.go` (former `helpers_test.go`)
* Other test files are new.

Respective changes to other parts of Prometheus to adjust to the Engine interface were made.
The `rules/` package became significantly slimmer, but the remains have unchanged behavior.
