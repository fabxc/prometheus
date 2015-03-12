# PromQL overhaul

This package is the result of refactoring the rules/ and rules/ast/ package.
It involves introducing a custom lexer/parser and centralizing query execution
in a query engine.

Sidenote: The approach of collecting behavior for all nodes (walks, typechecks,
evaluation, etc.) in one place rather than spreading it across all the nodes' was
taken by golang's parser, text/template, and InfluxDB.
I find it has high advantages in terms of maintainability and flexibility. Chances
are if I'm looking up the type-checks for unary expressions I also want to see it for
binary expressions a moment later. Looking 10 lines below rather than searching for
the method makes it easier.

## Parser and AST

Lexer and parser should both be well tested. As mentioned
the parsing now allows keywords for labelnames, handles operator precedence correctly, supports
different number notations including hex and octal.
Adding new functionality might require more LOC than in yacc but is very straightforward and
explicit so that it is less prone to unexpected behavior. Being able to test lexer/parser
is also a huge advantage.

The AST is slightly more relaxed regarding typing. In total, however, correct
typing should now be easier to track. By nature parsing and evaluating queries
involves lots of type-checks at runtime. Thus, over all it seems more
important that we can easily ensure where and how types are checked.

The old AST had stronger assumptions about a nodes' return types. The new parser
re-assigns child nodes in binary operations based on operator precedence. During
parsing the changes thus greatly simplified things.
After parsing the whole input, the parser enters its type-checking stage. The fully
constructed AST is traversed and proper typing is ensured.
This has the advantage of having all type-checks in a single, maintainable place.

Using the lexed token types directly in the AST was also chosen in text/template and
avoids constant mapping between enumerations and having repetitive String methods
for them all.
In go, every enumeration needs a switch-case with a default error either way. After
parsing an expression the default error can only happen if there is a bug in the first
place. The overhead of harder readibility, maintainability, and general code bloat does not
seem justified only for a (non-critical) bug to show a few microseconds earlier.

Looking at the actual code should reveal that it is not as much of an issue as this
whole explanation might let you think.

## Engine

The engine handles the complete control flow of a query from the input of a raw query string
to the eventual result. Querying parts of the code now do not need to know the components
involved in correctly executing the query (i.e., storage, rule manager, whatever
the future brings...).
Gathering of statistics, timeouts and cancellation are completely internal to the engine.
The clearer separation should allow arbitrary changes for all stages without touching
any code outside of promql.

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

The resulting control flow for any query could look like this (currently it is a bit more specific
about the query strings content, see below why):

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
makes it easy to jump between evaluations without type definitions and unrelated methods
interfering.
It also provides great flexibility for future optimizations, however they may look like.

The engine handles expression evaluation but the rules manager is still external. The rule 
manager registers handlers with the engine that are called on receiving new rule statements.
I consider it an easy and flexible way of breaking dependencies and it is heavily
used in camlistore (Brad Fitzpatrick) - so it cannot be completely off.

### Thoughts on the QL

To me a query language is a set of statements which I can throw at an application
(e.g. through a cmd) and they are simply executed. MongoDB, InfluxDB etc. are convenient
examples. There are statements getting data from the storage but also statements modifying
or informing about the current state (e.g., "SHOW DATABASES", "ADD USER").

PromQL currently does not entirely fulfill that. An alert statement in general is a command
that instructs to set a new alert (its not an alert in itself). The current engine reflects
that by informing the rule manager on receiving such a statement.
In contrast, an expression is not a statement. It needs more information to know what to do
with it, namely an evaluation timestamp or range. However, this is not part of the QL as of
now.

An evaluation statements could look like this:

	// Instant evaluation. Timestamp can also be 'now'
	[<timestamp>] <expression>
	[<negative duration>] <expression>

	// Range evaluation in different flavors. Timestamps can also be 'now'.
	[<start>:<end>:<step>] <expression>
	[<start>:<positive duration>:<step>] <expression>
	[<negative duration>:<timestamp>:<step>] <expression>
	[<negative duration>:<positive duration>:<step>] <expression>
	[<negative duration>:<negative duration>:<step>] <expression>

That's not say there cannot be functions like `engine.QueryRange(expr_str, start, end, step)`,
that imply a specific statement.

The future may bring more relevant requirements to modify Prometheus's state at runtime or get
information about it.
Backing up to long-term storage, removing old data, or anything that fulfills "Do this, now!"
and is thus not part of the more static configuration. The engine
is then a central place to control and relay those statements.

To me having a set of parsable statements that I can execute through a file, a shell, or over
HTTP seems very appealing. I know what I get and if there's anything wrong with my input
the engine will let me know.

### Functions

Using reflection for functions (like text/template does) was considered. There are few times
where reflection makes sense. This seemed to be one of it. Having the function's define what
data they want through their argument types would have been nice.
In general, though, there are edge cases in `delta` etc. that would make it overly complicated.
Leaving it as it is seems to provide greater flexibility. However, with the evaluator object,
evaluating the arguments became a bit cleaner.

Type constraints are now a bit stronger for functions - summed up, giving up a bit here, gaining
a bit there... in total it seems to be minor differences that are not all that important
in practice.
