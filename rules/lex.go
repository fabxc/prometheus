package rules

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// item represents a token or text string returned from the scanner.
type item struct {
	typ itemType // The type of this item.
	pos Pos      // The starting position, in bytes, of this item in the input string.
	val string   // The value of this item.
}

// String returns a descriptive string for the item.
func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case i.typ.isKeyword():
		return fmt.Sprintf("<%s>", i.val)
	case i.typ.isOperator():
		return fmt.Sprintf("<op:%s>", i.val)
	case i.typ.isAggregator():
		return fmt.Sprintf("<aggre:%s>", i.val)
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

// isOperator returns true if the item corresponds to a logical or arithmetic operator.
// Returns false otherwise.
func (i itemType) isOperator() bool { return i > itemOperators && i < itemAggregators }

// isAggregator returns true if the item belongs to the aggregator functions.
// Returns false otherwise
func (i itemType) isAggregator() bool { return i > itemAggregators && i < itemKeywords }

// isKeyword returns true if the item corresponds to a keyword.
// Returns false otherwise.
func (i itemType) isKeyword() bool { return i > itemKeywords }

// A set of constants for precedence-based expression parsing.
// Non-operators have lowest precedence, followed by operators
// starting with precedence 1 up to unary operators. The highest
// precedence serves as "catch-all" precedence for selector,
// indexing, and other operator and delimiter tokens.
const (
	LowestPrec  = 0 // non-operators
	UnaryPrec   = 6
	HighestPrec = 7
)

// Precedence returns the operator precedence of the binary
// operator op. If op is not a binary operator, the result
// is LowestPrec.
func (i itemType) precedence() int {
	switch i {
	case itemLOR:
		return 1
	case itemLAND:
		return 2
	case itemEQL, itemNEQ, itemLTE, itemLSS, itemGTE, itemGTR:
		return 3
	case itemADD, itemSUB:
		return 4
	case itemMUL, itemQUO, itemREM:
		return 5
	}
	return LowestPrec
}

type itemType int

const (
	itemError itemType = iota // Error occurred, value is error message
	itemEOF
	itemComment
	itemIdentifier
	itemMetricIdentifier
	itemLeftParen
	itemRightParen
	itemLeftBrace
	itemRightBrace
	itemLeftBracket
	itemRightBracket
	itemComma
	itemSemicolon
	itemString
	itemNumber
	itemDuration

	// regex ops are used for label matcher

	itemOperators
	// operators
	itemSUB
	itemADD
	itemMUL
	itemREM
	itemQUO
	itemLAND
	itemLOR
	itemEQL
	itemNEQ
	itemLTE
	itemLSS
	itemGTE
	itemGTR
	// these are only used for label matching
	itemEQLRegex
	itemNEQRegex

	itemAggregators
	// aggregators
	itemAvg
	itemCount
	itemSum
	itemMin
	itemMax

	itemKeywords
	// keywords
	itemAlert
	itemExec
	itemIf
	itemFor
	itemWith
	itemSummary
	itemDescription
	itemKeepingExtra
	itemPermanent
	itemOffset
	itemBy
	itemOn
	itemGroupLeft
	itemGroupRight
)

var key = map[string]itemType{
	// operators
	"AND": itemLAND,
	"OR":  itemLOR,

	// aggregators
	"SUM":   itemSum,
	"AVG":   itemAvg,
	"COUNT": itemCount,
	"MIN":   itemMin,
	"MAX":   itemMax,

	// keywords
	"ALERT":         itemAlert,
	"EXEC":          itemExec,
	"IF":            itemIf,
	"FOR":           itemFor,
	"WITH":          itemWith,
	"SUMMARY":       itemSummary,
	"DESCRIPTION":   itemDescription,
	"PERMANENT":     itemPermanent,
	"OFFSET":        itemOffset,
	"BY":            itemBy,
	"KEEPING_EXTRA": itemKeepingExtra,
	"ON":            itemOn,
	"GROUP_LEFT":    itemGroupLeft,
	"GROUP_RIGHT":   itemGroupRight,
}

const eof = -1

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*lexer) stateFn

type Pos int

// lexer holds the state of the scanner.
type lexer struct {
	name    string    // the name of the input; used only for error reports
	input   string    // the string being scanned
	state   stateFn   // the next lexing function to enter
	pos     Pos       // current position in the input
	start   Pos       // start position of this item
	width   Pos       // width of last rune read from input
	lastPos Pos       // position of most recent item returned by nextItem
	items   chan item // channel of scanned items

	parenDepth  int  // nesting depth of ( ) exprs
	braceOpen   bool // whether a { is opened
	bracketOpen bool // whether a [ is opened
	stringOpen  rune // quote char with which a string was opened
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = Pos(w)
	l.pos += l.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune. Can only be called once per call of next.
func (l *lexer) backup() {
	l.pos -= l.width
}

// emit passes an item back to the client.
func (l *lexer) emit(t itemType) {
	l.items <- item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.start = l.pos
}

// accept consumes the next rune if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, l.next()) >= 0 {
		// consume
	}
	l.backup()
}

// lineNumber reports which line we're on, based on the position of
// the previous item returned by nextItem. Doing it this way
// means we don't have to worry about peek double counting.
func (l *lexer) lineNumber() int {
	return 1 + strings.Count(l.input[:l.lastPos], "\n")
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, l.start, fmt.Sprintf(format, args...)}
	return nil
}

// nextItem returns the next item from the input.
func (l *lexer) nextItem() item {
	item := <-l.items
	l.lastPos = item.pos
	return item
}

// lex creates a new scanner for the input string.
func lex(name, input string) *lexer {
	l := &lexer{
		name:  name,
		input: input,
		items: make(chan item),
	}
	go l.run()
	return l
}

// run runs the state machine for the lexer.
func (l *lexer) run() {
	for l.state = lexStatements; l.state != nil; {
		l.state = l.state(l)
	}
	close(l.items)
}

const (
	lineComment  = "//"
	leftComment  = "/*"
	rightComment = "*/"
)

func lexStatements(l *lexer) stateFn {
	if l.braceOpen {
		return lexInsideBraces
	}
	if strings.HasPrefix(l.input[l.pos:], leftComment) {
		return lexComment
	}
	if strings.HasPrefix(l.input[l.pos:], lineComment) {
		return lexLineComment
	}

	switch r := l.next(); {
	case r == eof:
		if l.parenDepth != 0 {
			return l.errorf("unclosed left paren")
		} else if l.bracketOpen {
			return l.errorf("unclosed left bracket")
		}
		l.emit(itemEOF)
		return nil
	case r == ';':
		if l.parenDepth != 0 {
			return l.errorf("unclosed left paren")
		} else if l.bracketOpen {
			return l.errorf("unclosed left bracket")
		}
		l.emit(itemSemicolon)
	case r == ',':
		l.emit(itemComma)
	case isSpace(r):
		return lexSpace
	case r == '*':
		l.emit(itemMUL)
	case r == '/':
		l.emit(itemQUO)
	case r == '%':
		l.emit(itemREM)
	case r == '+':
		if strings.HasPrefix(l.input[l.pos:], "Inf") {
			l.pos += 3
			l.emit(itemNumber)
		} else {
			l.emit(itemADD)
		}
	case r == '-':
		if strings.HasPrefix(l.input[l.pos:], "Inf") {
			l.pos += 3
			l.emit(itemNumber)
		} else {
			l.emit(itemSUB)
		}
	case r == '=':
		if t := l.next(); t == '=' {
			l.emit(itemEQL)
		} else {
			l.backup()
			l.emit(itemEQL) // TODO: emit assignment item here?
		}
	case r == '!':
		if t := l.next(); t == '=' {
			l.emit(itemNEQ)
		} else {
			return l.errorf("unrecognized character after '!': %#U", t)
		}
	case r == '<':
		if t := l.peek(); t == '=' {
			l.next()
			l.emit(itemLTE)
		} else {
			l.emit(itemLSS)
		}
	case r == '>':
		if t := l.peek(); t == '=' {
			l.next()
			l.emit(itemGTE)
		} else {
			l.emit(itemGTR)
		}
	case '0' <= r && r <= '9' || r == '.':
		l.backup()
		return lexNumberOrDuration
	case r == '"':
		return lexString
	case r == 'N':
		if strings.HasPrefix(l.input[l.pos:], "aN") {
			l.pos += 2
			l.emit(itemNumber)
			break
		}
		fallthrough
	case isAlphaNumeric(r):
		l.backup()
		return lexKeywordOrIdentifier
	case r == '(':
		l.emit(itemLeftParen)
		l.parenDepth++
		return lexStatements
	case r == ')':
		l.emit(itemRightParen)
		l.parenDepth--
		if l.parenDepth < 0 {
			return l.errorf("unexpected right paren %#U", r)
		}
		return lexStatements
	case r == '{':
		l.emit(itemLeftBrace)
		l.braceOpen = true
		return lexInsideBraces(l)
	case r == '[':
		if l.bracketOpen {
			return l.errorf("unexpected left bracket %#U", r)
		}
		l.emit(itemLeftBracket)
		l.bracketOpen = true
		return lexDuration
	case r == ']':
		if !l.bracketOpen {
			return l.errorf("unexpected right bracket %#U", r)
		}
		l.emit(itemRightBracket)
		l.bracketOpen = false

	default:
		return l.errorf("unrecognized character in statement: %#U", r)
	}
	return lexStatements
}

// lexInsideBraces scans the inside of a vector selector. Keywords are ignored and
// scanned as identifiers.
func lexInsideBraces(l *lexer) stateFn {
	if strings.HasPrefix(l.input[l.pos:], leftComment) {
		lexComment(l)
	}
	if strings.HasPrefix(l.input[l.pos:], lineComment) {
		lexLineComment(l)
	}

	switch r := l.next(); {
	case r == eof:
		return l.errorf("unexpected EOF inside braces")
	case isSpace(r):
		return lexSpace
	case isAlphaNumeric(r):
		l.backup()
		return lexIdentifier
	case r == ',':
		l.emit(itemComma)
	case r == '"' || r == '\'':
		l.stringOpen = r
		return lexString
	case r == '=':
		if l.next() == '~' {
			l.emit(itemEQLRegex)
			break
		}
		l.backup()
		l.emit(itemEQL)
	case r == '!':
		switch nr := l.next(); {
		case nr == '~':
			l.emit(itemNEQRegex)
		case nr == '=':
			l.emit(itemNEQ)
		default:
			return l.errorf("unrecognized character after ! inside braces: %#U", nr)
		}
	case r == '{':
		return l.errorf("unexpected left brace %#U", r)
	case r == '}':
		l.emit(itemRightBrace)
		l.braceOpen = false
		return lexStatements
	default:
		return l.errorf("unrecognized character inside braces: %#U", r)
	}
	return lexInsideBraces
}

// lexString scans a quoted string. The initial quote has already been seen.
func lexString(l *lexer) stateFn {
Loop:
	for {
		switch l.next() {
		case '\\':
			if r := l.next(); r != eof && r != '\n' {
				break
			}
			fallthrough
		case eof, '\n':
			return l.errorf("unterminated quoted string")
		case l.stringOpen:
			break Loop
		}
	}
	l.emit(itemString)
	return lexStatements
}

// lexSpace scans a run of space characters. One space has already been seen.
func lexSpace(l *lexer) stateFn {
	for isSpace(l.peek()) {
		l.next()
	}
	l.ignore()
	return lexStatements
}

// lexLineComment scans a line comment. Left comment marker is known to be present.
func lexLineComment(l *lexer) stateFn {
	l.pos += Pos(len(lineComment))
	for r := l.next(); !isEndOfLine(r) && r != eof; {
		r = l.next()
	}
	l.backup()
	l.emit(itemComment)
	return lexStatements
}

// lexComment scans a comment. Left comment marker is known to be present.
func lexComment(l *lexer) stateFn {
	l.pos += Pos(len(leftComment))
	i := strings.Index(l.input[l.pos:], rightComment)
	if i < 0 {
		return l.errorf("unclosed comment")
	}
	l.pos += Pos(i + len(rightComment))
	l.emit(itemComment)
	return lexStatements
}

func lexDuration(l *lexer) stateFn {
	if l.scanNumber() {
		return l.errorf("missing unit character in duration")
	}
	// Next two chars must be a valid unit and a non-alphanumeric.
	if l.accept("smhdwy") && !isAlphaNumeric(l.peek()) {
		l.emit(itemDuration)
		return lexStatements
	}
	return l.errorf("bad duration syntax: %q", l.input[l.start:l.pos])
}

// lexNumber scans a number: decimal, hex, oct or float.
func lexNumber(l *lexer) stateFn {
	if !l.scanNumber() {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}
	l.emit(itemNumber)
	return lexStatements
}

func lexNumberOrDuration(l *lexer) stateFn {
	if l.scanNumber() {
		l.emit(itemNumber)
		return lexStatements
	}
	// Next two chars must be a valid unit and a non-alphanumeric.
	if l.accept("smhdwy") && !isAlphaNumeric(l.peek()) {
		l.emit(itemDuration)
		return lexStatements
	}
	return l.errorf("bad number or duration syntax: %q", l.input[l.start:l.pos])
}

func (l *lexer) scanNumber() bool {
	// Optional leading sign.
	l.accept("+-")
	// Is it hex?
	digits := "0123456789"
	if l.accept("0") && l.accept("xX") {
		digits = "0123456789abcdefABCDEF"
	}
	l.acceptRun(digits)
	if l.accept(".") {
		l.acceptRun(digits)
	}
	if l.accept("eE") {
		l.accept("+-")
		l.acceptRun("0123456789")
	}
	// Next thing mustn't be alphanumeric.
	if isAlphaNumeric(l.peek()) {
		return false
	}
	return true
}

// lexIdentifier scans an alphanumeric identifier.
func lexIdentifier(l *lexer) stateFn {
	for isAlphaNumeric(l.next()) {
		// absorb
	}
	l.backup()
	l.emit(itemIdentifier)
	return lexStatements
}

// lexKeywordOrIdentifier scans an alphanumeric identifier which may contain
// a colon rune. If the identifier is a keyword the respective keyword item
// is scanned.
func lexKeywordOrIdentifier(l *lexer) stateFn {
Loop:
	for {
		switch r := l.next(); {
		case isAlphaNumeric(r) || r == ':':
			// absorb.
		default:
			l.backup()
			word := l.input[l.start:l.pos]
			if kw, ok := key[strings.ToUpper(word)]; ok {
				l.emit(kw)
			} else if !strings.Contains(word, ":") {
				l.emit(itemIdentifier)
			} else {
				l.emit(itemMetricIdentifier)
			}
			break Loop
		}
	}
	return lexStatements
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n'
}

// isEndOfLine reports whether r is an end-of-line character.
func isEndOfLine(r rune) bool {
	return r == '\r' || r == '\n'
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
