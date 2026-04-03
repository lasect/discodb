package sql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"discodb/executor"
	"discodb/types"
)

type Statement interface {
	statement()
}

type SelectStmt struct {
	Columns     []SelectColumn
	From        *TableRef
	WhereClause *Predicate
	OrderBy     []OrderBy
	Limit       *int
	Offset      *int
}

func (SelectStmt) statement() {}

type InsertStmt struct {
	Table   TableRef
	Columns []string
	Values  [][]Expr
}

func (InsertStmt) statement() {}

type UpdateStmt struct {
	Table       TableRef
	Set         []SetClause
	WhereClause *Predicate
}

func (UpdateStmt) statement() {}

type DeleteStmt struct {
	Table       TableRef
	WhereClause *Predicate
}

func (DeleteStmt) statement() {}

type CreateTableStmt struct {
	Name    string
	Columns []ColumnDef
}

func (CreateTableStmt) statement() {}

type DropTableStmt struct {
	Name string
}

func (DropTableStmt) statement() {}

type CreateIndexStmt struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

func (CreateIndexStmt) statement() {}

type SelectColumn struct {
	All   bool
	Name  string
	Alias string
	Expr  *Expr
}

type SetClause struct {
	Column string
	Value  Expr
}

type ColumnDef struct {
	Name     string
	DataType SQLDataType
	Nullable bool
	Default  *Expr
}

type SQLDataType string

const (
	SQLBool      SQLDataType = "bool"
	SQLInt2      SQLDataType = "int2"
	SQLInt4      SQLDataType = "int4"
	SQLInt8      SQLDataType = "int8"
	SQLFloat4    SQLDataType = "float4"
	SQLFloat8    SQLDataType = "float8"
	SQLText      SQLDataType = "text"
	SQLJSON      SQLDataType = "json"
	SQLBlob      SQLDataType = "blob"
	SQLTimestamp SQLDataType = "timestamp"
)

type TableRef struct {
	Name  string
	Alias string
}

type Predicate struct {
	Comparison *Comparison
	Logical    *LogicalPredicate
}

type Comparison struct {
	Left  Expr
	Op    CompOp
	Right Expr
}

type LogicalPredicate struct {
	Left  *Predicate
	Op    LogicalOp
	Right *Predicate
}

type CompOp string
type LogicalOp string
type BinOp string

const (
	CompEq   CompOp = "="
	CompNe   CompOp = "!="
	CompLt   CompOp = "<"
	CompLe   CompOp = "<="
	CompGt   CompOp = ">"
	CompGe   CompOp = ">="
	CompLike CompOp = "like"
	CompIn   CompOp = "in"
)

const (
	LogicalAnd LogicalOp = "and"
	LogicalOr  LogicalOp = "or"
)

const (
	BinAdd BinOp = "+"
	BinSub BinOp = "-"
	BinMul BinOp = "*"
	BinDiv BinOp = "/"
	BinMod BinOp = "%"
)

type Expr struct {
	Column   string
	Constant *Constant
	Function string
	Args     []Expr
	Left     *Expr
	Op       BinOp
	Right    *Expr
}

type Constant struct {
	Value types.Value
}

type OrderBy struct {
	Expr      Expr
	Ascending bool
}

type Token struct {
	Kind TokenKind
	Val  string
}

type TokenKind int

const (
	TokEOF TokenKind = iota
	TokIdent
	TokString
	TokNumber
	TokSymbol
	TokKeyword
)

type Lexer struct {
	input string
	pos   int
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

func (l *Lexer) Next() Token {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return Token{Kind: TokEOF}
	}

	ch := l.input[l.pos]

	switch ch {
	case '(', ')', ',', '*', ';', '=', '<', '>', '!', '+', '-', '/':
		l.pos++
		if ch == '<' && l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Kind: TokSymbol, Val: "<="}
		}
		if ch == '>' && l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Kind: TokSymbol, Val: ">="}
		}
		if ch == '!' && l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Kind: TokSymbol, Val: "!="}
		}
		return Token{Kind: TokSymbol, Val: string(ch)}
	case '\'':
		return l.readString()
	case '"':
		return l.readQuotedIdent()
	}

	if isDigit(ch) {
		return l.readNumber()
	}

	if isAlpha(ch) || ch == '_' {
		return l.readIdent()
	}

	l.pos++
	return Token{Kind: TokSymbol, Val: string(ch)}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *Lexer) readString() Token {
	l.pos++
	start := l.pos
	for l.pos < len(l.input) {
		if l.input[l.pos] == '\'' {
			l.pos++
			return Token{Kind: TokString, Val: l.input[start : l.pos-1]}
		}
		l.pos++
	}
	return Token{Kind: TokString, Val: l.input[start:]}
}

func (l *Lexer) readQuotedIdent() Token {
	l.pos++
	start := l.pos
	for l.pos < len(l.input) {
		if l.input[l.pos] == '"' {
			l.pos++
			return Token{Kind: TokIdent, Val: l.input[start : l.pos-1]}
		}
		l.pos++
	}
	return Token{Kind: TokIdent, Val: l.input[start:]}
}

func (l *Lexer) readNumber() Token {
	start := l.pos
	for l.pos < len(l.input) && (isDigit(l.input[l.pos]) || l.input[l.pos] == '.') {
		l.pos++
	}
	return Token{Kind: TokNumber, Val: l.input[start:l.pos]}
}

func (l *Lexer) readIdent() Token {
	start := l.pos
	for l.pos < len(l.input) && (isAlpha(l.input[l.pos]) || isDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
		l.pos++
	}
	word := l.input[start:l.pos]
	upper := strings.ToUpper(word)
	if isKeyword(upper) {
		return Token{Kind: TokKeyword, Val: upper}
	}
	return Token{Kind: TokIdent, Val: word}
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }
func isAlpha(b byte) bool { return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') }

func isKeyword(word string) bool {
	switch word {
	case "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "IS", "NULL",
		"INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
		"CREATE", "TABLE", "DROP", "INDEX", "ON",
		"AS", "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET",
		"GROUP", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS",
		"INT2", "INT4", "INT8", "FLOAT4", "FLOAT8", "TEXT", "JSON", "BLOB", "BOOL", "TIMESTAMP", "DATE",
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"TRUE", "FALSE":
		return true
	}
	return false
}

type Parser struct {
	lexer *Lexer
	cur   Token
}

func NewParser(input string) *Parser {
	p := &Parser{lexer: NewLexer(input)}
	p.cur = p.lexer.Next()
	return p
}

func (p *Parser) advance() {
	p.cur = p.lexer.Next()
}

func (p *Parser) expectKeyword(kw string) error {
	if p.cur.Kind != TokKeyword || p.cur.Val != kw {
		return fmt.Errorf("expected %q, got %q", kw, p.cur.Val)
	}
	p.advance()
	return nil
}

func (p *Parser) expectSymbol(sym string) error {
	if p.cur.Kind != TokSymbol || p.cur.Val != sym {
		return fmt.Errorf("expected %q, got %q", sym, p.cur.Val)
	}
	p.advance()
	return nil
}

func (p *Parser) Parse() (Statement, error) {
	if p.cur.Kind == TokEOF {
		return nil, fmt.Errorf("empty query")
	}

	switch p.cur.Val {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert()
	case "CREATE":
		return p.parseCreate()
	case "DELETE":
		return nil, fmt.Errorf("unsupported: DELETE")
	case "UPDATE":
		return nil, fmt.Errorf("unsupported: UPDATE")
	case "DROP":
		return nil, fmt.Errorf("unsupported: DROP TABLE")
	default:
		return nil, fmt.Errorf("unsupported: %q (discodb supports SELECT, INSERT, CREATE TABLE)", p.cur.Val)
	}
}

func (p *Parser) parseSelect() (Statement, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}

	var columns []SelectColumn

	if p.cur.Kind == TokSymbol && p.cur.Val == "*" {
		columns = []SelectColumn{{All: true}}
		p.advance()
	} else {
		for {
			col, err := p.parseSelectColumn()
			if err != nil {
				return nil, err
			}
			columns = append(columns, col)
			if p.cur.Kind == TokSymbol && p.cur.Val == "," {
				p.advance()
			} else {
				break
			}
		}
	}

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}

	tableName := p.cur.Val
	if p.cur.Kind != TokIdent {
		return nil, fmt.Errorf("expected table name, got %q", p.cur.Val)
	}
	p.advance()

	stmt := SelectStmt{
		Columns: columns,
		From:    &TableRef{Name: tableName},
	}

	if p.cur.Kind == TokKeyword && p.cur.Val == "WHERE" {
		p.advance()
		pred, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		stmt.WhereClause = pred
	}

	if p.cur.Kind == TokKeyword && p.cur.Val == "ORDER" {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			ob, err := p.parseOrderBy()
			if err != nil {
				return nil, err
			}
			stmt.OrderBy = append(stmt.OrderBy, ob)
			if p.cur.Kind == TokSymbol && p.cur.Val == "," {
				p.advance()
			} else {
				break
			}
		}
	}

	if p.cur.Kind == TokKeyword && p.cur.Val == "LIMIT" {
		p.advance()
		if p.cur.Kind != TokNumber {
			return nil, fmt.Errorf("expected number after LIMIT, got %q", p.cur.Val)
		}
		n, err := strconv.Atoi(p.cur.Val)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value: %w", err)
		}
		stmt.Limit = &n
		p.advance()
	}

	return stmt, nil
}

func (p *Parser) parseSelectColumn() (SelectColumn, error) {
	if p.cur.Kind == TokSymbol && p.cur.Val == "*" {
		p.advance()
		return SelectColumn{All: true}, nil
	}

	name := p.cur.Val
	if p.cur.Kind != TokIdent {
		return SelectColumn{}, fmt.Errorf("expected column name, got %q", p.cur.Val)
	}
	p.advance()

	col := SelectColumn{Name: name}

	if p.cur.Kind == TokKeyword && p.cur.Val == "AS" {
		p.advance()
		col.Alias = p.cur.Val
		p.advance()
	}

	return col, nil
}

func (p *Parser) parseInsert() (Statement, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}

	tableName := p.cur.Val
	if p.cur.Kind != TokIdent {
		return nil, fmt.Errorf("expected table name, got %q", p.cur.Val)
	}
	p.advance()

	stmt := InsertStmt{
		Table: TableRef{Name: tableName},
	}

	if p.cur.Kind == TokSymbol && p.cur.Val == "(" {
		p.advance()
		for {
			if p.cur.Kind != TokIdent {
				return nil, fmt.Errorf("expected column name, got %q", p.cur.Val)
			}
			stmt.Columns = append(stmt.Columns, p.cur.Val)
			p.advance()
			if p.cur.Kind == TokSymbol && p.cur.Val == "," {
				p.advance()
			} else {
				break
			}
		}
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
	}

	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}

	for {
		if err := p.expectSymbol("("); err != nil {
			return nil, err
		}

		var row []Expr
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			row = append(row, expr)
			if p.cur.Kind == TokSymbol && p.cur.Val == "," {
				p.advance()
			} else {
				break
			}
		}
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, row)

		if p.cur.Kind == TokSymbol && p.cur.Val == "," {
			p.advance()
		} else {
			break
		}
	}

	return stmt, nil
}

func (p *Parser) parseCreate() (Statement, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	tableName := p.cur.Val
	if p.cur.Kind != TokIdent {
		return nil, fmt.Errorf("expected table name, got %q", p.cur.Val)
	}
	p.advance()

	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}

	var columns []ColumnDef
	for {
		colName := p.cur.Val
		if p.cur.Kind != TokIdent {
			return nil, fmt.Errorf("expected column name, got %q", p.cur.Val)
		}
		p.advance()

		dataType, err := p.parseDataType()
		if err != nil {
			return nil, err
		}

		col := ColumnDef{
			Name:     colName,
			DataType: dataType,
			Nullable: true,
		}

		for p.cur.Kind == TokKeyword {
			switch p.cur.Val {
			case "NOT":
				p.advance()
				if err := p.expectKeyword("NULL"); err != nil {
					return nil, err
				}
				col.Nullable = false
			case "NULL":
				p.advance()
				col.Nullable = true
			default:
				goto doneModifiers
			}
		}
	doneModifiers:

		columns = append(columns, col)

		if p.cur.Kind == TokSymbol && p.cur.Val == "," {
			p.advance()
		} else {
			break
		}
	}

	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}

	return CreateTableStmt{
		Name:    tableName,
		Columns: columns,
	}, nil
}

func (p *Parser) parseDataType() (SQLDataType, error) {
	if p.cur.Kind != TokKeyword {
		return "", fmt.Errorf("expected data type, got %q", p.cur.Val)
	}

	dt := SQLDataType(strings.ToLower(p.cur.Val))
	p.advance()
	return dt, nil
}

func (p *Parser) parseExpr() (Expr, error) {
	switch p.cur.Kind {
	case TokString:
		val := types.TextValue(p.cur.Val)
		p.advance()
		return Expr{Constant: &Constant{Value: val}}, nil
	case TokNumber:
		if strings.Contains(p.cur.Val, ".") {
			f, err := strconv.ParseFloat(p.cur.Val, 64)
			if err != nil {
				return Expr{}, fmt.Errorf("invalid float: %w", err)
			}
			p.advance()
			return Expr{Constant: &Constant{Value: types.Float8Value(f)}}, nil
		}
		n, err := strconv.ParseInt(p.cur.Val, 10, 64)
		if err != nil {
			return Expr{}, fmt.Errorf("invalid integer: %w", err)
		}
		p.advance()
		return Expr{Constant: &Constant{Value: types.Int8Value(n)}}, nil
	case TokKeyword:
		if p.cur.Val == "NULL" {
			p.advance()
			return Expr{Constant: &Constant{Value: types.NullValue()}}, nil
		}
		if p.cur.Val == "TRUE" {
			p.advance()
			return Expr{Constant: &Constant{Value: types.BoolValue(true)}}, nil
		}
		if p.cur.Val == "FALSE" {
			p.advance()
			return Expr{Constant: &Constant{Value: types.BoolValue(false)}}, nil
		}
		fallthrough
	case TokIdent:
		name := p.cur.Val
		p.advance()
		return Expr{Column: name}, nil
	default:
		return Expr{}, fmt.Errorf("unexpected token in expression: %q", p.cur.Val)
	}
}

func (p *Parser) parsePredicate() (*Predicate, error) {
	left, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	op := p.cur.Val
	if p.cur.Kind != TokSymbol {
		return nil, fmt.Errorf("expected comparison operator, got %q", p.cur.Val)
	}
	p.advance()

	right, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	pred := &Predicate{
		Comparison: &Comparison{
			Left:  left,
			Op:    CompOp(op),
			Right: right,
		},
	}

	if p.cur.Kind == TokKeyword && (p.cur.Val == "AND" || p.cur.Val == "OR") {
		logicalOp := LogicalOp(strings.ToLower(p.cur.Val))
		p.advance()
		rightPred, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		pred = &Predicate{
			Logical: &LogicalPredicate{
				Left:  pred,
				Op:    logicalOp,
				Right: rightPred,
			},
		}
	}

	return pred, nil
}

func (p *Parser) parseOrderBy() (OrderBy, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return OrderBy{}, err
	}

	ob := OrderBy{Expr: expr, Ascending: true}

	if p.cur.Kind == TokKeyword && p.cur.Val == "ASC" {
		p.advance()
	} else if p.cur.Kind == TokKeyword && p.cur.Val == "DESC" {
		ob.Ascending = false
		p.advance()
	}

	return ob, nil
}

func Parse(query string) (Statement, error) {
	p := NewParser(query)
	return p.Parse()
}

type Planner struct{}

func NewPlanner() Planner {
	return Planner{}
}

func (Planner) Plan(stmt Statement) (executor.PhysicalPlan, error) {
	switch stmt.(type) {
	case SelectStmt:
		return executor.PhysicalPlan{
			Root: executor.NewSeqScan(types.MinTableID(), nil, nil),
		}, nil
	default:
		return executor.PhysicalPlan{}, fmt.Errorf("unsupported: only SELECT is supported")
	}
}
