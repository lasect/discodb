package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"

	"discodb/engine"
	"discodb/types"
)

type QueryStatus byte

const (
	QueryStatusIdle             QueryStatus = 'I'
	QueryStatusTransactionBlock QueryStatus = 'T'
	QueryStatusFailed           QueryStatus = 'E'
)

type ErrorSeverity string

const (
	SeverityError  ErrorSeverity = "ERROR"
	SeverityFatal  ErrorSeverity = "FATAL"
	SeverityPanic  ErrorSeverity = "PANIC"
	SeverityWarn   ErrorSeverity = "WARNING"
	SeverityNotice ErrorSeverity = "NOTICE"
	SeverityDebug  ErrorSeverity = "DEBUG"
	SeverityInfo   ErrorSeverity = "INFO"
	SeverityLog    ErrorSeverity = "LOG"
)

type FieldDescription struct {
	Name         string
	TableOID     uint32
	ColumnIndex  uint16
	TypeOID      uint32
	TypeSize     int16
	TypeModifier int32
	Format       int16
}

type CommandTag struct {
	Command string
	Rows    *uint64
	Oid     uint64
}

func SelectTag(rows uint64) CommandTag {
	return CommandTag{Command: "SELECT", Rows: &rows}
}

func InsertTag(rows uint64) CommandTag {
	return CommandTag{Command: "INSERT", Rows: &rows}
}

func CreateTableTag() CommandTag {
	return CommandTag{Command: "CREATE TABLE"}
}

type ErrorResponse struct {
	Severity ErrorSeverity
	Code     string
	Message  string
	Detail   string
	Hint     string
}

func SyntaxError(message string) ErrorResponse {
	return ErrorResponse{Severity: SeverityError, Code: "42601", Message: message}
}

func InternalError(message string) ErrorResponse {
	return ErrorResponse{Severity: SeverityError, Code: "XX000", Message: message}
}

type Server struct {
	addr   string
	logger *slog.Logger
	engine *engine.Engine
}

func NewServer(addr string, logger *slog.Logger, eng *engine.Engine) *Server {
	return &Server{
		addr:   addr,
		logger: logger,
		engine: eng,
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	s.logger.Info("wire server listening", slog.String("addr", s.addr))

	connCounter := uint64(0)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		connCounter++
		connID := fmt.Sprintf("conn-%d", connCounter)
		go func() {
			if err := s.handleConnection(context.Background(), conn, connID); err != nil {
				s.logger.Error("connection failed", slog.String("conn_id", connID), slog.String("error", err.Error()))
			}
		}()
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn, connID string) error {
	defer conn.Close()

	if err := handleSSLNegotiation(conn); err != nil {
		return err
	}
	if err := readStartup(conn); err != nil {
		return err
	}
	if _, err := conn.Write(serializeAuthenticationOK()); err != nil {
		return err
	}

	s.engine.SetConnState(connID, engine.ConnTxnIdle)

	if _, err := conn.Write(serializeReadyForQuery(QueryStatusIdle)); err != nil {
		return err
	}

	for {
		msgType := make([]byte, 1)
		if _, err := io.ReadFull(conn, msgType); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lengthBuf); err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(lengthBuf)
		if length < 4 {
			return fmt.Errorf("invalid message length: %d", length)
		}
		payload := make([]byte, int(length)-4)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}

		switch msgType[0] {
		case 'Q':
			query := string(bytes.TrimRight(payload, "\x00"))
			queryErr := s.executeQuery(ctx, conn, connID, query)
			status := s.connStatus(connID, queryErr != nil)
			if _, err := conn.Write(serializeReadyForQuery(status)); err != nil {
				return err
			}
		case 'X':
			return nil
		default:
			errResp := SyntaxError(fmt.Sprintf("unsupported frontend message: %q", msgType[0]))
			if _, err := conn.Write(serializeErrorResponse(errResp)); err != nil {
				return err
			}
			if _, err := conn.Write(serializeReadyForQuery(QueryStatusFailed)); err != nil {
				return err
			}
		}
	}
}

func handleSSLNegotiation(conn net.Conn) error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(buf[0:4])
	code := binary.BigEndian.Uint32(buf[4:8])

	if length == 8 && code == 80877103 {
		conn.Write([]byte{'N'})
		return nil
	}

	return fmt.Errorf("expected SSLRequest or StartupMessage, got length=%d code=%d", length, code)
}

func readStartup(conn net.Conn) error {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBuf); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(lengthBuf)
	if length < 8 {
		return fmt.Errorf("invalid startup packet length: %d", length)
	}
	payload := make([]byte, int(length)-4)
	_, err := io.ReadFull(conn, payload)
	return err
}

func (s *Server) executeQuery(ctx context.Context, conn net.Conn, connID string, query string) error {
	_ = ctx

	query = strings.TrimSpace(query)
	if query == "" || query == ";" {
		return nil
	}

	cols, rows, affectedRows, err := s.engine.ExecuteQueryWithTxn(connID, query)
	if err != nil {
		_, writeErr := conn.Write(serializeErrorResponse(InternalError(err.Error())))
		return writeErr
	}

	if cols != nil {
		fields := make([]FieldDescription, len(cols))
		for i, col := range cols {
			fields[i] = FieldDescription{
				Name:        col.Name,
				TableOID:    0,
				ColumnIndex: uint16(col.Ordinal),
				TypeOID:     25,
				TypeSize:    -1,
				Format:      0,
			}
		}
		if _, err := conn.Write(serializeRowDescription(fields)); err != nil {
			return err
		}
	} else {
		if _, err := conn.Write(serializeRowDescription(nil)); err != nil {
			return err
		}
	}

	for _, row := range rows {
		values := make([]*string, len(row.Values))
		for j, v := range row.Values {
			text := ValueToPGText(v)
			values[j] = &text
		}
		if _, err := conn.Write(serializeDataRow(values)); err != nil {
			return err
		}
	}

	tag := determineCommandTag(query, affectedRows)
	if _, err := conn.Write(serializeCommandComplete(tag)); err != nil {
		return err
	}

	return nil
}

func (s *Server) connStatus(connID string, hadError bool) QueryStatus {
	state := s.engine.GetConnState(connID)
	if hadError || state == engine.ConnTxnFailed {
		return QueryStatusFailed
	}
	if state == engine.ConnTxnActive {
		return QueryStatusTransactionBlock
	}
	return QueryStatusIdle
}

func determineCommandTag(query string, rowCount uint64) CommandTag {
	upper := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return SelectTag(rowCount)
	case strings.HasPrefix(upper, "INSERT"):
		return InsertTag(rowCount)
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return CreateTableTag()
	default:
		return CommandTag{Command: strings.Fields(upper)[0]}
	}
}

func serializeAuthenticationOK() []byte {
	return []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
}

func serializeReadyForQuery(status QueryStatus) []byte {
	return []byte{'Z', 0, 0, 0, 5, byte(status)}
}

func serializeRowDescription(fields []FieldDescription) []byte {
	var body bytes.Buffer
	_ = binary.Write(&body, binary.BigEndian, uint16(len(fields)))
	for _, field := range fields {
		body.WriteString(field.Name)
		body.WriteByte(0)
		_ = binary.Write(&body, binary.BigEndian, field.TableOID)
		_ = binary.Write(&body, binary.BigEndian, field.ColumnIndex)
		_ = binary.Write(&body, binary.BigEndian, field.TypeOID)
		_ = binary.Write(&body, binary.BigEndian, field.TypeSize)
		_ = binary.Write(&body, binary.BigEndian, field.TypeModifier)
		_ = binary.Write(&body, binary.BigEndian, field.Format)
	}
	return frameMessage('T', body.Bytes())
}

func serializeDataRow(values []*string) []byte {
	var body bytes.Buffer
	_ = binary.Write(&body, binary.BigEndian, uint16(len(values)))
	for _, value := range values {
		if value == nil {
			_ = binary.Write(&body, binary.BigEndian, int32(-1))
			continue
		}
		_ = binary.Write(&body, binary.BigEndian, int32(len(*value)))
		body.WriteString(*value)
	}
	return frameMessage('D', body.Bytes())
}

func serializeCommandComplete(tag CommandTag) []byte {
	var content string
	if tag.Command == "INSERT" && tag.Rows != nil {
		content = fmt.Sprintf("%s %d %d", tag.Command, tag.Oid, *tag.Rows)
	} else if tag.Rows != nil {
		content = fmt.Sprintf("%s %d", tag.Command, *tag.Rows)
	} else {
		content = tag.Command
	}
	return frameMessage('C', append([]byte(content), 0))
}

func serializeErrorResponse(resp ErrorResponse) []byte {
	var body bytes.Buffer
	writeErrorField(&body, 'S', string(resp.Severity))
	writeErrorField(&body, 'C', resp.Code)
	writeErrorField(&body, 'M', resp.Message)
	if resp.Detail != "" {
		writeErrorField(&body, 'D', resp.Detail)
	}
	if resp.Hint != "" {
		writeErrorField(&body, 'H', resp.Hint)
	}
	body.WriteByte(0)
	return frameMessage('E', body.Bytes())
}

func writeErrorField(buf *bytes.Buffer, field byte, value string) {
	buf.WriteByte(field)
	buf.WriteString(value)
	buf.WriteByte(0)
}

func frameMessage(kind byte, payload []byte) []byte {
	out := make([]byte, 1+4+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)+4))
	copy(out[5:], payload)
	return out
}

func ValueToPGText(value types.Value) string {
	return value.PGText()
}
