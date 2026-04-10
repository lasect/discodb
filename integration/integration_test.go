package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"discodb/config"
	"discodb/engine"
	"discodb/types"
	"discodb/wire"
)

// ---------------------------------------------------------------------------
// Env helpers
// ---------------------------------------------------------------------------

func requireEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Skipf("skipping: %s not set", name)
	}
	return v
}

// ---------------------------------------------------------------------------
// Wire-protocol client (minimal pg wire)
// ---------------------------------------------------------------------------

type pgConn struct {
	conn   net.Conn
	reader *bufio.Reader
}

func dialPG(t *testing.T, addr string) *pgConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return &pgConn{conn: conn, reader: bufio.NewReader(conn)}
}

func (c *pgConn) startup(t *testing.T) {
	t.Helper()

	// SSLRequest
	buf := make([]byte, 0, 8)
	buf = binary.BigEndian.AppendUint32(buf, 8)
	buf = binary.BigEndian.AppendUint32(buf, 80877103)
	_, err := c.conn.Write(buf)
	if err != nil {
		t.Fatalf("write SSLRequest: %v", err)
	}

	resp := make([]byte, 1)
	if _, err := c.reader.Read(resp); err != nil {
		t.Fatalf("read SSL response: %v", err)
	}

	// StartupMessage (protocol 3.0 = 196608)
	params := []byte("user\x00discodb\x00database\x00discodb\x00\x00")
	startupLen := 4 + 4 + len(params)
	startup := make([]byte, 4+4+len(params))
	binary.BigEndian.PutUint32(startup[0:4], uint32(startupLen))
	binary.BigEndian.PutUint32(startup[4:8], 196608)
	copy(startup[8:], params)
	if _, err := c.conn.Write(startup); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	if _, err := c.conn.Write(startup); err != nil {
		t.Fatalf("write startup: %v", err)
	}

	// Auth OK
	if err := c.readMsg(t); err != nil {
		t.Fatalf("read AuthOK: %v", err)
	}

	// ReadyForQuery
	if err := c.readMsg(t); err != nil {
		t.Fatalf("read ReadyForQuery: %v", err)
	}
}

func (c *pgConn) query(t *testing.T, sql string) (cols []string, rows [][]string, tag string) {
	t.Helper()

	payload := append([]byte(sql), 0)
	msg := make([]byte, 1, 5+len(payload))
	msg[0] = 'Q'
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(payload)+4))
	msg = append(msg, payload...)
	if _, err := c.conn.Write(msg); err != nil {
		t.Fatalf("write query: %v", err)
	}

	for {
		kind, body, err := c.readMsgBody(t)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}

		switch kind {
		case 'T':
			numCols := int(binary.BigEndian.Uint16(body[0:2]))
			off := 2
			for i := 0; i < numCols; i++ {
				nullIdx := bytes.IndexByte(body[off:], 0)
				colName := string(body[off : off+nullIdx])
				cols = append(cols, colName)
				off += nullIdx + 1 + 18
			}

		case 'D':
			numVals := int(binary.BigEndian.Uint16(body[0:2]))
			off := 2
			var row []string
			for i := 0; i < numVals; i++ {
				valLen := int32(binary.BigEndian.Uint32(body[off : off+4]))
				off += 4
				if valLen < 0 {
					row = append(row, "NULL")
				} else {
					row = append(row, string(body[off:off+int(valLen)]))
					off += int(valLen)
				}
			}
			rows = append(rows, row)

		case 'C':
			nullIdx := bytes.IndexByte(body, 0)
			tag = string(body[:nullIdx])

		case 'Z':
			return

		case 'E':
			fields := parseErrorFields(body)
			t.Fatalf("query error [%s]: %s", fields["C"], fields["M"])
		}
	}
}

func (c *pgConn) queryExpectError(t *testing.T, sql string) string {
	t.Helper()

	payload := append([]byte(sql), 0)
	msg := make([]byte, 1, 5+len(payload))
	msg[0] = 'Q'
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(payload)+4))
	msg = append(msg, payload...)
	if _, err := c.conn.Write(msg); err != nil {
		t.Fatalf("write query: %v", err)
	}

	for {
		kind, body, err := c.readMsgBody(t)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}

		switch kind {
		case 'E':
			fields := parseErrorFields(body)
			return fields["M"]

		case 'Z':
			t.Fatal("expected error response but got ReadyForQuery")
			return ""

		case 'T', 'D', 'C':
			// skip, keep reading for error
		}
	}
}

func (c *pgConn) readMsg(t *testing.T) error {
	t.Helper()
	_, _, err := c.readMsgBody(t)
	return err
}

func (c *pgConn) readMsgBody(t *testing.T) (byte, []byte, error) {
	t.Helper()
	kind := make([]byte, 1)
	if _, err := c.reader.Read(kind); err != nil {
		return 0, nil, err
	}
	lenBuf := make([]byte, 4)
	if _, err := c.reader.Read(lenBuf); err != nil {
		return 0, nil, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)
	body := make([]byte, msgLen-4)
	if _, err := c.reader.Read(body); err != nil {
		return 0, nil, err
	}
	return kind[0], body, nil
}

func parseErrorFields(body []byte) map[string]string {
	fields := make(map[string]string)
	off := 0
	for off < len(body) {
		fieldType := body[off]
		off++
		if fieldType == 0 {
			break
		}
		nullIdx := bytes.IndexByte(body[off:], 0)
		if nullIdx < 0 {
			break
		}
		value := string(body[off : off+nullIdx])
		fields[string(fieldType)] = value
		off += nullIdx + 1
	}
	return fields
}

func (c *pgConn) close() {
	c.conn.Write([]byte{'X', 0, 0, 0, 4})
	c.conn.Close()
}

// ---------------------------------------------------------------------------
// Server startup
// ---------------------------------------------------------------------------

func startServer(t *testing.T) (addr string, eng *engine.Engine, cleanup func()) {
	t.Helper()

	cfg := config.Default()
	cfg.Discord.Tokens.WAL = requireEnv(t, "DISCORD_BOT_TOKEN_WAL")
	cfg.Discord.Tokens.Heap = requireEnv(t, "DISCORD_BOT_TOKEN_HEAP")
	cfg.Discord.Tokens.Index = requireEnv(t, "DISCORD_BOT_TOKEN_INDEX")
	cfg.Discord.Tokens.Catalog = requireEnv(t, "DISCORD_BOT_TOKEN_CATALOG")
	cfg.Discord.Tokens.Overflow = requireEnv(t, "DISCORD_BOT_TOKEN_OVERFLOW")

	guildStr := requireEnv(t, "DISCORD_GUILD_ID")
	var guildRaw uint64
	if _, err := fmt.Sscanf(guildStr, "%d", &guildRaw); err != nil {
		t.Fatalf("parse DISCORD_GUILD_ID: %v", err)
	}
	guildID, err := types.NewGuildID(guildRaw)
	if err != nil {
		t.Fatalf("new GuildID: %v", err)
	}
	cfg.Discord.GuildIDs = []types.GuildID{guildID}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	eng, err = engine.NewEngine(cfg, logger)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = ln.Addr().String()
	ln.Close()

	srv := wire.NewServer(addr, logger, eng)
	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe()
	}()

	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cleanup = func() {
		eng.Close()
	}

	_ = done

	return addr, eng, cleanup
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

func assertTag(t *testing.T, tag, expected string) {
	t.Helper()
	if !strings.HasPrefix(tag, expected) {
		t.Errorf("expected tag %q, got %q", expected, tag)
	}
}

func assertRowCount(t *testing.T, rows [][]string, expected int) {
	t.Helper()
	if len(rows) != expected {
		t.Errorf("expected %d rows, got %d: %v", expected, len(rows), rows)
	}
}

func assertRowContains(t *testing.T, rows [][]string, colIdx int, value string) {
	t.Helper()
	for _, row := range rows {
		if colIdx < len(row) && row[colIdx] == value {
			return
		}
	}
	t.Errorf("no row with column %d = %q in %v", colIdx, value, rows)
}

// ---------------------------------------------------------------------------
// Demo queries — the X post test
// ---------------------------------------------------------------------------

func TestIntegration_DemoQueries(t *testing.T) {
	addr, _, cleanup := startServer(t)
	defer cleanup()

	conn := dialPG(t, addr)
	defer conn.close()
	conn.startup(t)

	t.Run("01_create_table_tweets", func(t *testing.T) {
		_, _, tag := conn.query(t, `CREATE TABLE tweets (
			id int4,
			username text,
			content text,
			likes int4
		)`)
		assertTag(t, tag, "CREATE TABLE")
		t.Log("CREATE TABLE tweets")
	})

	t.Run("02_insert_rows", func(t *testing.T) {
		_, _, tag := conn.query(t, `INSERT INTO tweets VALUES
			(1, 'elonmusk', 'buying another company', 42069),
			(2, 'naval', 'wealth is assets that earn while you sleep', 12000),
			(3, 'pmarca', 'software is still eating the world', 8500)`)
		assertTag(t, tag, "INSERT")
		t.Log("INSERT 3 rows")
	})

	t.Run("03_select_all", func(t *testing.T) {
		cols, rows, tag := conn.query(t, `SELECT * FROM tweets`)
		assertTag(t, tag, "SELECT")
		assertRowCount(t, rows, 3)
		t.Logf("SELECT * -> %d rows, columns: %v", len(rows), cols)
	})

	t.Run("04_select_order_by_limit", func(t *testing.T) {
		_, rows, tag := conn.query(t, `SELECT username, content, likes FROM tweets ORDER BY likes DESC LIMIT 2`)
		assertTag(t, tag, "SELECT")
		assertRowCount(t, rows, 2)
		assertRowContains(t, rows, 0, "elonmusk")
		t.Log("ORDER BY likes DESC LIMIT 2")
	})

	t.Run("05_select_where", func(t *testing.T) {
		_, rows, tag := conn.query(t, `SELECT username, content FROM tweets WHERE username = 'naval'`)
		assertTag(t, tag, "SELECT")
		assertRowCount(t, rows, 1)
		assertRowContains(t, rows, 0, "naval")
		t.Log("SELECT WHERE username = 'naval'")
	})

	t.Run("06_update", func(t *testing.T) {
		_, _, tag := conn.query(t, `UPDATE tweets SET likes = 99999 WHERE username = 'naval'`)
		assertTag(t, tag, "UPDATE")
		t.Log("UPDATE naval -> 99999 likes")
	})

	t.Run("07_verify_update", func(t *testing.T) {
		_, rows, _ := conn.query(t, `SELECT username, likes FROM tweets WHERE username = 'naval'`)
		assertRowCount(t, rows, 1)
		assertRowContains(t, rows, 1, "99999")
		t.Log("verified: naval now has 99999 likes")
	})

	t.Run("08_delete", func(t *testing.T) {
		_, _, tag := conn.query(t, `DELETE FROM tweets WHERE username = 'elonmusk'`)
		assertTag(t, tag, "DELETE")
		t.Log("DELETE elonmusk")
	})

	t.Run("09_verify_delete", func(t *testing.T) {
		_, rows, _ := conn.query(t, `SELECT username FROM tweets WHERE username = 'elonmusk'`)
		assertRowCount(t, rows, 0)
		t.Log("verified: elonmusk gone")
	})

	t.Run("10_transaction", func(t *testing.T) {
		conn.query(t, `BEGIN`)
		conn.query(t, `UPDATE tweets SET likes = likes + 1000 WHERE username = 'naval'`)
		conn.query(t, `UPDATE tweets SET likes = likes + 500 WHERE username = 'pmarca'`)
		_, _, tag := conn.query(t, `COMMIT`)
		assertTag(t, tag, "COMMIT")
		t.Log("BEGIN -> 2 UPDATEs -> COMMIT")
	})

	t.Run("11_create_index", func(t *testing.T) {
		_, _, tag := conn.query(t, `CREATE INDEX idx_tweets_username ON tweets (username)`)
		assertTag(t, tag, "CREATE INDEX")
		t.Log("CREATE INDEX on username")
	})

	t.Run("12_select_via_index", func(t *testing.T) {
		_, rows, tag := conn.query(t, `SELECT * FROM tweets WHERE username = 'pmarca'`)
		assertTag(t, tag, "SELECT")
		assertRowCount(t, rows, 1)
		assertRowContains(t, rows, 1, "pmarca")
		t.Log("index scan: SELECT WHERE username = 'pmarca'")
	})

	t.Run("13_drop_table", func(t *testing.T) {
		_, _, tag := conn.query(t, `DROP TABLE tweets`)
		assertTag(t, tag, "DROP TABLE")
		t.Log("DROP TABLE tweets")
	})

	t.Run("14_table_gone", func(t *testing.T) {
		errMsg := conn.queryExpectError(t, `SELECT * FROM tweets`)
		if !strings.Contains(strings.ToLower(errMsg), "not found") {
			t.Errorf("expected 'not found' error, got: %s", errMsg)
		}
		t.Log("verified: tweets table no longer exists")
	})
}

// ---------------------------------------------------------------------------
// Secondary scenario — users table
// ---------------------------------------------------------------------------

func TestIntegration_CreateAndSelectUsers(t *testing.T) {
	addr, _, cleanup := startServer(t)
	defer cleanup()

	conn := dialPG(t, addr)
	defer conn.close()
	conn.startup(t)

	t.Run("create_users", func(t *testing.T) {
		_, _, tag := conn.query(t, `CREATE TABLE users (
			id int4,
			name text,
			email text,
			active bool
		)`)
		assertTag(t, tag, "CREATE TABLE")
	})

	t.Run("insert_users", func(t *testing.T) {
		_, _, tag := conn.query(t, `INSERT INTO users VALUES
			(1, 'Alice', 'alice@example.com', true),
			(2, 'Bob', 'bob@example.com', true),
			(3, 'Charlie', 'charlie@example.com', false)`)
		assertTag(t, tag, "INSERT")
	})

	t.Run("select_active", func(t *testing.T) {
		_, rows, _ := conn.query(t, `SELECT name, email FROM users WHERE active = true`)
		assertRowCount(t, rows, 2)
	})

	t.Run("select_with_limit", func(t *testing.T) {
		_, rows, _ := conn.query(t, `SELECT name FROM users LIMIT 1`)
		assertRowCount(t, rows, 1)
	})
}
