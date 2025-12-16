// NyanTEST (single-file CLI for SQL template tests, v0.0.1)
// - NyanQL風テンプレSQLをレンダリング→expectedと比較→DBで実行（既定は ROLLBACK）
// - params は JSONC（コメント/末尾カンマOK）の「ファイルパス」または「インラインオブジェクト」
// - test.json で複数テストを定義（各種パスは test.json からの相対パス）
// - -auto-params / -auto-expected / -snapshot-update による自動生成・更新に対応
// - JUnit XML レポート出力（-junit-out）
// - -readonly で可能なDBは読み取り専用を強制、-noexec でDB実行をスキップ
//
// ★本版の実行結果表示は phpunit 風に変更：
//
//	・成功: '.'、失敗(アサーション): 'F'、実行エラー: 'E' を進捗として出力
//	・詳細は E/F のみ最後にまとめて表示（成功ケースの詳細は表示しない）
//
// さらに **SQLジェネレータを同梱**：
// サブコマンド:
//
//	gen-sql  … .sql を走査して *.test.jsonc と expected を生成（任意で -combine で test.json 作成）
//	           IF/BEGIN/OPTIONAL ブロックから「パラメータ有/無」のバリアントも自動生成（キー名ベース）
//	combine  … *.test.jsonc をまとめ直して test.json を生成
//
// ビルド:
//
//	go build -o NyanTEST .
//
// 生成例:
//
//	./NyanTEST gen-sql \
//	  -src ./sql -out ./sql/jsonc -expected ./sql/expected -combine ./sql/test.json -auto-expected
//
// 実行例（phpunit風出力）:
//
//	./NyanTEST -config ./sql/test.json -nyanconf ./config.json
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	// DB drivers
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // driver name "pgx" (postgres)
	_ "github.com/marcboeker/go-duckdb"
	_ "modernc.org/sqlite"
)

/* ============== Version ============== */

const Version = "0.0.1"

/* ============== Usage text (for -help/-h) ============== */

const usageText = `
NyanTEST - NyanQL-style SQL template tester (with SQL generator)

SUBCOMMANDS
  gen-sql  Generate SQL test templates (1 sql => 1..N *.test.jsonc), optional -combine test.json
  combine  Combine *.test.jsonc into a single test.json

RUNNER (default when no subcommand):
  - Render templated SQL with parameters (JSONC allowed) and compare to expected
  - Execute on DB in a single transaction (default: rollback)
  - Generate missing params/expected with -auto-params / -auto-expected
  - Update expected with -snapshot-update
  - Output JUnit XML with -junit-out
  - phpunit-like progress: '.' (pass), 'F' (assertion failure), 'E' (error)
`

/* ============== Types (Runner) ============== */

type TestCase struct {
	Name         string
	SQLPath      string
	Expected     string
	Seed         string
	ParamsPath   string         // JSONC path（test.json からの相対）
	ParamsInline map[string]any // inline object
	ActualOut    string         // rendered SQL の出力先（任意）
}

type LogConfig struct {
	Filename      string `json:"Filename"`
	MaxSize       int    `json:"MaxSize"`
	MaxBackups    int    `json:"MaxBackups"`
	MaxAge        int    `json:"MaxAge"`
	Compress      bool   `json:"Compress"`
	EnableLogging bool   `json:"EnableLogging"`
}
type BasicAuth struct {
	Username string `json:"Username"`
	Password string `json:"Password"`
}
type NyanConfig struct {
	Name                   string     `json:"name"`
	Profile                string     `json:"profile"`
	Version                string     `json:"version"`
	Port                   int        `json:"Port"`
	BasicAuth              *BasicAuth `json:"BasicAuth"`
	CertPath               string     `json:"certPath"`
	KeyPath                string     `json:"keyPath"`

	DBType                 string     `json:"DBType"`
	DBUser                 string     `json:"DBUser"`
	DBPassword             string     `json:"DBPassword"`
	DBName                 string     `json:"DBName"`
	DBHost                 string     `json:"DBHost"`
	DBPort                 string     `json:"DBPort"`

	MaxOpenConnections     int        `json:"MaxOpenConnections"`
	MaxIdleConnections     int        `json:"MaxIdleConnections"`
	ConnMaxLifetimeSeconds int        `json:"ConnMaxLifetimeSeconds"`

	Log               *LogConfig `json:"log"`
	JavascriptInclude []string   `json:"javascript_include"`
}

/* ============== JUnit (Runner) ============== */

type junitSuite struct {
	XMLName  xml.Name    `xml:"testsuite"`
	Name     string      `xml:"name,attr"`
	Tests    int         `xml:"tests,attr"`
	Failures int         `xml:"failures,attr"`
	Errors   int         `xml:"errors,attr"`
	Skipped  int         `xml:"skipped,attr"`
	Time     string      `xml:"time,attr"`
	Cases    []junitCase `xml:"testcase"`
}
type junitCase struct {
	Name    string     `xml:"name,attr"`
	Time    string     `xml:"time,attr"`
	Failure *junitFail `xml:"failure,omitempty"`
	Error   *junitErr  `xml:"error,omitempty"`
}
type junitFail struct {
	Message string `xml:"message,attr,omitempty"`
	Type    string `xml:"type,attr,omitempty"`
	Text    string `xml:",chardata"`
}
type junitErr struct {
	Message string `xml:"message,attr,omitempty"`
	Type    string `xml:"type,attr,omitempty"`
	Text    string `xml:",chardata"`
}

/* ============== Flags (Runner) ============== */

var (
	configPath     string // test.json
	nyanConf       string // NyanQL config.json
	driver         string // sqlite|mysql|postgres|duckdb (flag優先)
	dsn            string // flag優先
	globalSeed     string
	noexec         bool
	timeoutSec     int
	printSQL       bool
	doCommit       bool
	readOnly       bool
	showVersion    bool
	autoParams     bool
	autoExpected   bool
	snapshotUpdate bool
	junitOut       string

	onlyList string
	runRegex string
)

func init() {
	flag.StringVar(&configPath, "config", "test.json", "path to test.json (combined)")
	flag.StringVar(&nyanConf, "nyanconf", "", "path to NyanQL-like config.json (DB settings)")
	flag.StringVar(&driver, "driver", "", "db driver override: sqlite|mysql|postgres|duckdb")
	flag.StringVar(&dsn, "dsn", "", "DB DSN override")
	flag.StringVar(&globalSeed, "seed", "", "optional global seed.sql (executed inside test transaction)")

	flag.BoolVar(&noexec, "noexec", false, "render+compare only; skip DB execution")
	flag.IntVar(&timeoutSec, "timeout", 15, "DB execution timeout seconds")
	flag.BoolVar(&printSQL, "print-sql", false, "include rendered SQL in E/F details")
	flag.BoolVar(&doCommit, "commit", false, "commit after execution (default: rollback)")
	flag.BoolVar(&readOnly, "readonly", false, "enforce READ ONLY (where supported); writes will error")
	flag.BoolVar(&showVersion, "version", false, "print NyanTEST version and exit")

	flag.BoolVar(&autoParams, "auto-params", false, "generate params JSONC if missing (SQL placeholders -> empty values)")
	flag.BoolVar(&autoExpected, "auto-expected", false, "generate expected SQL if missing (rendered result)")
	flag.BoolVar(&snapshotUpdate, "snapshot-update", false, "always overwrite expected with current rendered SQL")
	flag.StringVar(&junitOut, "junit-out", "", "write a JUnit XML report to this path")

	flag.StringVar(&onlyList, "only", "", `comma-separated test names to run (e.g. "test1,test3")`)
	flag.StringVar(&runRegex, "run", "", `regular expression to select tests by name (e.g. "^group:")`)

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usageText)
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr)
	}
}

/* ============== main ============== */

func main() {
	// subcommands
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "gen-sql":
			genSQLCmd(os.Args[2:])
			return
		case "combine":
			combineCmd(os.Args[2:])
			return
		}
	}

	flag.Parse()

	if showVersion {
		fmt.Printf("NyanTEST version %s\n", Version)
		return
	}

	cfgDir := "."
	if abs, err := filepath.Abs(configPath); err == nil {
		cfgDir = filepath.Dir(abs)
	}

	tests, err := loadTests(configPath, cfgDir)
	dieIf(err)

	tests = filterTests(tests, onlyList, runRegex)
	if len(tests) == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: no tests matched by -only / -run filters")
		os.Exit(1)
	}

	var conf *NyanConfig
	if nyanConf != "" {
		b, err := os.ReadFile(nyanConf)
		dieIf(err)
		var c NyanConfig
		s := stripTrailingCommas(stripJSONC(string(b)))
		dieIf(json.Unmarshal([]byte(s), &c))
		conf = &c
	}

	drv, effDSN, err := resolveDB(conf, driver, dsn)
	dieIf(err)

	// ヘッダ（軽め）
	fmt.Printf("NyanTEST: %d test(s)\n", len(tests))
	fmt.Printf("driver=%s dsn=%s\n", drv, maskPassword(effDSN))
	if !doCommit {
		fmt.Println("note: transaction ROLLBACK (no persistent changes)")
	} else {
		fmt.Println("note: COMMIT enabled")
	}
	if readOnly {
		fmt.Println("note: READ ONLY (best-effort)")
	}
	fmt.Println()

	fail := 0
	errCount := 0
	var cases []junitCase
	startSuite := time.Now()

	// 失敗/エラーの詳細を最後に出すためバッファ
	type detail struct {
		name    string
		kind    string // "F" or "E"
		timeSec float64
		text    string // メッセージ＋差分等
	}
	var details []detail

	// 進捗行（phpunit風）。※ここでは per-test の見出しや成功メッセージは一切出さない
	for _, tc := range tests {
		t0 := time.Now()
		actual, e := runOne(tc, cfgDir, drv, effDSN, conf)

		switch classifyErr(e) {
		case "F":
			fail++
			fmt.Print("F")
			msg := e.Error()
			if printSQL && strings.TrimSpace(actual) != "" {
				msg += "\n--- Rendered SQL ---\n" + actual + "\n--------------------"
			}
			details = append(details, detail{
				name:    tc.Name,
				kind:    "F",
				timeSec: time.Since(t0).Seconds(),
				text:    msg,
			})
			cases = append(cases, junitCase{
				Name: tc.Name, Time: fmt.Sprintf("%.3f", time.Since(t0).Seconds()),
				Failure: &junitFail{Message: "SQL mismatch", Type: "AssertionError", Text: msg},
			})
		case "E":
			errCount++
			fmt.Print("E")
			msg := e.Error()
			if printSQL && strings.TrimSpace(actual) != "" {
				msg += "\n--- Rendered SQL ---\n" + actual + "\n--------------------"
			}
			details = append(details, detail{
				name:    tc.Name,
				kind:    "E",
				timeSec: time.Since(t0).Seconds(),
				text:    msg,
			})
			cases = append(cases, junitCase{
				Name: tc.Name, Time: fmt.Sprintf("%.3f", time.Since(t0).Seconds()),
				Error: &junitErr{Message: "Test execution error", Type: "Error", Text: msg},
			})
		default:
			fmt.Print(".")
			cases = append(cases, junitCase{
				Name: tc.Name, Time: fmt.Sprintf("%.3f", time.Since(t0).Seconds()),
			})
		}
	}

	fmt.Println() // 進捗行の改行

	// 失敗/エラー詳細（成功ケースの詳細は出さない）
	if fail > 0 {
		fmt.Printf("\nFailures (%d):\n", fail)
		i := 1
		for _, d := range details {
			if d.kind != "F" {
				continue
			}
			fmt.Printf("%d) %s (%.3fs)\n%s\n\n", i, d.name, d.timeSec, d.text)
			i++
		}
	}
	if errCount > 0 {
		fmt.Printf("\nErrors (%d):\n", errCount)
		i := 1
		for _, d := range details {
			if d.kind != "E" {
				continue
			}
			fmt.Printf("%d) %s (%.3fs)\n%s\n\n", i, d.name, d.timeSec, d.text)
			i++
		}
	}

	// サマリ
	fmt.Printf("Time: %.3fs, Tests: %d, Failures: %d, Errors: %d\n",
		time.Since(startSuite).Seconds(), len(tests), fail, errCount)

	// JUnit
	if strings.TrimSpace(junitOut) != "" {
		suite := junitSuite{
			Name:     "NyanTEST",
			Tests:    len(cases),
			Failures: fail,
			Errors:   errCount,
			Skipped:  0,
			Time:     fmt.Sprintf("%.3f", time.Since(startSuite).Seconds()),
			Cases:    cases,
		}
		if err := writeJUnit(junitOut, suite); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: failed to write JUnit report: %v\n", err)
		} else {
			fmt.Printf("JUnit report written: %s\n", junitOut)
		}
	}

	if fail+errCount > 0 {
		os.Exit(1)
	}
}

// 'F'（期待と不一致）/ 'E'（その他エラー）/ ''（成功）
func classifyErr(e error) string {
	if e == nil {
		return ""
	}
	if strings.HasPrefix(e.Error(), "SQL mismatch:") {
		return "F"
	}
	return "E"
}

/* ============== Test filtering (Runner) ============== */

func filterTests(all []TestCase, onlyCSV, regex string) []TestCase {
	if strings.TrimSpace(onlyCSV) == "" && strings.TrimSpace(regex) == "" {
		out := append([]TestCase(nil), all...)
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out
	}
	allow := map[string]struct{}{}
	if onlyCSV != "" {
		for _, n := range strings.Split(onlyCSV, ",") {
			n = strings.TrimSpace(n)
			if n != "" {
				allow[n] = struct{}{}
			}
		}
	}
	var re *regexp.Regexp
	if regex != "" {
		var err error
		re, err = regexp.Compile(regex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: invalid -run regex: %v\n", err)
			os.Exit(1)
		}
	}

	var out []TestCase
	for _, t := range all {
		ok := true
		if len(allow) > 0 {
			_, ok = allow[t.Name]
		}
		if ok && re != nil {
			ok = re.MatchString(t.Name)
		}
		if ok {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

/* ============== Runner core ============== */

func runOne(tc TestCase, cfgDir, drvName, effDSN string, conf *NyanConfig) (string, error) {
	// 1) SQLテンプレ読み込み
	tplBytes, err := os.ReadFile(tc.SQLPath)
	if err != nil {
		return "", fmt.Errorf("read sql: %w", err)
	}

	// 2) params 読み込み（JSONC or inline）。-auto-params サポート
	var paramsBytes []byte
	if tc.ParamsInline != nil {
		b, err := json.Marshal(tc.ParamsInline)
		if err != nil {
			return "", fmt.Errorf("marshal inline params: %w", err)
		}
		paramsBytes = b
	} else {
		b, err := os.ReadFile(tc.ParamsPath)
		if err != nil {
			if os.IsNotExist(err) && autoParams && strings.TrimSpace(tc.ParamsPath) != "" {
				if e := autoGenParamsJSONC(tc.ParamsPath, string(tplBytes)); e != nil {
					return "", fmt.Errorf("auto-gen params: %w", e)
				}
				b, err = os.ReadFile(tc.ParamsPath)
			}
		}
		if err != nil {
			return "", fmt.Errorf("read params: %w", err)
		}
		paramsBytes = b
	}

	// 3) params デコード（JSONC対応）
	params, err := decodeParams(paramsBytes)
	if err != nil {
		return "", fmt.Errorf("decode params: %w", err)
	}

	// 4) レンダリング
	actualSQL, err := renderNyanSQL(string(tplBytes), params)
	if err != nil {
		return "", fmt.Errorf("render: %w", err)
	}
	// （成功ケースでは何も出力しない。詳細は最終まとめで E/F のみ）

	// optional: write actual
	if tc.ActualOut != "" {
		_ = os.WriteFile(tc.ActualOut, []byte(actualSQL), 0o644)
	}

	// 5) expected 読み込み/生成/更新/比較
	expBytes, err := os.ReadFile(tc.Expected)
	if err != nil && !os.IsNotExist(err) {
		return actualSQL, fmt.Errorf("read expected: %w", err)
	}
	if os.IsNotExist(err) && autoExpected {
		if err := os.MkdirAll(filepath.Dir(tc.Expected), 0o755); err != nil {
			return actualSQL, fmt.Errorf("make expected dir: %w", err)
		}
		if err := os.WriteFile(tc.Expected, []byte(addNewline(actualSQL)), 0o644); err != nil {
			return actualSQL, fmt.Errorf("write expected: %w", err)
		}
	} else if snapshotUpdate && err == nil {
		if err := os.WriteFile(tc.Expected, []byte(addNewline(actualSQL)), 0o644); err != nil {
			return actualSQL, fmt.Errorf("snapshot update failed: %w", err)
		}
	} else if err == nil {
		expectedSQL := string(expBytes)
		if !equalSQL(expectedSQL, actualSQL) {
			return actualSQL, fmt.Errorf("SQL mismatch:\n%s", diff(expectedSQL, actualSQL))
		}
	}

	// 6) DB 実行（-noexec なら終了）
	if noexec {
		return actualSQL, nil
	}

	// seeds: global -> per-test（テスト用トランザクション内で実行）
	var seeds []string
	if globalSeed != "" {
		if b, err := os.ReadFile(rel(filepath.Dir(configPath), globalSeed)); err == nil {
			seeds = append(seeds, string(b))
		} else {
			return actualSQL, fmt.Errorf("read global seed: %w", err)
		}
	}
	if tc.Seed != "" {
		if b, err := os.ReadFile(tc.Seed); err == nil {
			seeds = append(seeds, string(b))
		} else {
			return actualSQL, fmt.Errorf("read per-test seed: %w", err)
		}
	}

	if err := execOnDBTx(actualSQL, drvName, effDSN, seeds, conf, time.Duration(timeoutSec)*time.Second, doCommit, readOnly); err != nil {
		return actualSQL, fmt.Errorf("execute DB: %w", err)
	}
	return actualSQL, nil
}

/* ============== DB config / DSN resolve (Runner) ============== */

func resolveDB(conf *NyanConfig, flagDriver, flagDSN string) (driverName, DSN string, err error) {
	drv := strings.ToLower(strings.TrimSpace(flagDriver))
	if drv == "" && conf != nil {
		drv = strings.ToLower(strings.TrimSpace(conf.DBType))
	}
	if drv == "" {
		drv = "sqlite"
	}

	if flagDSN != "" {
		return mapDriver(drv), flagDSN, nil
	}
	if conf == nil {
		switch drv {
		case "sqlite", "duckdb":
			return mapDriver(drv), ":memory:", nil
		default:
			return "", "", fmt.Errorf("DSN not provided (set -dsn or -nyanconf config.json)")
		}
	}

	switch drv {
	case "mysql":
		host := defaultIfEmpty(conf.DBHost, "127.0.0.1")
		port := defaultIfEmpty(conf.DBPort, "3306")
		user := conf.DBUser
		pass := conf.DBPassword
		name := conf.DBName
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true", user, pass, host, port, name)
		return "mysql", dsn, nil
	case "postgres":
		host := defaultIfEmpty(conf.DBHost, "127.0.0.1")
		port := defaultIfEmpty(conf.DBPort, "5432")
		user := url.QueryEscape(conf.DBUser)
		pass := url.QueryEscape(conf.DBPassword)
		name := conf.DBName
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)
		return "pgx", dsn, nil
	case "duckdb":
		name := conf.DBName
		if name == "" {
			name = ":memory:"
		}
		return "duckdb", name, nil
	case "sqlite":
		name := conf.DBName
		if name == "" {
			name = ":memory:"
		}
		return "sqlite", name, nil
	default:
		return "", "", fmt.Errorf("unsupported DBType: %s", drv)
	}
}

func mapDriver(d string) string {
	if d == "postgres" {
		return "pgx"
	}
	return d
}

func defaultIfEmpty(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func applyPool(db *sql.DB, conf *NyanConfig) {
	if conf == nil {
		return
	}
	if conf.MaxOpenConnections > 0 {
		db.SetMaxOpenConns(conf.MaxOpenConnections)
	}
	if conf.MaxIdleConnections > 0 {
		db.SetMaxIdleConns(conf.MaxIdleConnections)
	}
	if conf.ConnMaxLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(conf.ConnMaxLifetimeSeconds) * time.Second)
	}
}

/* ============== DB Execution (Runner) ============== */

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func execOnDBTx(sqlText, driverName, dsn string, seeds []string, conf *NyanConfig, timeout time.Duration, doCommit, readOnly bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	applyPool(db, conf)

	txOpts := &sql.TxOptions{}
	tx, err := db.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}

	cleanup, roErr := enforceReadOnly(ctx, tx, driverName, readOnly)
	if roErr != nil {
		_ = tx.Rollback()
		return roErr
	}
	defer func() {
		if cleanup != nil {
			_ = cleanup()
		}
	}()

	if len(seeds) > 0 {
		if err := execBatch(ctx, tx, strings.Join(seeds, ";\n")); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("seed: %w", err)
		}
	}

	if err := execBatch(ctx, tx, sqlText); err != nil {
		_ = tx.Rollback()
		return err
	}

	if doCommit {
		return tx.Commit()
	}
	return tx.Rollback()
}

func enforceReadOnly(ctx context.Context, tx *sql.Tx, driverName string, enable bool) (cleanup func() error, err error) {
	if !enable {
		return nil, nil
	}
	switch driverName {
	case "pgx":
		if _, err := tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
			return nil, fmt.Errorf("set read-only (postgres): %w", err)
		}
		return nil, nil
	case "mysql":
		if _, err := tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
			return nil, fmt.Errorf("set read-only (mysql): %w", err)
		}
		return nil, nil
	case "sqlite":
		if _, err := tx.ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
			return nil, fmt.Errorf("set read-only (sqlite PRAGMA query_only=ON): %w", err)
		}
		return func() error {
			_, e := tx.ExecContext(context.Background(), "PRAGMA query_only=OFF")
			return e
		}, nil
	case "duckdb":
		if _, err := tx.ExecContext(ctx, "SET access_mode='READ_ONLY'"); err != nil {
			return nil, nil
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func execBatch(ctx context.Context, ex execer, batch string) error {
	stmts := splitBySemicolon(batch)
	for _, s := range stmts {
		q := strings.TrimSpace(s)
		if q == "" {
			continue
		}
		if _, err := ex.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func splitBySemicolon(s string) []string {
	return strings.Split(s, ";")
}

/* ============== Template Renderer (Runner) ============== */

var (
	// case-insensitive に対応（/*iF*/, /*begin*/ などもOK）
	reBeginEnd  = regexp.MustCompile(`(?i)/\*BEGIN\*/([\s\S]*?)/\*(?:END|ENDIF|FI)\*/`)
	reIfBlock   = regexp.MustCompile(`(?i)/\*IF\s+([^*]+?)\*/([\s\S]*?)/\*(?:END|ENDIF|FI)\*/`)
	reOptBlock  = regexp.MustCompile(`(?i)/\*\?\s*([A-Za-z0-9_]+)\s*\?\*/([\s\S]*?)/\*\?\s*\*/`)
	// デフォルト値が '...' だけでなく "..." も許容
	reParam = regexp.MustCompile(`(?i)/\*([a-zA-Z0-9_]+)\*/("([^"]*)"|'([^']*)'|[0-9.+-]+|true|false|null)`)
	// BEGIN…END の中身が実質カラなら落とす
	reEmptyWhere = regexp.MustCompile(`^(?:WHERE)?\s*(?:AND|OR)?\s*\(?\s*\)?$`)
)

// 処理順: 1) /*? key ?*/ → 2) /*IF ...*/ → 3) /*BEGIN..END*/ → 4) パラメータ置換 → 5) 整形
func renderNyanSQL(tpl string, params map[string]any) (string, error) {
	sqlText := tpl

	// 1) 可変ブロック: /*? key ?*/ ... /*?*/
	sqlText = reOptBlock.ReplaceAllStringFunc(sqlText, func(m string) string {
		sm := reOptBlock.FindStringSubmatch(m)
		if len(sm) < 3 {
			return ""
		}
		key := strings.TrimSpace(sm[1])
		body := sm[2]
		if isTruthy(params[key]) {
			return body
		}
		return ""
	})

	// 2) IF ブロック
	sqlText = reIfBlock.ReplaceAllStringFunc(sqlText, func(m string) string {
		sm := reIfBlock.FindStringSubmatch(m)
		if len(sm) < 3 {
			return ""
		}
		cond := strings.TrimSpace(sm[1])
		body := sm[2]
		if evalCondFlexible(cond, params) {
			return body
		}
		return ""
	})

	// 3) BEGIN…END
	sqlText = reBeginEnd.ReplaceAllStringFunc(sqlText, func(m string) string {
		sm := reBeginEnd.FindStringSubmatch(m)
		if len(sm) < 2 {
			return ""
		}
		inner := sm[1]
		only := normalizeWhitespace(stripComments(inner))
		up := strings.ToUpper(strings.TrimSpace(only))
		if up == "" || reEmptyWhere.MatchString(up) {
			return ""
		}
		return inner
	})

	// 4) パラメータ置換（デフォルトのクォート形式を尊重: '...' or "..."）
	sqlText = reParam.ReplaceAllStringFunc(sqlText, func(m string) string {
		sm := reParam.FindStringSubmatch(m)
		name := sm[1]
		defWhole := sm[2]
		if v, ok := params[name]; ok && v != nil {
			switch vv := v.(type) {
			case bool:
				if vv {
					return "true"
				}
				return "false"
			case float64: // JSON number
				if float64(int64(vv)) == vv {
					return strconv.FormatInt(int64(vv), 10)
				}
				return strconv.FormatFloat(vv, 'f', -1, 64)
			case json.Number:
				if i, err := vv.Int64(); err == nil {
					return strconv.FormatInt(i, 10)
				}
				if f, err := vv.Float64(); err == nil {
					return strconv.FormatFloat(f, 'f', -1, 64)
				}
				return vv.String()
			case string:
				return quoteByDefault(defWhole, vv)
			default:
				return quoteByDefault(defWhole, toString(v))
			}
		}
		return defWhole
	})

	// 5) 整形
	return strings.TrimSpace(normalizeWhitespace(sqlText)), nil
}

func quoteByDefault(defWhole, s string) string {
	if strings.HasPrefix(defWhole, `"`) {
		// double-quote スタイル
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	// single-quote（既定）
	return "'" + strings.ReplaceAll(s, `'`, `''`) + "'"
}

func isTruthy(v any) bool {
	if v == nil {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return strings.TrimSpace(t) != ""
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i != 0
		}
		if f, err := t.Float64(); err == nil {
			return f != 0
		}
		return t.String() != "0"
	case float64:
		return t != 0
	case int, int64, int32, int16, int8:
		return fmt.Sprintf("%v", t) != "0"
	}
	return true
}

// IF 条件の柔軟評価
//   /*IF key*/                 // truthy
//   /*IF key == null*/ / != null
//   /*IF key == 'x'*/ / != 'x' // 文字列（' or " で囲む）
//   /*IF key == 123*/          // 数値
func evalCondFlexible(cond string, params map[string]any) bool {
	c := strings.TrimSpace(cond)
	if !strings.ContainsAny(c, " =!<>") {
		return isTruthy(params[c])
	}
	var op string
	if strings.Contains(c, "==") {
		op = "=="
	} else if strings.Contains(c, "!=") {
		op = "!="
	} else {
		return false
	}
	parts := strings.SplitN(c, op, 2)
	if len(parts) != 2 {
		return false
	}
	lhs := strings.TrimSpace(parts[0])
	rhs := strings.TrimSpace(parts[1])

	var rhsV any
	switch {
	case strings.EqualFold(rhs, "null"):
		rhsV = nil
	case strings.EqualFold(rhs, "true"):
		rhsV = true
	case strings.EqualFold(rhs, "false"):
		rhsV = false
	case (strings.HasPrefix(rhs, "'") && strings.HasSuffix(rhs, "'")) ||
		(strings.HasPrefix(rhs, `"`) && strings.HasSuffix(rhs, `"`)):
		unq := rhs[1 : len(rhs)-1]
		rhsV = strings.ReplaceAll(strings.ReplaceAll(unq, `''`, `'`), `""`, `"`)
	default:
		if i, err := strconv.ParseInt(rhs, 10, 64); err == nil {
			rhsV = i
		} else if f, err := strconv.ParseFloat(rhs, 64); err == nil {
			rhsV = f
		} else {
			return false
		}
	}

	lv, exists := params[lhs]
	if !exists {
		lv = nil
	}

	eq := func(a, b any) bool {
		switch aa := a.(type) {
		case json.Number:
			if bb, ok := b.(int); ok {
				if i, err := aa.Int64(); err == nil {
					return i == int64(bb)
				}
			}
			if bb, ok := b.(int64); ok {
				if i, err := aa.Int64(); err == nil {
					return i == bb
				}
			}
			if bb, ok := b.(float64); ok {
				if f, err := aa.Float64(); err == nil {
					return f == bb
				}
			}
			return aa.String() == fmt.Sprintf("%v", b)
		case string:
			return aa == fmt.Sprintf("%v", b)
		case bool:
			if bb, ok := b.(bool); ok {
				return aa == bb
			}
			return fmt.Sprintf("%v", aa) == fmt.Sprintf("%v", b)
		case int, int64, float64:
			return fmt.Sprintf("%v", aa) == fmt.Sprintf("%v", b)
		default:
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
		}
	}

	switch op {
	case "==":
		return eq(lv, rhsV)
	case "!=":
		return !eq(lv, rhsV)
	}
	return false
}

/* ============== Comparison Helpers (Runner) ============== */

func normalizeForCompare(s string) string {
	s = stripComments(s)
	reSpace := regexp.MustCompile(`\s+`)
	s = reSpace.ReplaceAllString(s, " ")
	s = strings.ReplaceAll(s, "( ", "(")
	s = strings.ReplaceAll(s, " )", ")")
	s = strings.ReplaceAll(s, " ,", ",")
	s = strings.ReplaceAll(s, " ;", ";")
	return strings.TrimSpace(s)
}
func equalSQL(a, b string) bool {
	return normalizeForCompare(a) == normalizeForCompare(b)
}
func diff(expected, actual string) string {
	e := normalizeForCompare(expected)
	a := normalizeForCompare(actual)
	if e == a {
		return ""
	}
	return "- " + e + "\n+ " + a
}

/* ============== JSONC utils (shared) ============== */

func decodeParams(b []byte) (map[string]any, error) {
	clean := stripTrailingCommas(stripJSONC(string(b)))
	dec := json.NewDecoder(strings.NewReader(clean))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

func stripJSONC(s string) string {
	s = strings.TrimPrefix(s, "\uFEFF")
	var b strings.Builder
	b.Grow(len(s))
	inStr, inLine, inBlock, esc := false, false, false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		var next byte
		if i+1 < len(s) {
			next = s[i+1]
		}
		if inLine {
			if c == '\n' || c == '\r' {
				inLine = false
				b.WriteByte(c)
			}
			continue
		}
		if inBlock {
			if c == '*' && next == '/' {
				inBlock = false
				i++
			}
			continue
		}
		if inStr {
			b.WriteByte(c)
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
			continue
		}
		if c == '"' {
			inStr = true
			b.WriteByte(c)
			continue
		}
		if c == '/' && next == '/' {
			inLine = true
			i++
			continue
		}
		if c == '/' && next == '*' {
			inBlock = true
			i++
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func stripTrailingCommas(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	inStr := false
	esc := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inStr {
			out.WriteByte(c)
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
			continue
		}
		if c == '"' {
			inStr = true
			out.WriteByte(c)
			continue
		}
		if c == ',' {
			j := i + 1
			for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
				j++
			}
			if j < len(s) && (s[j] == ']' || s[j] == '}') {
				continue
			}
		}
		out.WriteByte(c)
	}
	return out.String()
}

/* ============== test.json loader (Runner) ============== */

func loadTests(path, cfgDir string) ([]TestCase, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw map[string]map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("invalid test.json: %w", err)
	}
	names := make([]string, 0, len(raw))
	for k := range raw {
		names = append(names, k)
	}
	sort.Strings(names)

	out := make([]TestCase, 0, len(raw))
	for _, name := range names {
		v := raw[name]
		tc := TestCase{Name: name}

		tc.SQLPath = pickString(v, "sql")
		if tc.SQLPath == "" {
			tc.SQLPath = pickString(v, "SQL")
		}
		tc.Expected = pickString(v, "expected")
		if tc.Expected == "" {
			tc.Expected = pickString(v, "結果として出力される予定のSQLフのファイルのパス")
		}
		tc.Seed = pickString(v, "seed")

		if p, ok := v["params"]; ok {
			switch pv := p.(type) {
			case string:
				pp := strings.TrimSpace(pv)
				pp = strings.TrimPrefix(pp, "config:")
				tc.ParamsPath = rel(cfgDir, pp)
			case map[string]any:
				tc.ParamsInline = pv
			default:
				return nil, fmt.Errorf("test '%s' has invalid 'params' (string path or object expected)", name)
			}
		} else if p2, ok := v["リクエストするJSON"]; ok {
			switch pv := p2.(type) {
			case string:
				tc.ParamsPath = rel(cfgDir, pv)
			case map[string]any:
				tc.ParamsInline = pv
			default:
				return nil, fmt.Errorf("test '%s' has invalid 'リクエストするJSON'", name)
			}
		}

		tc.ActualOut = pickString(v, "actual")
		if tc.ActualOut == "" {
			tc.ActualOut = pickString(v, "out")
		}

		if tc.SQLPath == "" || tc.Expected == "" {
			return nil, fmt.Errorf("test '%s' missing sql/expected", name)
		}
		if tc.ParamsPath == "" && tc.ParamsInline == nil {
			return nil, fmt.Errorf("test '%s' missing params (set a file path or an inline object)", name)
		}

		tc.SQLPath = rel(cfgDir, tc.SQLPath)
		tc.Expected = rel(cfgDir, tc.Expected)
		if tc.Seed != "" {
			tc.Seed = rel(cfgDir, tc.Seed)
		}
		if tc.ActualOut != "" {
			tc.ActualOut = rel(cfgDir, tc.ActualOut)
		}

		out = append(out, tc)
	}
	return out, nil
}

func pickString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func rel(base, p string) string {
	if filepath.IsAbs(p) || p == "" {
		return p
	}
	return filepath.Join(base, p)
}

/* ============== Params auto-generation (Runner) ============== */

var rePHKeys = regexp.MustCompile(`/\*([A-Za-z0-9_]+)\*/'[^']*'|"[^"]*"`)
var rePHKeysStrict = regexp.MustCompile(`/\*([A-Za-z0-9_]+)\*/('(?:[^']*)'|"(?:[^"]*)")`)
var reBlockKeys = regexp.MustCompile(`(?i)/\*\?\s*([A-Za-z0-9_]+)\s*\?\*/`)

// 追加：SQL中の /*key*/<literal> からデフォルト値を取り出す
func extractParamDefaults(sqlText string) map[string]any {
	m := map[string]any{}
	for _, sm := range reParam.FindAllStringSubmatch(sqlText, -1) {
		if len(sm) < 3 {
			continue
		}
		name := sm[1]
		defWhole := sm[2]
		low := strings.ToLower(defWhole)

		switch {
		case strings.HasPrefix(defWhole, "'") && strings.HasSuffix(defWhole, "'"):
			v := defWhole[1 : len(defWhole)-1]
			v = strings.ReplaceAll(v, "''", "'")
			m[name] = v
		case strings.HasPrefix(defWhole, `"`) && strings.HasSuffix(defWhole, `"`):
			v := defWhole[1 : len(defWhole)-1]
			v = strings.ReplaceAll(v, `""`, `"`)
			m[name] = v
		case low == "true":
			m[name] = true
		case low == "false":
			m[name] = false
		case low == "null":
			m[name] = nil
		default:
			// 数値（整数/小数）として解釈。失敗したら文字列として保持
			if i, err := strconv.ParseInt(defWhole, 10, 64); err == nil {
				m[name] = i
			} else if f, err := strconv.ParseFloat(defWhole, 64); err == nil {
				m[name] = f
			} else {
				m[name] = defWhole
			}
		}
	}
	return m
}

func autoGenParamsJSONC(path string, sqlText string) error {
	defs := extractParamDefaults(sqlText)

	keys := map[string]struct{}{}
	for _, m := range rePHKeysStrict.FindAllStringSubmatch(sqlText, -1) {
		if len(m) >= 2 {
			keys[m[1]] = struct{}{}
		}
	}
	for _, m := range reBlockKeys.FindAllStringSubmatch(sqlText, -1) {
		if len(m) >= 2 {
			keys[m[1]] = struct{}{}
		}
	}

	params := map[string]any{}
	for k := range keys {
		if v, ok := defs[k]; ok {
			params[k] = v
		} else {
			params[k] = "" // デフォルトが無い可変キーは空
		}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		return err
	}
	return writeParamsJSONC(path, params, "Auto-generated from SQL defaults by -auto-params")
}

/* ============== Generator: gen-sql / combine ============== */

type TestDef struct {
	Name        string         `json:"name,omitempty"`
	Tags        []string       `json:"tags,omitempty"`
	SQL         string         `json:"sql,omitempty"`
	Params      any            `json:"params,omitempty"`    // string path or object
	Expected    string         `json:"expected"`
	Normalize   map[string]any `json:"normalize,omitempty"` // e.g. {"sqlFmt": true}
	Description string         `json:"description,omitempty"`
	Seed        string         `json:"seed,omitempty"`
}

func genSQLCmd(args []string) {
	fs := flag.NewFlagSet("gen-sql", flag.ExitOnError)
	var srcDir, outDir, expDir, combineOut string
	var overwrite, autoExp bool
	fs.StringVar(&srcDir, "src", "./sql", "directory containing SQL files")
	fs.StringVar(&outDir, "out", "./tests-sql", "directory to write *.test.jsonc")
	fs.StringVar(&expDir, "expected", "./expected-sql", "directory to write expected rendered SQL")
	fs.StringVar(&combineOut, "combine", "", "write combined test.json here (optional)")
	fs.BoolVar(&overwrite, "overwrite", false, "overwrite existing test files")
	fs.BoolVar(&autoExp, "auto-expected", false, "render and write expected for each test if missing (or when -overwrite)")
	_ = fs.Parse(args)

	die(os.MkdirAll(outDir, 0o755))
	die(os.MkdirAll(expDir, 0o755))
	paramsDir := filepath.Join(outDir, "_params")
	die(os.MkdirAll(paramsDir, 0o755))

	// collect
	var files []string
	die(filepath.Walk(srcDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(p), ".sql") {
			files = append(files, p)
		}
		return nil
	}))
	sort.Strings(files)
	if len(files) == 0 {
		fmt.Println("No .sql files found under", srcDir)
		return
	}

	for _, sqlPath := range files {
		sqlBase := strings.TrimSuffix(filepath.Base(sqlPath), ".sql")
		sqlContentB, err := os.ReadFile(sqlPath)
		die(err)
		sqlContent := string(sqlContentB)

		// ベース（パラメータ空）テスト ---------------
		outPath := filepath.Join(outDir, sqlBase+".test.jsonc")
		paramPath := filepath.Join(paramsDir, sqlBase+".params.jsonc")
		expPath := filepath.Join(expDir, sqlBase+".expected.sql")

		if overwrite || fileNotExists(outPath) {
			params := guessParamsFromSQL(sqlContent)
			if _, err := os.Stat(paramPath); os.IsNotExist(err) || overwrite {
				die(writeParamsJSONC(paramPath, params, "Auto-generated from SQL placeholders"))
			}
			if autoExp {
				pb, _ := os.ReadFile(paramPath)
				pm, _ := decodeParams(pb)
				rendered, err := renderNyanSQL(sqlContent, pm)
				die(err)
				die(os.MkdirAll(filepath.Dir(expPath), 0o755))
				die(os.WriteFile(expPath, []byte(addNewline(rendered)), 0o644))
			} else {
				if fileNotExists(expPath) {
					_ = os.WriteFile(expPath, []byte("-- filled by snapshot update\n"), 0o644)
				}
			}
			td := TestDef{
				Name:        sqlBase,
				SQL:         relFrom(outDir, sqlPath),
				Params:      relFrom(outDir, paramPath),
				Expected:    relFrom(outDir, expPath),
				Normalize:   map[string]any{"sqlFmt": true},
				Tags:        []string{"auto", "sql"},
				Description: "Auto-generated by NyanTEST gen-sql",
			}
			die(writeJSONC(outPath, td))
			fmt.Println("generated:", outPath)
		} else {
			fmt.Println("skip (exists):", outPath)
		}

		// 条件付き（IF/OPTIONAL）由来の「有値バリアント」を追加 ---------------
		condKeys := findTruthyKeysForVariants(sqlContent)
		for _, key := range condKeys {
			name2 := fmt.Sprintf("%s__%s", sqlBase, key)
			outPath2 := filepath.Join(outDir, name2+".test.jsonc")
			paramPath2 := filepath.Join(paramsDir, name2+".params.jsonc")
			expPath2 := filepath.Join(expDir, name2+".expected.sql")

			if !overwrite {
				if _, err := os.Stat(outPath2); err == nil {
					fmt.Println("skip (exists):", outPath2)
					continue
				}
			}

			// ベース params を読み、該当キーにダミー値を設定
			baseParams := guessParamsFromSQL(sqlContent)
			for k := range baseParams {
				// 既にデフォルトがあるものはそのまま、無いものは空
				if baseParams[k] == nil {
					baseParams[k] = ""
				}
			}
			baseParams[key] = sampleValueForKey(key)

			die(writeParamsJSONC(paramPath2, baseParams, "Auto-generated variant (truthy) for "+key))

			if autoExp {
				pm, _ := decodeParams([]byte(toJSONString(baseParams)))
				rendered, err := renderNyanSQL(sqlContent, pm)
				die(err)
				die(os.MkdirAll(filepath.Dir(expPath2), 0o755))
				die(os.WriteFile(expPath2, []byte(addNewline(rendered)), 0o644))
			} else {
				if fileNotExists(expPath2) {
					_ = os.WriteFile(expPath2, []byte("-- filled by snapshot update\n"), 0o644)
				}
			}

			td2 := TestDef{
				Name:        name2,
				SQL:         relFrom(outDir, sqlPath),
				Params:      relFrom(outDir, paramPath2),
				Expected:    relFrom(outDir, expPath2),
				Normalize:   map[string]any{"sqlFmt": true},
				Tags:        []string{"auto", "sql", "variant"},
				Description: "Auto-generated truthy variant by NyanTEST gen-sql",
			}
			die(writeJSONC(outPath2, td2))
			fmt.Println("generated:", outPath2)
		}
	}

	if strings.TrimSpace(combineOut) != "" {
		die(combineTests(outDir, combineOut))
		fmt.Println("combined:", combineOut)
	}

	fmt.Printf("Done. You can now run:\n  NyanTEST -config %s\n", firstNonEmpty(combineOut, outDir))
}

func combineCmd(args []string) {
	fs := flag.NewFlagSet("combine", flag.ExitOnError)
	var inDir, outFile string
	fs.StringVar(&inDir, "in", "./tests-sql", "directory containing *.test.jsonc")
	fs.StringVar(&outFile, "out", "./tests-sql/test.json", "output combined test.json path")
	_ = fs.Parse(args)

	die(combineTests(inDir, outFile))
	fmt.Println("combined:", outFile)
}

func combineTests(inDir, outFile string) error {
	fi, err := os.Stat(inDir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.New("combine: -in must be a directory")
	}

	var testFiles []string
	err = filepath.Walk(inDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		low := strings.ToLower(p)
		if strings.HasSuffix(low, ".test.jsonc") || strings.HasSuffix(low, ".test.json") {
			testFiles = append(testFiles, p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(testFiles)
	if len(testFiles) == 0 {
		return fmt.Errorf("combine: no *.test.jsonc found under %s", inDir)
	}

	combined := map[string]any{}
	nameUsed := map[string]struct{}{}

	outDir := filepath.Dir(outFile)
	paramsDir := filepath.Join(outDir, "_params") // 必要時に writeParamsJSONC が作成

	for _, f := range testFiles {
		td, err := readTestDef(f)
		if err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}

		key := strings.TrimSpace(td.Name)
		if key == "" {
			key = strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
			key = strings.TrimSuffix(key, ".test")
		}
		base := key
		i := 2
		for {
			if _, ok := nameUsed[key]; !ok {
				break
			}
			key = fmt.Sprintf("%s__%d", base, i)
			i++
		}
		nameUsed[key] = struct{}{}

		td.Expected = relFrom(outDir, absFrom(filepath.Dir(f), td.Expected))
		if td.SQL != "" {
			td.SQL = relFrom(outDir, absFrom(filepath.Dir(f), td.SQL))
		}

		switch pv := td.Params.(type) {
		case string:
			s := strings.TrimSpace(pv)
			s = strings.TrimPrefix(s, "config:")
			td.Params = relFrom(outDir, absFrom(filepath.Dir(f), s))
		case map[string]any:
			filename := safeName(base) + ".params.jsonc"
			pp := filepath.Join(paramsDir, filename)
				// インライン params を型維持で JSONC 化
			if err := writeParamsJSONC(pp, pv, "Converted from inline params by combine"); err != nil {
				return err
			}
			td.Params = relFrom(outDir, pp)
		case nil:
		default:
		}

		combined[key] = td
	}

	b, err := json.MarshalIndent(combined, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil && !os.IsExist(err) {
		return err
	}
	return os.WriteFile(outFile, b, 0o644)
}

/* ============== Generator helpers ============== */

func guessParamsFromSQL(sqlContent string) map[string]any {
	// デフォルト値を抽出
	defs := extractParamDefaults(sqlContent)

	// 可変ブロック部から（未定義なら空値を追加）
	for _, m := range reBlockKeys.FindAllStringSubmatch(sqlContent, -1) {
		if len(m) >= 2 {
			if _, ok := defs[m[1]]; !ok {
				defs[m[1]] = ""
			}
		}
	}
	return defs
}

var reIfHead = regexp.MustCompile(`(?i)/\*IF\s+([^*]+?)\*/`)

func findTruthyKeysForVariants(sqlContent string) []string {
	set := map[string]struct{}{}
	// IF 条件からキー候補を抽出
	for _, m := range reIfHead.FindAllStringSubmatch(sqlContent, -1) {
		if len(m) < 2 {
			continue
		}
		cond := m[1]
		for _, w := range regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`).FindAllString(cond, -1) {
			lw := strings.ToLower(w)
			if lw == "null" || lw == "true" || lw == "false" || lw == "and" || lw == "or" || lw == "not" {
				continue
			}
			set[w] = struct{}{}
		}
	}
	// 可変ブロック /*? key ?*/ も truthy セットに加える
	for _, m := range reBlockKeys.FindAllStringSubmatch(sqlContent, -1) {
		if len(m) >= 2 {
			set[m[1]] = struct{}{}
		}
	}
	var keys []string
	for k := range set {
		keys = append(keys, k)
	}
	sort.Sort(sort.StringSlice(keys))
	return keys
}

func sampleValueForKey(key string) any {
	l := strings.ToLower(key)
	switch {
	case strings.Contains(l, "email"):
		return "dummy@example.com"
	case strings.Contains(l, "name") || strings.Contains(l, "user"):
		return "dummyname"
	case strings.HasSuffix(l, "id") || strings.Contains(l, "count") || strings.Contains(l, "num"):
		return 1
	case strings.Contains(l, "date") || strings.HasSuffix(l, "_at"):
		return "2025-01-01 00:00:00"
	default:
		return "x"
	}
}

func toJSONString(m map[string]any) string {
	b, _ := json.Marshal(m)
	return string(b)
}

func writeJSONC(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// 型を維持して JSONC を書き出す
func writeParamsJSONC(path string, params map[string]any, note string) error {
	var lines []string
	lines = append(lines, "// "+note)
	lines = append(lines, "{")
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		comma := ","
		if i == len(keys)-1 {
			comma = ""
		}
		lines = append(lines, fmt.Sprintf(`  // %s`, k))
		lines = append(lines, fmt.Sprintf(`  "%s": %s%s`, k, jsonLiteral(params[k]), comma))
	}
	lines = append(lines, "}")
	content := strings.Join(lines, "\n") + "\n"

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// 値を JSON リテラルへ（数値/真偽/null/文字列/配列/オブジェクト）
func jsonLiteral(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	case string:
		b, _ := json.Marshal(t)
		return string(b)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func readTestDef(path string) (TestDef, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return TestDef{}, err
	}
	s := stripTrailingCommas(stripJSONC(string(b)))
	var td TestDef
	if err := json.Unmarshal([]byte(s), &td); err != nil {
		return TestDef{}, err
	}
	return td, nil
}

func relFrom(fromDir, toPath string) string {
	if filepath.IsAbs(toPath) {
		return toPath
	}
	absTo, _ := filepath.Abs(toPath)
	absFrom, _ := filepath.Abs(fromDir)
	rel, err := filepath.Rel(absFrom, absTo)
	if err != nil {
		return toPath
	}
	return filepath.ToSlash(rel)
}

func absFrom(base, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Clean(filepath.Join(base, p))
}

func safeName(s string) string {
	s = strings.ToLower(s)
	s = regexp.MustCompile(`[^a-z0-9_-]+`).ReplaceAllString(s, "_")
	s = strings.Trim(s, "_-")
	if s == "" {
		s = "test"
	}
	return s
}

func fileNotExists(p string) bool {
	_, err := os.Stat(p)
	return os.IsNotExist(err)
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

/* ============== Misc (shared) ============== */

func stripComments(s string) string {
	var out bytes.Buffer
	i := 0
	for i < len(s) {
		if strings.HasPrefix(s[i:], "/*") {
			j := strings.Index(s[i+2:], "*/")
			if j < 0 {
				break
			}
			i += 2 + j + 2
			continue
		}
		if strings.HasPrefix(s[i:], "--") {
			j := strings.IndexByte(s[i:], '\n')
			if j < 0 {
				break
			}
			i += j + 1
			continue
		}
		out.WriteByte(s[i])
		i++
	}
	return out.String()
}

func normalizeWhitespace(s string) string {
	lines := strings.Split(s, "\n")
	for i := range lines {
		fields := strings.Fields(lines[i])
		lines[i] = strings.Join(fields, " ")
	}
	var buf []string
	for _, ln := range lines {
		if ln == "" && len(buf) > 0 && buf[len(buf)-1] == "" {
			continue
		}
		buf = append(buf, ln)
	}
	return strings.TrimSpace(strings.Join(buf, "\n"))
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		b, _ := json.Marshal(v)
		return string(bytes.Trim(b, `"`))
	}
}

func addNewline(s string) string {
	if strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}

func writeJUnit(path string, suite junitSuite) error {
	out, err := xml.MarshalIndent(suite, "", "  ")
	if err != nil {
		return err
	}
	data := append([]byte(xml.Header), out...)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func maskPassword(s string) string {
	if i := strings.Index(s, "://"); i >= 0 {
		rest := s[i+3:]
		at := strings.Index(rest, "@")
		colon := strings.Index(rest, ":")
		if at > 0 && colon >= 0 && colon < at {
			return s[:i+3] + rest[:colon] + ":***" + rest[at:]
		}
		return s
	}
	if i := strings.Index(s, "@"); i > 0 {
		front := s[:i]
		if c := strings.Index(front, ":"); c > 0 {
			return front[:c] + ":***" + s[i:]
		}
	}
	return s
}

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
func dieIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
