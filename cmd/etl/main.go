package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/etl/internal/luaruntime"
	"example.com/etl/internal/mysqlcdc"
	"example.com/etl/internal/server"
	"example.com/etl/pkg/pipeline"
)

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: etl serve | validate -file pipeline.json")
	}
	switch args[0] {
	case "lua-worker":
		return luaruntime.RunWorker(os.Stdin, out)
	case "validate":
		flags := flag.NewFlagSet("validate", flag.ContinueOnError)
		path := flags.String("file", "", "pipeline JSON file")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *path == "" || flags.NArg() != 0 {
			return fmt.Errorf("-file is required")
		}
		f, err := os.Open(*path)
		if err != nil {
			return err
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
		if err != nil {
			return err
		}
		d, err := pipeline.DecodeJSON(data)
		if err != nil {
			return err
		}
		plan, err := pipeline.Compile(d)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	case "serve":
		flags := flag.NewFlagSet("serve", flag.ContinueOnError)
		addr := flags.String("listen", "127.0.0.1:8080", "loopback HTTP address")
		data := flags.String("data", ".local/etl", "persistent local data directory")
		mysqlAddr := flags.String("mysql-addr", "127.0.0.1:13306", "MySQL address")
		user := flags.String("mysql-user", "etl", "MySQL user")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("unexpected arguments")
		}
		host, _, err := net.SplitHostPort(*addr)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("local MVP only permits numeric loopback listen addresses")
		}
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		s, err := server.Open(context.Background(), server.Config{DataDir: *data, MySQL: mysqlcdc.Connection{Addr: *mysqlAddr, User: *user, Password: os.Getenv("ETL_MYSQL_PASSWORD"), ServerID: 200000}, WorkerExecutable: exe})
		if err != nil {
			return err
		}
		httpServer := &http.Server{Addr: *addr, Handler: s, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
		listener, err := net.Listen("tcp", *addr)
		if err != nil {
			s.Close(context.Background())
			return err
		}
		errCh := make(chan error, 1)
		go func() { errCh <- httpServer.Serve(listener) }()
		fmt.Fprintf(out, "ETL local MVP listening on http://%s\n", *addr)
		select {
		case <-ctx.Done():
		case err = <-errCh:
			if err == http.ErrServerClosed {
				err = nil
			}
		}
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
		if closeErr := s.Close(shutdown); err == nil {
			err = closeErr
		}
		return err
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
