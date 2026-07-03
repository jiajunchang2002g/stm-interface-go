// Command booking starts an HTTP server that exposes a movie seat booking API
// backed by the in-process Software Transactional Memory (STM) engine.
//
// Usage:
//
//	./booking [-port 8080]
//
// Endpoints:
//
//	GET  /screens                — list available cinema screens
//	GET  /screens/{id}/seats     — consistent snapshot of all seats on a screen
//	POST /book                   — atomically book one or more seats
//	POST /cancel                 — cancel a booking
package main

import (
	"assign2/submission"
	"assign2/wg"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	port := flag.String("port", "8080", "HTTP listening port")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialise the STM engine.
	var stmWg wg.WaitGroup
	var stm submission.StmInterface
	stm.Init(ctx, &stmWg)

	svc := NewBookingService(&stm)
	handler := NewRouter(svc)

	server := &http.Server{
		Addr:    ":" + *port,
		Handler: handler,
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		cancel()
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "Movie booking server listening on :%s\n", *port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}

	stm.Shutdown(ctx)
}
