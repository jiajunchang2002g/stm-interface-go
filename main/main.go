// This file contains main() as well as the logic setting up the I/O.
// There is no need to modify this file.

package main

import (
	"assign2/submission"
	"assign2/wg"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func handleSigs(cancel func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	cancel()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <socket path>\n", os.Args[0])
		return
	}

	socketPath := os.Args[1]
	if err := os.RemoveAll(socketPath); err != nil {
		log.Fatal("remove existing sock error: ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var mainWg wg.WaitGroup
	mainWg.Add(1)
	go func() {
		defer mainWg.Done()
		handleSigs(cancel)
	}()

	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "unix", socketPath)
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	mainWg.Add(1)
	go func() {
		defer mainWg.Done()
		<-ctx.Done()
		if err := l.Close(); err != nil {
			log.Fatal("close listener error: ", err)
		}
	}()

	numGoroutinesStart := runtime.NumGoroutine()
	fmt.Fprintf(os.Stderr, "\n\033[33mNumber of goroutines before the engine is initialised: %d\033[0m\n", numGoroutinesStart)

	var stmInterfaceWg wg.WaitGroup
	var stmInterface submission.StmInterface
	stmInterface.Init(ctx, &stmInterfaceWg)
	for {
		conn, err := l.Accept()
		if err != nil {
			break
		}
		stmInterface.Accept(ctx, conn)
	}

	mainWg.Wait()
	stmInterface.Shutdown(ctx)
	numGoroutinesEnd := runtime.NumGoroutine()
	fmt.Fprintf(os.Stderr, "\n\033[33mNumber of goroutines after the engine is shutdown: %d\033[0m\n", numGoroutinesEnd)
	if numGoroutinesEnd > 2 {
		fmt.Fprintf(os.Stderr, "\033[31mPotential goroutine leak detected! Please check.\033[0m\n")
	}
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	fmt.Fprintf(os.Stderr, "\033[32m%s\033[0m", buf[:n])
}
