package pudgedb

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/recoilme/pudgedb/api"
	"github.com/recoilme/slowpoke"
	"google.golang.org/grpc"
)

// Start - start server
func Start(dir string, grpcPort int) error {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}

	// create a server instance
	s := api.Server{}

	// create a gRPC server object
	grpcServer := grpc.NewServer()

	// attach the Ping service to the server
	api.RegisterOkdbServer(grpcServer, &s)

	// start the server

	go func() {
		log.Fatal(grpcServer.Serve(lis))
		closeErr := slowpoke.CloseAll()
		if closeErr != nil {
			log.Fatal(closeErr.Error())

		}
	}()
	// handle kill
	signalChan := make(chan os.Signal, 1) //https://go101.org/article/panic-and-recover-use-cases.html
	//SIGHUP: Process restart/reload (example: nginx, sshd, apache)? syscall.SIGUSR2?
	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT) //syscall.SIGINT, syscall.SIGTERM, syscall.SIGILL,

	<-signalChan

	log.Println("Shutdown signal received, exiting...")
	closeErr := slowpoke.CloseAll()
	if closeErr != nil {
		log.Fatal(closeErr.Error())
		return closeErr
	}
	return nil
}
