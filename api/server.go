package api

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/recoilme/pudge"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//generate api: protoc -I api/ -I${GOPATH}/src --go_out=plugins=grpc:api api/api.proto

var grpcServer *grpc.Server

// Server represents the gRPC server
type Server struct {
}

// SayOk generates response ok to a Ping request
func (s *Server) SayOk(ctx context.Context, in *Empty) (*Ok, error) {
	//log.Printf("Receive message")
	return &Ok{Message: "ok"}, nil
}

// Set store key and value in file
func (s *Server) Set(ctx context.Context, cmdSet *CmdSet) (*Empty, error) {
	err := pudge.Set(cmdSet.File, cmdSet.Key, cmdSet.Val)
	if err != nil {
		return &Empty{}, status.Errorf(codes.Unknown, err.Error())
	}
	return &Empty{}, nil
}

// Get get value by key
func (s *Server) Get(ctx context.Context, cmdGet *CmdGet) (*ResBytes, error) {
	var bytes []byte
	err := pudge.Get(cmdGet.File, cmdGet.Key, &bytes)
	if err != nil {
		return &ResBytes{}, status.Errorf(codes.Unknown, err.Error())
	}

	return &ResBytes{Bytes: bytes}, nil
}

// Sets - write key/value pairs -  return error if any
func (s *Server) Sets(ctx context.Context, cmdSets *CmdSets) (*Empty, error) {
	var keys []interface{}
	for _, key := range cmdSets.Keys {
		keys = append(keys, key)
	}
	err := pudge.Sets(cmdSets.File, keys)
	if err != nil {
		return &Empty{}, status.Errorf(codes.Unknown, err.Error())
	}
	return &Empty{}, nil
}

// Keys return keys from file
func (s *Server) Keys(ctx context.Context, cmdKeys *CmdKeys) (*ResKeys, error) {
	b, err := pudge.Keys(cmdKeys.File, cmdKeys.From, int(cmdKeys.Limit), int(cmdKeys.Offset), cmdKeys.Asc)
	if err != nil {
		return &ResKeys{}, status.Errorf(codes.Unknown, err.Error())
	}
	return &ResKeys{Keys: b}, nil
}

// Gets return key/value pairs
func (s *Server) Gets(ctx context.Context, cmdGets *CmdGets) (*ResPairs, error) {
	var keys []interface{}
	for _, key := range cmdGets.Keys {
		keys = append(keys, key)
	}
	b := pudge.Gets(cmdGets.File, keys)
	//	slowpoke.Delete()
	return &ResPairs{Pairs: b}, nil
}

// Delete key and val by key
func (s *Server) Delete(ctx context.Context, cmdDel *CmdDel) (*ResDel, error) {
	err := pudge.Delete(cmdDel.File, cmdDel.Key)
	if err != nil {
		return &ResDel{Deleted: false}, status.Errorf(codes.Unknown, err.Error())
	}
	return &ResDel{Deleted: true}, nil
}

// DeleteFile delete file by name
func (s *Server) DeleteFile(ctx context.Context, cmdDelFile *CmdDelFile) (*Empty, error) {
	err := pudge.DeleteFile(cmdDelFile.File)
	if err != nil {
		return &Empty{}, status.Errorf(codes.Unknown, err.Error())
	}
	return &Empty{}, nil
}

// Start - start server
func Start(dir string, grpcPort int) error {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}

	// create a server instance
	s := Server{}

	// create a gRPC server object
	grpcServer = grpc.NewServer()

	// attach the service to the server
	RegisterPudgeDbApiServer(grpcServer, &s)

	// start the server
	go onKill()

	// start the server
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
		return err
	}
	return nil
}

func onKill() {
	signalChan := make(chan os.Signal, 1) //https://go101.org/article/panic-and-recover-use-cases.html
	//SIGHUP: Process restart/reload (example: nginx, sshd, apache)? syscall.SIGUSR2?
	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT) //syscall.SIGINT, syscall.SIGTERM, syscall.SIGILL,

	<-signalChan
	err := pudge.CloseAll()
	log.Println("Shutdown signal received, exiting...")
	log.Println("pudge.CloseAll", err)
}

// Stop Gracefully shutdown connections
func Stop() error {
	log.Println("Stop")
	grpcServer.GracefulStop()
	err := pudge.CloseAll()
	log.Println("Stop errors", err)
	return err
}
