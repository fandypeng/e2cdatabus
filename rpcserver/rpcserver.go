package rpcserver

import (
	"errors"
	"github.com/fandypeng/e2cdatabus/auth"
	pb "github.com/fandypeng/e2cdatabus/proto"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

type Conf struct {
	Port      int
	AppKey    string
	AppSecret string
}

func Start(conf Conf, service pb.DatabusServer, handlers ...grpc.UnaryServerInterceptor) error {
	if conf.Port == 0 {
		conf.Port = 10000
	}
	if conf.AppKey == "" || conf.AppSecret == "" {
		return errors.New("invalid appKey or appSecret")
	}
	if service == nil {
		return errors.New("invalid service")
	}
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}
	opt := make([]grpc.ServerOption, 0)
	authService := auth.New(conf.AppKey, conf.AppSecret)
	authService.Use(authService.AccessControl())
	authService.Use(handlers...)
	opt = append(opt, grpc.UnaryInterceptor(authService.Interceptor))
	s := grpc.NewServer(opt...)
	pb.RegisterDatabusServer(s, service)
	go func() {
		log.Println("e2cdatabus started")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	singleHandler(s)
	return nil
}

func singleHandler(svr *grpc.Server) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGINT, syscall.SIGUSR2)
	for {
		s := <-c
		log.Printf("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGINT, syscall.SIGUSR2:
			svr.GracefulStop()
			log.Println("e2cdatabus exit")
			return
		case syscall.SIGHUP:
			return
		default:
			return
		}
	}
}
