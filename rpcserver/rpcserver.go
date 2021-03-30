package rpcserver

import (
	"errors"
	"github.com/fandypeng/e2cdatabus/auth"
	pb "github.com/fandypeng/e2cdatabus/proto"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"log"
	"net"
	"strconv"
)

type Conf struct {
	Port      int
	AppKey    string
	AppSecret string
}

func Start(conf Conf, service pb.DatabusServer) error {
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
	opt = append(opt, grpc.UnaryInterceptor(authService.AccessControl()))
	s := grpc.NewServer(opt...)
	pb.RegisterDatabusServer(s, service)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return err
	}
	return nil
}
