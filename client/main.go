package main

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	descpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/karldoenitz/grpcall"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

func main() {
	protoSet, err := newProtoSet("./helloworld.protoset")
	if err != nil {
		panic(err)
	}
	fmt.Println(protoSet)
	ctx, _ := context.WithTimeout(context.TODO(), time.Second*5)
	cc, err := grpc.DialContext(ctx, "127.0.0.1:50051", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()
	resp, err := invoke(ctx, protoSet, cc, "helloworld.Greeter", "SayHello", `{"name": "hello world"}`)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
}

type protoSet struct {
	fdMap map[string]*desc.FileDescriptor
}

func newProtoSet(fileName string) (*protoSet, error) {
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("could not load protoset file %q: %v", fileName, err)
	}
	var fs descpb.FileDescriptorSet
	err = proto.Unmarshal(b, &fs)
	if err != nil {
		return nil, fmt.Errorf("could not parse contents of protoset file %q: %v", fileName, err)
	}
	protoMap := make(map[string]*descpb.FileDescriptorProto, len(fs.File))
	for _, fd := range fs.File {
		protoMap[fd.GetName()] = fd
	}
	s := &protoSet{fdMap: make(map[string]*desc.FileDescriptor)}
	for _, fd := range fs.File {
		_, err = s.resolve(protoMap, fd.GetName())
		if err != nil {
			return nil, err
		}
	}
	return s, err
}

func parseFileDescriptorProto(fileName string) {

}

func (s *protoSet) resolve(protoMap map[string]*descpb.FileDescriptorProto, fileName string) (*desc.FileDescriptor, error) {
	if r, ok := s.fdMap[fileName]; ok {
		return r, nil
	}
	fd, ok := protoMap[fileName]
	if !ok {
		return nil, fmt.Errorf("no descriptor found for %q", fileName)
	}
	deps := make([]*desc.FileDescriptor, 0, len(fd.GetDependency()))
	for _, dep := range fd.GetDependency() {
		depFd, err := s.resolve(protoMap, dep)
		if err != nil {
			return nil, err
		}
		deps = append(deps, depFd)
	}
	result, err := desc.CreateFileDescriptor(fd, deps...)
	s.fdMap[fileName] = result
	return result, err
}

func (s *protoSet) findService(svcName string) (*desc.ServiceDescriptor, error) {
	for _, fd := range s.fdMap {
		svc := fd.FindService(svcName)
		if svc != nil {
			return svc, nil
		}
	}
	return nil, fmt.Errorf("service %v not found", svcName)
}

func invoke(ctx context.Context, s *protoSet, cc *grpc.ClientConn, svc, mth, data string) (proto.Message, error) {
	sd, err := s.findService(svc)
	if err != nil {
		return nil, err
	}
	mtd := sd.FindMethodByName(mth)
	if mtd == nil {
		return nil, fmt.Errorf("service %q does not include a method named %q", svc, mth)
	}
	msgFactory := dynamic.NewMessageFactoryWithExtensionRegistry(dynamic.NewExtensionRegistryWithDefaults())
	req := msgFactory.NewMessage(mtd.GetInputType())
	resp := msgFactory.NewMessage(mtd.GetOutputType())
	var inData io.Reader
	inData = strings.NewReader(data)
	rf, err := s.requestParserFor(inData)
	if err != nil {
		return nil, err
	}
	err = rf.Next(req)
	if err != nil {
		return nil, err
	}
	err = cc.Invoke(ctx, fmt.Sprintf("/%s/%s", mtd.GetService().GetFullyQualifiedName(), mtd.GetName()), req, resp)
	return resp, err
}

func (s *protoSet) anyResolver() (jsonpb.AnyResolver, error) {
	files := make([]*desc.FileDescriptor, 0)
	for _, fd := range s.fdMap {
		files = append(files, fd)
	}

	var er dynamic.ExtensionRegistry
	for _, fd := range files {
		er.AddExtensionsFromFile(fd)
	}
	mf := dynamic.NewMessageFactoryWithExtensionRegistry(&er)
	return dynamic.AnyResolver(mf, files...), nil
}

func (s *protoSet) requestParserFor(in io.Reader) (grpcall.RequestParser, error) {
	resolver, err := s.anyResolver()
	if err != nil {
		return nil, fmt.Errorf("error creating message resolver: %v", err)
	}

	return grpcall.NewJSONRequestParser(in, resolver), nil
}
