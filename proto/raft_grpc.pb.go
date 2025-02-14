// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v3.21.12
// source: proto/raft.proto

package raftpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	Raft_RequestVote_FullMethodName   = "/raft.Raft/RequestVote"
	Raft_AppendEntries_FullMethodName = "/raft.Raft/AppendEntries"
	Raft_Heartbeat_FullMethodName     = "/raft.Raft/Heartbeat"
)

// RaftClient is the client API for Raft service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RaftClient interface {
	RequestVote(ctx context.Context, in *VoteRequest, opts ...grpc.CallOption) (*VoteResponse, error)
	AppendEntries(ctx context.Context, in *AppendRequest, opts ...grpc.CallOption) (*AppendResponse, error)
	Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
}

type raftClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftClient(cc grpc.ClientConnInterface) RaftClient {
	return &raftClient{cc}
}

func (c *raftClient) RequestVote(ctx context.Context, in *VoteRequest, opts ...grpc.CallOption) (*VoteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(VoteResponse)
	err := c.cc.Invoke(ctx, Raft_RequestVote_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftClient) AppendEntries(ctx context.Context, in *AppendRequest, opts ...grpc.CallOption) (*AppendResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AppendResponse)
	err := c.cc.Invoke(ctx, Raft_AppendEntries_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HeartbeatResponse)
	err := c.cc.Invoke(ctx, Raft_Heartbeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServer is the server API for Raft service.
// All implementations must embed UnimplementedRaftServer
// for forward compatibility.
type RaftServer interface {
	RequestVote(context.Context, *VoteRequest) (*VoteResponse, error)
	AppendEntries(context.Context, *AppendRequest) (*AppendResponse, error)
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	mustEmbedUnimplementedRaftServer()
}

// UnimplementedRaftServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedRaftServer struct{}

func (UnimplementedRaftServer) RequestVote(context.Context, *VoteRequest) (*VoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (UnimplementedRaftServer) AppendEntries(context.Context, *AppendRequest) (*AppendResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}
func (UnimplementedRaftServer) Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedRaftServer) mustEmbedUnimplementedRaftServer() {}
func (UnimplementedRaftServer) testEmbeddedByValue()              {}

// UnsafeRaftServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RaftServer will
// result in compilation errors.
type UnsafeRaftServer interface {
	mustEmbedUnimplementedRaftServer()
}

func RegisterRaftServer(s grpc.ServiceRegistrar, srv RaftServer) {
	// If the following call pancis, it indicates UnimplementedRaftServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Raft_ServiceDesc, srv)
}

func _Raft_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(VoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Raft_RequestVote_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).RequestVote(ctx, req.(*VoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Raft_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Raft_AppendEntries_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).AppendEntries(ctx, req.(*AppendRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Raft_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Raft_Heartbeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Raft_ServiceDesc is the grpc.ServiceDesc for Raft service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Raft_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "raft.Raft",
	HandlerType: (*RaftServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestVote",
			Handler:    _Raft_RequestVote_Handler,
		},
		{
			MethodName: "AppendEntries",
			Handler:    _Raft_AppendEntries_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _Raft_Heartbeat_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/raft.proto",
}
