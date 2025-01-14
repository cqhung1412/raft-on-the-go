// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.2
// 	protoc        v3.12.4
// source: proto/raft.proto

package raftpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type VoteRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Term          int32                  `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	CandidateId   string                 `protobuf:"bytes,2,opt,name=candidateId,proto3" json:"candidateId,omitempty"`
	LastLogIndex  int32                  `protobuf:"varint,3,opt,name=lastLogIndex,proto3" json:"lastLogIndex,omitempty"`
	LastLogTerm   int32                  `protobuf:"varint,4,opt,name=lastLogTerm,proto3" json:"lastLogTerm,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *VoteRequest) Reset() {
	*x = VoteRequest{}
	mi := &file_proto_raft_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *VoteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VoteRequest) ProtoMessage() {}

func (x *VoteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VoteRequest.ProtoReflect.Descriptor instead.
func (*VoteRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{0}
}

func (x *VoteRequest) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *VoteRequest) GetCandidateId() string {
	if x != nil {
		return x.CandidateId
	}
	return ""
}

func (x *VoteRequest) GetLastLogIndex() int32 {
	if x != nil {
		return x.LastLogIndex
	}
	return 0
}

func (x *VoteRequest) GetLastLogTerm() int32 {
	if x != nil {
		return x.LastLogTerm
	}
	return 0
}

type VoteResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Term          int32                  `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	VoteGranted   bool                   `protobuf:"varint,2,opt,name=voteGranted,proto3" json:"voteGranted,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *VoteResponse) Reset() {
	*x = VoteResponse{}
	mi := &file_proto_raft_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *VoteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VoteResponse) ProtoMessage() {}

func (x *VoteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VoteResponse.ProtoReflect.Descriptor instead.
func (*VoteResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{1}
}

func (x *VoteResponse) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *VoteResponse) GetVoteGranted() bool {
	if x != nil {
		return x.VoteGranted
	}
	return false
}

type AppendRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Term          int32                  `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	LeaderId      string                 `protobuf:"bytes,2,opt,name=leaderId,proto3" json:"leaderId,omitempty"`
	Entries       []string               `protobuf:"bytes,3,rep,name=entries,proto3" json:"entries,omitempty"`
	LeaderCommit  int32                  `protobuf:"varint,4,opt,name=leaderCommit,proto3" json:"leaderCommit,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AppendRequest) Reset() {
	*x = AppendRequest{}
	mi := &file_proto_raft_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AppendRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendRequest) ProtoMessage() {}

func (x *AppendRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendRequest.ProtoReflect.Descriptor instead.
func (*AppendRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{2}
}

func (x *AppendRequest) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendRequest) GetLeaderId() string {
	if x != nil {
		return x.LeaderId
	}
	return ""
}

func (x *AppendRequest) GetEntries() []string {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *AppendRequest) GetLeaderCommit() int32 {
	if x != nil {
		return x.LeaderCommit
	}
	return 0
}

type AppendResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Term          int32                  `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Success       bool                   `protobuf:"varint,2,opt,name=success,proto3" json:"success,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AppendResponse) Reset() {
	*x = AppendResponse{}
	mi := &file_proto_raft_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AppendResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendResponse) ProtoMessage() {}

func (x *AppendResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendResponse.ProtoReflect.Descriptor instead.
func (*AppendResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{3}
}

func (x *AppendResponse) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

var File_proto_raft_proto protoreflect.FileDescriptor

var file_proto_raft_proto_rawDesc = []byte{
	0x0a, 0x10, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x04, 0x72, 0x61, 0x66, 0x74, 0x22, 0x89, 0x01, 0x0a, 0x0b, 0x56, 0x6f, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x20, 0x0a, 0x0b,
	0x63, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0b, 0x63, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x64, 0x12, 0x22,
	0x0a, 0x0c, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64,
	0x65, 0x78, 0x12, 0x20, 0x0a, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72,
	0x6d, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67,
	0x54, 0x65, 0x72, 0x6d, 0x22, 0x44, 0x0a, 0x0c, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x20, 0x0a, 0x0b, 0x76, 0x6f, 0x74, 0x65,
	0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0b, 0x76,
	0x6f, 0x74, 0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x22, 0x7d, 0x0a, 0x0d, 0x41, 0x70,
	0x70, 0x65, 0x6e, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x74,
	0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12,
	0x1a, 0x0a, 0x08, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x65,
	0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x65, 0x6e,
	0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x22, 0x0a, 0x0c, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x43,
	0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x6c, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x22, 0x3e, 0x0a, 0x0e, 0x41, 0x70, 0x70,
	0x65, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74,
	0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12,
	0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x32, 0x78, 0x0a, 0x04, 0x52, 0x61, 0x66,
	0x74, 0x12, 0x34, 0x0a, 0x0b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65,
	0x12, 0x11, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x12, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x56, 0x6f, 0x74, 0x65, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x3a, 0x0a, 0x0d, 0x41, 0x70, 0x70, 0x65, 0x6e,
	0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x13, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e,
	0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e,
	0x72, 0x61, 0x66, 0x74, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x42, 0x10, 0x5a, 0x0e, 0x2e, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x3b, 0x72,
	0x61, 0x66, 0x74, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_raft_proto_rawDescOnce sync.Once
	file_proto_raft_proto_rawDescData = file_proto_raft_proto_rawDesc
)

func file_proto_raft_proto_rawDescGZIP() []byte {
	file_proto_raft_proto_rawDescOnce.Do(func() {
		file_proto_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_raft_proto_rawDescData)
	})
	return file_proto_raft_proto_rawDescData
}

var file_proto_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_proto_raft_proto_goTypes = []any{
	(*VoteRequest)(nil),    // 0: raft.VoteRequest
	(*VoteResponse)(nil),   // 1: raft.VoteResponse
	(*AppendRequest)(nil),  // 2: raft.AppendRequest
	(*AppendResponse)(nil), // 3: raft.AppendResponse
}
var file_proto_raft_proto_depIdxs = []int32{
	0, // 0: raft.Raft.RequestVote:input_type -> raft.VoteRequest
	2, // 1: raft.Raft.AppendEntries:input_type -> raft.AppendRequest
	1, // 2: raft.Raft.RequestVote:output_type -> raft.VoteResponse
	3, // 3: raft.Raft.AppendEntries:output_type -> raft.AppendResponse
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_proto_raft_proto_init() }
func file_proto_raft_proto_init() {
	if File_proto_raft_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_raft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_raft_proto_goTypes,
		DependencyIndexes: file_proto_raft_proto_depIdxs,
		MessageInfos:      file_proto_raft_proto_msgTypes,
	}.Build()
	File_proto_raft_proto = out.File
	file_proto_raft_proto_rawDesc = nil
	file_proto_raft_proto_goTypes = nil
	file_proto_raft_proto_depIdxs = nil
}
