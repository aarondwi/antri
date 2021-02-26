// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.15.0
// source: antri.proto

package proto

import (
	proto "github.com/golang/protobuf/proto"
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

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type NewTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Content        []byte `protobuf:"bytes,1,opt,name=content,proto3" json:"content,omitempty"`
	SecondsFromNow uint32 `protobuf:"varint,2,opt,name=secondsFromNow,proto3" json:"secondsFromNow,omitempty"`
}

func (x *NewTask) Reset() {
	*x = NewTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NewTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NewTask) ProtoMessage() {}

func (x *NewTask) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NewTask.ProtoReflect.Descriptor instead.
func (*NewTask) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{0}
}

func (x *NewTask) GetContent() []byte {
	if x != nil {
		return x.Content
	}
	return nil
}

func (x *NewTask) GetSecondsFromNow() uint32 {
	if x != nil {
		return x.SecondsFromNow
	}
	return 0
}

type AddTasksRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tasks []*NewTask `protobuf:"bytes,1,rep,name=tasks,proto3" json:"tasks,omitempty"`
}

func (x *AddTasksRequest) Reset() {
	*x = AddTasksRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddTasksRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddTasksRequest) ProtoMessage() {}

func (x *AddTasksRequest) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddTasksRequest.ProtoReflect.Descriptor instead.
func (*AddTasksRequest) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{1}
}

func (x *AddTasksRequest) GetTasks() []*NewTask {
	if x != nil {
		return x.Tasks
	}
	return nil
}

type AddTasksResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Result bool `protobuf:"varint,1,opt,name=result,proto3" json:"result,omitempty"`
}

func (x *AddTasksResponse) Reset() {
	*x = AddTasksResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddTasksResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddTasksResponse) ProtoMessage() {}

func (x *AddTasksResponse) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddTasksResponse.ProtoReflect.Descriptor instead.
func (*AddTasksResponse) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{2}
}

func (x *AddTasksResponse) GetResult() bool {
	if x != nil {
		return x.Result
	}
	return false
}

type CommitTasksRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Keys []string `protobuf:"bytes,1,rep,name=keys,proto3" json:"keys,omitempty"`
}

func (x *CommitTasksRequest) Reset() {
	*x = CommitTasksRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CommitTasksRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommitTasksRequest) ProtoMessage() {}

func (x *CommitTasksRequest) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommitTasksRequest.ProtoReflect.Descriptor instead.
func (*CommitTasksRequest) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{3}
}

func (x *CommitTasksRequest) GetKeys() []string {
	if x != nil {
		return x.Keys
	}
	return nil
}

type CommitTasksResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Keys []string `protobuf:"bytes,1,rep,name=keys,proto3" json:"keys,omitempty"`
}

func (x *CommitTasksResponse) Reset() {
	*x = CommitTasksResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CommitTasksResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommitTasksResponse) ProtoMessage() {}

func (x *CommitTasksResponse) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommitTasksResponse.ProtoReflect.Descriptor instead.
func (*CommitTasksResponse) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{4}
}

func (x *CommitTasksResponse) GetKeys() []string {
	if x != nil {
		return x.Keys
	}
	return nil
}

type GetTasksRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MaxN uint32 `protobuf:"varint,1,opt,name=maxN,proto3" json:"maxN,omitempty"`
}

func (x *GetTasksRequest) Reset() {
	*x = GetTasksRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetTasksRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetTasksRequest) ProtoMessage() {}

func (x *GetTasksRequest) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetTasksRequest.ProtoReflect.Descriptor instead.
func (*GetTasksRequest) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{5}
}

func (x *GetTasksRequest) GetMaxN() uint32 {
	if x != nil {
		return x.MaxN
	}
	return 0
}

type RetrievedTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key         string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Content     []byte `protobuf:"bytes,2,opt,name=content,proto3" json:"content,omitempty"`
	RetryNumber int32  `protobuf:"varint,3,opt,name=retryNumber,proto3" json:"retryNumber,omitempty"`
}

func (x *RetrievedTask) Reset() {
	*x = RetrievedTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RetrievedTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RetrievedTask) ProtoMessage() {}

func (x *RetrievedTask) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RetrievedTask.ProtoReflect.Descriptor instead.
func (*RetrievedTask) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{6}
}

func (x *RetrievedTask) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *RetrievedTask) GetContent() []byte {
	if x != nil {
		return x.Content
	}
	return nil
}

func (x *RetrievedTask) GetRetryNumber() int32 {
	if x != nil {
		return x.RetryNumber
	}
	return 0
}

type GetTasksResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tasks []*RetrievedTask `protobuf:"bytes,1,rep,name=tasks,proto3" json:"tasks,omitempty"`
}

func (x *GetTasksResponse) Reset() {
	*x = GetTasksResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_antri_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetTasksResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetTasksResponse) ProtoMessage() {}

func (x *GetTasksResponse) ProtoReflect() protoreflect.Message {
	mi := &file_antri_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetTasksResponse.ProtoReflect.Descriptor instead.
func (*GetTasksResponse) Descriptor() ([]byte, []int) {
	return file_antri_proto_rawDescGZIP(), []int{7}
}

func (x *GetTasksResponse) GetTasks() []*RetrievedTask {
	if x != nil {
		return x.Tasks
	}
	return nil
}

var File_antri_proto protoreflect.FileDescriptor

var file_antri_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x61, 0x6e, 0x74, 0x72, 0x69, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x4b, 0x0a,
	0x07, 0x4e, 0x65, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x18, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74,
	0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65,
	0x6e, 0x74, 0x12, 0x26, 0x0a, 0x0e, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x46, 0x72, 0x6f,
	0x6d, 0x4e, 0x6f, 0x77, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0e, 0x73, 0x65, 0x63, 0x6f,
	0x6e, 0x64, 0x73, 0x46, 0x72, 0x6f, 0x6d, 0x4e, 0x6f, 0x77, 0x22, 0x31, 0x0a, 0x0f, 0x41, 0x64,
	0x64, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1e, 0x0a,
	0x05, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x08, 0x2e, 0x4e,
	0x65, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x05, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x22, 0x2a, 0x0a,
	0x10, 0x41, 0x64, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x16, 0x0a, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x22, 0x28, 0x0a, 0x12, 0x43, 0x6f, 0x6d,
	0x6d, 0x69, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x12, 0x0a, 0x04, 0x6b, 0x65, 0x79, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x6b,
	0x65, 0x79, 0x73, 0x22, 0x29, 0x0a, 0x13, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x61, 0x73,
	0x6b, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6b, 0x65,
	0x79, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x6b, 0x65, 0x79, 0x73, 0x22, 0x25,
	0x0a, 0x0f, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x12, 0x0a, 0x04, 0x6d, 0x61, 0x78, 0x4e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52,
	0x04, 0x6d, 0x61, 0x78, 0x4e, 0x22, 0x5d, 0x0a, 0x0d, 0x52, 0x65, 0x74, 0x72, 0x69, 0x65, 0x76,
	0x65, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x18, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74,
	0x65, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65,
	0x6e, 0x74, 0x12, 0x20, 0x0a, 0x0b, 0x72, 0x65, 0x74, 0x72, 0x79, 0x4e, 0x75, 0x6d, 0x62, 0x65,
	0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x72, 0x65, 0x74, 0x72, 0x79, 0x4e, 0x75,
	0x6d, 0x62, 0x65, 0x72, 0x22, 0x38, 0x0a, 0x10, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x24, 0x0a, 0x05, 0x74, 0x61, 0x73, 0x6b,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x52, 0x65, 0x74, 0x72, 0x69, 0x65,
	0x76, 0x65, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x05, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x32, 0xa3,
	0x01, 0x0a, 0x05, 0x41, 0x6e, 0x74, 0x72, 0x69, 0x12, 0x2f, 0x0a, 0x08, 0x41, 0x64, 0x64, 0x54,
	0x61, 0x73, 0x6b, 0x73, 0x12, 0x10, 0x2e, 0x41, 0x64, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x11, 0x2e, 0x41, 0x64, 0x64, 0x54, 0x61, 0x73, 0x6b,
	0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2f, 0x0a, 0x08, 0x47, 0x65, 0x74,
	0x54, 0x61, 0x73, 0x6b, 0x73, 0x12, 0x10, 0x2e, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x73,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x11, 0x2e, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73,
	0x6b, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x38, 0x0a, 0x0b, 0x43, 0x6f,
	0x6d, 0x6d, 0x69, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x12, 0x13, 0x2e, 0x43, 0x6f, 0x6d, 0x6d,
	0x69, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x14,
	0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x42, 0x09, 0x5a, 0x07, 0x2e, 0x3b, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_antri_proto_rawDescOnce sync.Once
	file_antri_proto_rawDescData = file_antri_proto_rawDesc
)

func file_antri_proto_rawDescGZIP() []byte {
	file_antri_proto_rawDescOnce.Do(func() {
		file_antri_proto_rawDescData = protoimpl.X.CompressGZIP(file_antri_proto_rawDescData)
	})
	return file_antri_proto_rawDescData
}

var file_antri_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_antri_proto_goTypes = []interface{}{
	(*NewTask)(nil),             // 0: NewTask
	(*AddTasksRequest)(nil),     // 1: AddTasksRequest
	(*AddTasksResponse)(nil),    // 2: AddTasksResponse
	(*CommitTasksRequest)(nil),  // 3: CommitTasksRequest
	(*CommitTasksResponse)(nil), // 4: CommitTasksResponse
	(*GetTasksRequest)(nil),     // 5: GetTasksRequest
	(*RetrievedTask)(nil),       // 6: RetrievedTask
	(*GetTasksResponse)(nil),    // 7: GetTasksResponse
}
var file_antri_proto_depIdxs = []int32{
	0, // 0: AddTasksRequest.tasks:type_name -> NewTask
	6, // 1: GetTasksResponse.tasks:type_name -> RetrievedTask
	1, // 2: Antri.AddTasks:input_type -> AddTasksRequest
	5, // 3: Antri.GetTasks:input_type -> GetTasksRequest
	3, // 4: Antri.CommitTasks:input_type -> CommitTasksRequest
	2, // 5: Antri.AddTasks:output_type -> AddTasksResponse
	7, // 6: Antri.GetTasks:output_type -> GetTasksResponse
	4, // 7: Antri.CommitTasks:output_type -> CommitTasksResponse
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_antri_proto_init() }
func file_antri_proto_init() {
	if File_antri_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_antri_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NewTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_antri_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddTasksRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_antri_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddTasksResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_antri_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CommitTasksRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_antri_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CommitTasksResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_antri_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetTasksRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_antri_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RetrievedTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_antri_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetTasksResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_antri_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_antri_proto_goTypes,
		DependencyIndexes: file_antri_proto_depIdxs,
		MessageInfos:      file_antri_proto_msgTypes,
	}.Build()
	File_antri_proto = out.File
	file_antri_proto_rawDesc = nil
	file_antri_proto_goTypes = nil
	file_antri_proto_depIdxs = nil
}
