// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.24.4
// source: api/gateway.proto

package api

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	GatewayService_Create_FullMethodName                    = "/api.GatewayService/Create"
	GatewayService_Get_FullMethodName                       = "/api.GatewayService/Get"
	GatewayService_Update_FullMethodName                    = "/api.GatewayService/Update"
	GatewayService_Delete_FullMethodName                    = "/api.GatewayService/Delete"
	GatewayService_List_FullMethodName                      = "/api.GatewayService/List"
	GatewayService_GenerateClientCertificate_FullMethodName = "/api.GatewayService/GenerateClientCertificate"
	GatewayService_GetMetrics_FullMethodName                = "/api.GatewayService/GetMetrics"
	GatewayService_GetDutyCycleMetrics_FullMethodName       = "/api.GatewayService/GetDutyCycleMetrics"
	GatewayService_GetRelayGateway_FullMethodName           = "/api.GatewayService/GetRelayGateway"
	GatewayService_ListRelayGateways_FullMethodName         = "/api.GatewayService/ListRelayGateways"
	GatewayService_UpdateRelayGateway_FullMethodName        = "/api.GatewayService/UpdateRelayGateway"
	GatewayService_DeleteRelayGateway_FullMethodName        = "/api.GatewayService/DeleteRelayGateway"
)

// GatewayServiceClient is the client API for GatewayService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type GatewayServiceClient interface {
	// Create creates the given gateway.
	Create(ctx context.Context, in *CreateGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Get returns the gateway for the given Gateway ID.
	Get(ctx context.Context, in *GetGatewayRequest, opts ...grpc.CallOption) (*GetGatewayResponse, error)
	// Update updates the given gateway.
	Update(ctx context.Context, in *UpdateGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Delete deletes the gateway matching the given Gateway ID.
	Delete(ctx context.Context, in *DeleteGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Get the list of gateways.
	List(ctx context.Context, in *ListGatewaysRequest, opts ...grpc.CallOption) (*ListGatewaysResponse, error)
	// Generate client-certificate for the gateway.
	GenerateClientCertificate(ctx context.Context, in *GenerateGatewayClientCertificateRequest, opts ...grpc.CallOption) (*GenerateGatewayClientCertificateResponse, error)
	// GetMetrics returns the gateway metrics.
	GetMetrics(ctx context.Context, in *GetGatewayMetricsRequest, opts ...grpc.CallOption) (*GetGatewayMetricsResponse, error)
	// GetDutyCycleMetrics returns the duty-cycle metrics.
	// Note that only the last 2 hours of data are stored. Currently only per minute aggregation is available.
	GetDutyCycleMetrics(ctx context.Context, in *GetGatewayDutyCycleMetricsRequest, opts ...grpc.CallOption) (*GetGatewayDutyCycleMetricsResponse, error)
	// Get the given Relay Gateway.
	GetRelayGateway(ctx context.Context, in *GetRelayGatewayRequest, opts ...grpc.CallOption) (*GetRelayGatewayResponse, error)
	// List the detected Relay Gateways.
	ListRelayGateways(ctx context.Context, in *ListRelayGatewaysRequest, opts ...grpc.CallOption) (*ListRelayGatewaysResponse, error)
	// Update the given Relay Gateway.
	UpdateRelayGateway(ctx context.Context, in *UpdateRelayGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Delete the given Relay Gateway.
	DeleteRelayGateway(ctx context.Context, in *DeleteRelayGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type gatewayServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewGatewayServiceClient(cc grpc.ClientConnInterface) GatewayServiceClient {
	return &gatewayServiceClient{cc}
}

func (c *gatewayServiceClient) Create(ctx context.Context, in *CreateGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, GatewayService_Create_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) Get(ctx context.Context, in *GetGatewayRequest, opts ...grpc.CallOption) (*GetGatewayResponse, error) {
	out := new(GetGatewayResponse)
	err := c.cc.Invoke(ctx, GatewayService_Get_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) Update(ctx context.Context, in *UpdateGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, GatewayService_Update_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) Delete(ctx context.Context, in *DeleteGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, GatewayService_Delete_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) List(ctx context.Context, in *ListGatewaysRequest, opts ...grpc.CallOption) (*ListGatewaysResponse, error) {
	out := new(ListGatewaysResponse)
	err := c.cc.Invoke(ctx, GatewayService_List_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) GenerateClientCertificate(ctx context.Context, in *GenerateGatewayClientCertificateRequest, opts ...grpc.CallOption) (*GenerateGatewayClientCertificateResponse, error) {
	out := new(GenerateGatewayClientCertificateResponse)
	err := c.cc.Invoke(ctx, GatewayService_GenerateClientCertificate_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) GetMetrics(ctx context.Context, in *GetGatewayMetricsRequest, opts ...grpc.CallOption) (*GetGatewayMetricsResponse, error) {
	out := new(GetGatewayMetricsResponse)
	err := c.cc.Invoke(ctx, GatewayService_GetMetrics_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) GetDutyCycleMetrics(ctx context.Context, in *GetGatewayDutyCycleMetricsRequest, opts ...grpc.CallOption) (*GetGatewayDutyCycleMetricsResponse, error) {
	out := new(GetGatewayDutyCycleMetricsResponse)
	err := c.cc.Invoke(ctx, GatewayService_GetDutyCycleMetrics_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) GetRelayGateway(ctx context.Context, in *GetRelayGatewayRequest, opts ...grpc.CallOption) (*GetRelayGatewayResponse, error) {
	out := new(GetRelayGatewayResponse)
	err := c.cc.Invoke(ctx, GatewayService_GetRelayGateway_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) ListRelayGateways(ctx context.Context, in *ListRelayGatewaysRequest, opts ...grpc.CallOption) (*ListRelayGatewaysResponse, error) {
	out := new(ListRelayGatewaysResponse)
	err := c.cc.Invoke(ctx, GatewayService_ListRelayGateways_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) UpdateRelayGateway(ctx context.Context, in *UpdateRelayGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, GatewayService_UpdateRelayGateway_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) DeleteRelayGateway(ctx context.Context, in *DeleteRelayGatewayRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, GatewayService_DeleteRelayGateway_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GatewayServiceServer is the server API for GatewayService service.
// All implementations must embed UnimplementedGatewayServiceServer
// for forward compatibility
type GatewayServiceServer interface {
	// Create creates the given gateway.
	Create(context.Context, *CreateGatewayRequest) (*emptypb.Empty, error)
	// Get returns the gateway for the given Gateway ID.
	Get(context.Context, *GetGatewayRequest) (*GetGatewayResponse, error)
	// Update updates the given gateway.
	Update(context.Context, *UpdateGatewayRequest) (*emptypb.Empty, error)
	// Delete deletes the gateway matching the given Gateway ID.
	Delete(context.Context, *DeleteGatewayRequest) (*emptypb.Empty, error)
	// Get the list of gateways.
	List(context.Context, *ListGatewaysRequest) (*ListGatewaysResponse, error)
	// Generate client-certificate for the gateway.
	GenerateClientCertificate(context.Context, *GenerateGatewayClientCertificateRequest) (*GenerateGatewayClientCertificateResponse, error)
	// GetMetrics returns the gateway metrics.
	GetMetrics(context.Context, *GetGatewayMetricsRequest) (*GetGatewayMetricsResponse, error)
	// GetDutyCycleMetrics returns the duty-cycle metrics.
	// Note that only the last 2 hours of data are stored. Currently only per minute aggregation is available.
	GetDutyCycleMetrics(context.Context, *GetGatewayDutyCycleMetricsRequest) (*GetGatewayDutyCycleMetricsResponse, error)
	// Get the given Relay Gateway.
	GetRelayGateway(context.Context, *GetRelayGatewayRequest) (*GetRelayGatewayResponse, error)
	// List the detected Relay Gateways.
	ListRelayGateways(context.Context, *ListRelayGatewaysRequest) (*ListRelayGatewaysResponse, error)
	// Update the given Relay Gateway.
	UpdateRelayGateway(context.Context, *UpdateRelayGatewayRequest) (*emptypb.Empty, error)
	// Delete the given Relay Gateway.
	DeleteRelayGateway(context.Context, *DeleteRelayGatewayRequest) (*emptypb.Empty, error)
	mustEmbedUnimplementedGatewayServiceServer()
}

// UnimplementedGatewayServiceServer must be embedded to have forward compatible implementations.
type UnimplementedGatewayServiceServer struct {
}

func (UnimplementedGatewayServiceServer) Create(context.Context, *CreateGatewayRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Create not implemented")
}
func (UnimplementedGatewayServiceServer) Get(context.Context, *GetGatewayRequest) (*GetGatewayResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedGatewayServiceServer) Update(context.Context, *UpdateGatewayRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Update not implemented")
}
func (UnimplementedGatewayServiceServer) Delete(context.Context, *DeleteGatewayRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (UnimplementedGatewayServiceServer) List(context.Context, *ListGatewaysRequest) (*ListGatewaysResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method List not implemented")
}
func (UnimplementedGatewayServiceServer) GenerateClientCertificate(context.Context, *GenerateGatewayClientCertificateRequest) (*GenerateGatewayClientCertificateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GenerateClientCertificate not implemented")
}
func (UnimplementedGatewayServiceServer) GetMetrics(context.Context, *GetGatewayMetricsRequest) (*GetGatewayMetricsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMetrics not implemented")
}
func (UnimplementedGatewayServiceServer) GetDutyCycleMetrics(context.Context, *GetGatewayDutyCycleMetricsRequest) (*GetGatewayDutyCycleMetricsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDutyCycleMetrics not implemented")
}
func (UnimplementedGatewayServiceServer) GetRelayGateway(context.Context, *GetRelayGatewayRequest) (*GetRelayGatewayResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRelayGateway not implemented")
}
func (UnimplementedGatewayServiceServer) ListRelayGateways(context.Context, *ListRelayGatewaysRequest) (*ListRelayGatewaysResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListRelayGateways not implemented")
}
func (UnimplementedGatewayServiceServer) UpdateRelayGateway(context.Context, *UpdateRelayGatewayRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateRelayGateway not implemented")
}
func (UnimplementedGatewayServiceServer) DeleteRelayGateway(context.Context, *DeleteRelayGatewayRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteRelayGateway not implemented")
}
func (UnimplementedGatewayServiceServer) mustEmbedUnimplementedGatewayServiceServer() {}

// UnsafeGatewayServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to GatewayServiceServer will
// result in compilation errors.
type UnsafeGatewayServiceServer interface {
	mustEmbedUnimplementedGatewayServiceServer()
}

func RegisterGatewayServiceServer(s grpc.ServiceRegistrar, srv GatewayServiceServer) {
	s.RegisterService(&GatewayService_ServiceDesc, srv)
}

func _GatewayService_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateGatewayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_Create_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).Create(ctx, req.(*CreateGatewayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetGatewayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_Get_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).Get(ctx, req.(*GetGatewayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_Update_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateGatewayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).Update(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_Update_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).Update(ctx, req.(*UpdateGatewayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteGatewayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_Delete_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).Delete(ctx, req.(*DeleteGatewayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_List_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListGatewaysRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).List(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_List_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).List(ctx, req.(*ListGatewaysRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_GenerateClientCertificate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GenerateGatewayClientCertificateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).GenerateClientCertificate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_GenerateClientCertificate_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).GenerateClientCertificate(ctx, req.(*GenerateGatewayClientCertificateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_GetMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetGatewayMetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).GetMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_GetMetrics_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).GetMetrics(ctx, req.(*GetGatewayMetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_GetDutyCycleMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetGatewayDutyCycleMetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).GetDutyCycleMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_GetDutyCycleMetrics_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).GetDutyCycleMetrics(ctx, req.(*GetGatewayDutyCycleMetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_GetRelayGateway_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRelayGatewayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).GetRelayGateway(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_GetRelayGateway_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).GetRelayGateway(ctx, req.(*GetRelayGatewayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_ListRelayGateways_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListRelayGatewaysRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).ListRelayGateways(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_ListRelayGateways_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).ListRelayGateways(ctx, req.(*ListRelayGatewaysRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_UpdateRelayGateway_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateRelayGatewayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).UpdateRelayGateway(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_UpdateRelayGateway_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).UpdateRelayGateway(ctx, req.(*UpdateRelayGatewayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_DeleteRelayGateway_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRelayGatewayRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).DeleteRelayGateway(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_DeleteRelayGateway_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).DeleteRelayGateway(ctx, req.(*DeleteRelayGatewayRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// GatewayService_ServiceDesc is the grpc.ServiceDesc for GatewayService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var GatewayService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.GatewayService",
	HandlerType: (*GatewayServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Create",
			Handler:    _GatewayService_Create_Handler,
		},
		{
			MethodName: "Get",
			Handler:    _GatewayService_Get_Handler,
		},
		{
			MethodName: "Update",
			Handler:    _GatewayService_Update_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _GatewayService_Delete_Handler,
		},
		{
			MethodName: "List",
			Handler:    _GatewayService_List_Handler,
		},
		{
			MethodName: "GenerateClientCertificate",
			Handler:    _GatewayService_GenerateClientCertificate_Handler,
		},
		{
			MethodName: "GetMetrics",
			Handler:    _GatewayService_GetMetrics_Handler,
		},
		{
			MethodName: "GetDutyCycleMetrics",
			Handler:    _GatewayService_GetDutyCycleMetrics_Handler,
		},
		{
			MethodName: "GetRelayGateway",
			Handler:    _GatewayService_GetRelayGateway_Handler,
		},
		{
			MethodName: "ListRelayGateways",
			Handler:    _GatewayService_ListRelayGateways_Handler,
		},
		{
			MethodName: "UpdateRelayGateway",
			Handler:    _GatewayService_UpdateRelayGateway_Handler,
		},
		{
			MethodName: "DeleteRelayGateway",
			Handler:    _GatewayService_DeleteRelayGateway_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/gateway.proto",
}
