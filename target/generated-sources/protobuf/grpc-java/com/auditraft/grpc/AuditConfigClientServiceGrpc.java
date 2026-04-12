package com.auditraft.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * --- Client API ---
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: audit_raft.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class AuditConfigClientServiceGrpc {

  private AuditConfigClientServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "auditraft.AuditConfigClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.auditraft.grpc.PutRequest,
      com.auditraft.grpc.PutResponse> getPutConfigMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutConfig",
      requestType = com.auditraft.grpc.PutRequest.class,
      responseType = com.auditraft.grpc.PutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.auditraft.grpc.PutRequest,
      com.auditraft.grpc.PutResponse> getPutConfigMethod() {
    io.grpc.MethodDescriptor<com.auditraft.grpc.PutRequest, com.auditraft.grpc.PutResponse> getPutConfigMethod;
    if ((getPutConfigMethod = AuditConfigClientServiceGrpc.getPutConfigMethod) == null) {
      synchronized (AuditConfigClientServiceGrpc.class) {
        if ((getPutConfigMethod = AuditConfigClientServiceGrpc.getPutConfigMethod) == null) {
          AuditConfigClientServiceGrpc.getPutConfigMethod = getPutConfigMethod =
              io.grpc.MethodDescriptor.<com.auditraft.grpc.PutRequest, com.auditraft.grpc.PutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutConfig"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.PutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.PutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuditConfigClientServiceMethodDescriptorSupplier("PutConfig"))
              .build();
        }
      }
    }
    return getPutConfigMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.auditraft.grpc.GetRequest,
      com.auditraft.grpc.GetResponse> getGetConfigMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConfig",
      requestType = com.auditraft.grpc.GetRequest.class,
      responseType = com.auditraft.grpc.GetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.auditraft.grpc.GetRequest,
      com.auditraft.grpc.GetResponse> getGetConfigMethod() {
    io.grpc.MethodDescriptor<com.auditraft.grpc.GetRequest, com.auditraft.grpc.GetResponse> getGetConfigMethod;
    if ((getGetConfigMethod = AuditConfigClientServiceGrpc.getGetConfigMethod) == null) {
      synchronized (AuditConfigClientServiceGrpc.class) {
        if ((getGetConfigMethod = AuditConfigClientServiceGrpc.getGetConfigMethod) == null) {
          AuditConfigClientServiceGrpc.getGetConfigMethod = getGetConfigMethod =
              io.grpc.MethodDescriptor.<com.auditraft.grpc.GetRequest, com.auditraft.grpc.GetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetConfig"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.GetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.GetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AuditConfigClientServiceMethodDescriptorSupplier("GetConfig"))
              .build();
        }
      }
    }
    return getGetConfigMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AuditConfigClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuditConfigClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuditConfigClientServiceStub>() {
        @java.lang.Override
        public AuditConfigClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuditConfigClientServiceStub(channel, callOptions);
        }
      };
    return AuditConfigClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AuditConfigClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuditConfigClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuditConfigClientServiceBlockingStub>() {
        @java.lang.Override
        public AuditConfigClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuditConfigClientServiceBlockingStub(channel, callOptions);
        }
      };
    return AuditConfigClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AuditConfigClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AuditConfigClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AuditConfigClientServiceFutureStub>() {
        @java.lang.Override
        public AuditConfigClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AuditConfigClientServiceFutureStub(channel, callOptions);
        }
      };
    return AuditConfigClientServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * --- Client API ---
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void putConfig(com.auditraft.grpc.PutRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.PutResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPutConfigMethod(), responseObserver);
    }

    /**
     */
    default void getConfig(com.auditraft.grpc.GetRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.GetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetConfigMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service AuditConfigClientService.
   * <pre>
   * --- Client API ---
   * </pre>
   */
  public static abstract class AuditConfigClientServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return AuditConfigClientServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service AuditConfigClientService.
   * <pre>
   * --- Client API ---
   * </pre>
   */
  public static final class AuditConfigClientServiceStub
      extends io.grpc.stub.AbstractAsyncStub<AuditConfigClientServiceStub> {
    private AuditConfigClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuditConfigClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuditConfigClientServiceStub(channel, callOptions);
    }

    /**
     */
    public void putConfig(com.auditraft.grpc.PutRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.PutResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPutConfigMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getConfig(com.auditraft.grpc.GetRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.GetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetConfigMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service AuditConfigClientService.
   * <pre>
   * --- Client API ---
   * </pre>
   */
  public static final class AuditConfigClientServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<AuditConfigClientServiceBlockingStub> {
    private AuditConfigClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuditConfigClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuditConfigClientServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.auditraft.grpc.PutResponse putConfig(com.auditraft.grpc.PutRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPutConfigMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.auditraft.grpc.GetResponse getConfig(com.auditraft.grpc.GetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetConfigMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service AuditConfigClientService.
   * <pre>
   * --- Client API ---
   * </pre>
   */
  public static final class AuditConfigClientServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<AuditConfigClientServiceFutureStub> {
    private AuditConfigClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AuditConfigClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AuditConfigClientServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.auditraft.grpc.PutResponse> putConfig(
        com.auditraft.grpc.PutRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPutConfigMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.auditraft.grpc.GetResponse> getConfig(
        com.auditraft.grpc.GetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetConfigMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PUT_CONFIG = 0;
  private static final int METHODID_GET_CONFIG = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUT_CONFIG:
          serviceImpl.putConfig((com.auditraft.grpc.PutRequest) request,
              (io.grpc.stub.StreamObserver<com.auditraft.grpc.PutResponse>) responseObserver);
          break;
        case METHODID_GET_CONFIG:
          serviceImpl.getConfig((com.auditraft.grpc.GetRequest) request,
              (io.grpc.stub.StreamObserver<com.auditraft.grpc.GetResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getPutConfigMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.auditraft.grpc.PutRequest,
              com.auditraft.grpc.PutResponse>(
                service, METHODID_PUT_CONFIG)))
        .addMethod(
          getGetConfigMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.auditraft.grpc.GetRequest,
              com.auditraft.grpc.GetResponse>(
                service, METHODID_GET_CONFIG)))
        .build();
  }

  private static abstract class AuditConfigClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AuditConfigClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.auditraft.grpc.AuditRaft.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AuditConfigClientService");
    }
  }

  private static final class AuditConfigClientServiceFileDescriptorSupplier
      extends AuditConfigClientServiceBaseDescriptorSupplier {
    AuditConfigClientServiceFileDescriptorSupplier() {}
  }

  private static final class AuditConfigClientServiceMethodDescriptorSupplier
      extends AuditConfigClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    AuditConfigClientServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (AuditConfigClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AuditConfigClientServiceFileDescriptorSupplier())
              .addMethod(getPutConfigMethod())
              .addMethod(getGetConfigMethod())
              .build();
        }
      }
    }
    return result;
  }
}
