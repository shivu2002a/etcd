package com.auditraft.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: audit_raft.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class RaftInternalServiceGrpc {

  private RaftInternalServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "auditraft.RaftInternalService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.auditraft.grpc.VoteRequest,
      com.auditraft.grpc.VoteResponse> getRequestVoteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RequestVote",
      requestType = com.auditraft.grpc.VoteRequest.class,
      responseType = com.auditraft.grpc.VoteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.auditraft.grpc.VoteRequest,
      com.auditraft.grpc.VoteResponse> getRequestVoteMethod() {
    io.grpc.MethodDescriptor<com.auditraft.grpc.VoteRequest, com.auditraft.grpc.VoteResponse> getRequestVoteMethod;
    if ((getRequestVoteMethod = RaftInternalServiceGrpc.getRequestVoteMethod) == null) {
      synchronized (RaftInternalServiceGrpc.class) {
        if ((getRequestVoteMethod = RaftInternalServiceGrpc.getRequestVoteMethod) == null) {
          RaftInternalServiceGrpc.getRequestVoteMethod = getRequestVoteMethod =
              io.grpc.MethodDescriptor.<com.auditraft.grpc.VoteRequest, com.auditraft.grpc.VoteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RequestVote"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.VoteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.VoteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftInternalServiceMethodDescriptorSupplier("RequestVote"))
              .build();
        }
      }
    }
    return getRequestVoteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.auditraft.grpc.AppendRequest,
      com.auditraft.grpc.AppendResponse> getAppendEntriesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AppendEntries",
      requestType = com.auditraft.grpc.AppendRequest.class,
      responseType = com.auditraft.grpc.AppendResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.auditraft.grpc.AppendRequest,
      com.auditraft.grpc.AppendResponse> getAppendEntriesMethod() {
    io.grpc.MethodDescriptor<com.auditraft.grpc.AppendRequest, com.auditraft.grpc.AppendResponse> getAppendEntriesMethod;
    if ((getAppendEntriesMethod = RaftInternalServiceGrpc.getAppendEntriesMethod) == null) {
      synchronized (RaftInternalServiceGrpc.class) {
        if ((getAppendEntriesMethod = RaftInternalServiceGrpc.getAppendEntriesMethod) == null) {
          RaftInternalServiceGrpc.getAppendEntriesMethod = getAppendEntriesMethod =
              io.grpc.MethodDescriptor.<com.auditraft.grpc.AppendRequest, com.auditraft.grpc.AppendResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AppendEntries"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.AppendRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.auditraft.grpc.AppendResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftInternalServiceMethodDescriptorSupplier("AppendEntries"))
              .build();
        }
      }
    }
    return getAppendEntriesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RaftInternalServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftInternalServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftInternalServiceStub>() {
        @java.lang.Override
        public RaftInternalServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftInternalServiceStub(channel, callOptions);
        }
      };
    return RaftInternalServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RaftInternalServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftInternalServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftInternalServiceBlockingStub>() {
        @java.lang.Override
        public RaftInternalServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftInternalServiceBlockingStub(channel, callOptions);
        }
      };
    return RaftInternalServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RaftInternalServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftInternalServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftInternalServiceFutureStub>() {
        @java.lang.Override
        public RaftInternalServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftInternalServiceFutureStub(channel, callOptions);
        }
      };
    return RaftInternalServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void requestVote(com.auditraft.grpc.VoteRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.VoteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRequestVoteMethod(), responseObserver);
    }

    /**
     */
    default void appendEntries(com.auditraft.grpc.AppendRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.AppendResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAppendEntriesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service RaftInternalService.
   */
  public static abstract class RaftInternalServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RaftInternalServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service RaftInternalService.
   */
  public static final class RaftInternalServiceStub
      extends io.grpc.stub.AbstractAsyncStub<RaftInternalServiceStub> {
    private RaftInternalServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftInternalServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftInternalServiceStub(channel, callOptions);
    }

    /**
     */
    public void requestVote(com.auditraft.grpc.VoteRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.VoteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRequestVoteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void appendEntries(com.auditraft.grpc.AppendRequest request,
        io.grpc.stub.StreamObserver<com.auditraft.grpc.AppendResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service RaftInternalService.
   */
  public static final class RaftInternalServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RaftInternalServiceBlockingStub> {
    private RaftInternalServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftInternalServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftInternalServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.auditraft.grpc.VoteResponse requestVote(com.auditraft.grpc.VoteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRequestVoteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.auditraft.grpc.AppendResponse appendEntries(com.auditraft.grpc.AppendRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAppendEntriesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service RaftInternalService.
   */
  public static final class RaftInternalServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<RaftInternalServiceFutureStub> {
    private RaftInternalServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftInternalServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftInternalServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.auditraft.grpc.VoteResponse> requestVote(
        com.auditraft.grpc.VoteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRequestVoteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.auditraft.grpc.AppendResponse> appendEntries(
        com.auditraft.grpc.AppendRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REQUEST_VOTE = 0;
  private static final int METHODID_APPEND_ENTRIES = 1;

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
        case METHODID_REQUEST_VOTE:
          serviceImpl.requestVote((com.auditraft.grpc.VoteRequest) request,
              (io.grpc.stub.StreamObserver<com.auditraft.grpc.VoteResponse>) responseObserver);
          break;
        case METHODID_APPEND_ENTRIES:
          serviceImpl.appendEntries((com.auditraft.grpc.AppendRequest) request,
              (io.grpc.stub.StreamObserver<com.auditraft.grpc.AppendResponse>) responseObserver);
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
          getRequestVoteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.auditraft.grpc.VoteRequest,
              com.auditraft.grpc.VoteResponse>(
                service, METHODID_REQUEST_VOTE)))
        .addMethod(
          getAppendEntriesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.auditraft.grpc.AppendRequest,
              com.auditraft.grpc.AppendResponse>(
                service, METHODID_APPEND_ENTRIES)))
        .build();
  }

  private static abstract class RaftInternalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RaftInternalServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.auditraft.grpc.AuditRaft.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RaftInternalService");
    }
  }

  private static final class RaftInternalServiceFileDescriptorSupplier
      extends RaftInternalServiceBaseDescriptorSupplier {
    RaftInternalServiceFileDescriptorSupplier() {}
  }

  private static final class RaftInternalServiceMethodDescriptorSupplier
      extends RaftInternalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RaftInternalServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (RaftInternalServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RaftInternalServiceFileDescriptorSupplier())
              .addMethod(getRequestVoteMethod())
              .addMethod(getAppendEntriesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
