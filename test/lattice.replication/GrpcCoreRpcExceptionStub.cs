namespace Grpc.Core;

/// <summary>
/// Test-local stub that mimics the fully-qualified type name and
/// minimal shape of <c>Grpc.Core.RpcException</c> for the
/// reflection-based status-code extraction path in
/// <c>LatticeBootstrapTransientFaultClassifier</c>. Living in the
/// <c>Grpc.Core</c> namespace inside the test assembly is what
/// makes <see cref="Type.FullName"/> resolve to
/// <c>Grpc.Core.RpcException</c>, so the classifier's by-name match
/// fires without taking a runtime dependency on the real gRPC
/// package.
/// </summary>
internal sealed class RpcException : Exception
{
    /// <summary>
    /// gRPC status code as an <see cref="int"/>, matching the
    /// reflective read path in the classifier. The real
    /// <c>Grpc.Core.RpcException</c> exposes a
    /// <c>Grpc.Core.StatusCode</c> enum here; the classifier
    /// converts to <see cref="int"/> regardless, so an integer
    /// stub is sufficient.
    /// </summary>
    public int StatusCode { get; }

    public RpcException(int statusCode, string message) : base(message)
    {
        StatusCode = statusCode;
    }
}
