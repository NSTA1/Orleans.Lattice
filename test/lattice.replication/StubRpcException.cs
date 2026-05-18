namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Helper that builds an instance of a stub
/// <c>Grpc.Core.RpcException</c>-shaped type for the classifier
/// tests. The classifier matches by fully-qualified type name and
/// reads <c>StatusCode</c> via reflection, so the stub lives in the
/// <c>Grpc.Core</c> namespace (declared further down in this file)
/// and exposes an integer <c>StatusCode</c> property; this lets the
/// tests assert the reflection-based code path without taking a
/// real <c>Grpc.Core</c> dependency on the replication tests
/// project.
/// </summary>
internal static class StubRpcException
{
    public static Grpc.Core.RpcException Create(int statusCode, string message)
        => new(statusCode, message);
}
