using System.Reflection;
using System.Reflection.Emit;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the <c>Status.StatusCode</c> fallback in
/// <see cref="LatticeBootstrapTransientFaultClassifier"/>'s reflective status
/// read. The classifier prefers <c>RpcException.StatusCode</c> and falls back to
/// the nested <c>Status.StatusCode</c> "for older shapes", which the
/// <see cref="Grpc.Core.RpcException"/> stub in this assembly cannot exercise -
/// that stub deliberately models the modern shape, and a single assembly cannot
/// hold two types with the same fully-qualified name.
/// <para>
/// So the older shape is emitted at run time instead: a type genuinely named
/// <c>Grpc.Core.RpcException</c> that has no <c>StatusCode</c> property at all,
/// only a <c>Status</c> whose value the test supplies. That is the shape the
/// fallback exists for, and it is the only way to reach it without taking a real
/// <c>Grpc.Core</c> dependency in the replication package.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeBootstrapTransientFaultClassifierStatusShapeTests
{
    /// <summary>A status carrier exposing the nested integer status code.</summary>
    private sealed class StatusWithCode(int statusCode)
    {
        public int StatusCode { get; } = statusCode;
    }

    /// <summary>The real gRPC type nests an enum here, so model that too.</summary>
    public enum GrpcStatusCode
    {
        DeadlineExceeded = 4,
        Aborted = 10,
        Unavailable = 14,
        PermissionDenied = 7,
    }

    private sealed class StatusWithEnumCode(GrpcStatusCode statusCode)
    {
        public GrpcStatusCode StatusCode { get; } = statusCode;
    }

    /// <summary>A status carrier with no status code at all.</summary>
    private sealed class StatusWithoutCode
    {
        public string Detail => "no status code here";
    }

    private static readonly Type LegacyShape = EmitLegacyRpcExceptionShape();

    /// <summary>
    /// Emits <c>Grpc.Core.RpcException : Exception</c> with a single
    /// <c>Status</c> property backed by a constructor argument, and no
    /// <c>StatusCode</c> property, so
    /// <see cref="Type.FullName"/> matches the name the classifier looks for
    /// while the direct read finds nothing.
    /// </summary>
    private static Type EmitLegacyRpcExceptionShape()
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("Orleans.Lattice.Replication.Tests.LegacyGrpcShape"),
            AssemblyBuilderAccess.Run);
        var module = assembly.DefineDynamicModule("Main");
        var type = module.DefineType(
            "Grpc.Core.RpcException",
            TypeAttributes.Public | TypeAttributes.Class,
            typeof(Exception));

        var field = type.DefineField("_status", typeof(object), FieldAttributes.Private);

        var ctor = type.DefineConstructor(
            MethodAttributes.Public, CallingConventions.Standard, [typeof(object)]);
        var ctorIl = ctor.GetILGenerator();
        ctorIl.Emit(OpCodes.Ldarg_0);
        ctorIl.Emit(OpCodes.Call, typeof(Exception).GetConstructor(Type.EmptyTypes)!);
        ctorIl.Emit(OpCodes.Ldarg_0);
        ctorIl.Emit(OpCodes.Ldarg_1);
        ctorIl.Emit(OpCodes.Stfld, field);
        ctorIl.Emit(OpCodes.Ret);

        var getter = type.DefineMethod(
            "get_Status",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(object),
            Type.EmptyTypes);
        var getterIl = getter.GetILGenerator();
        getterIl.Emit(OpCodes.Ldarg_0);
        getterIl.Emit(OpCodes.Ldfld, field);
        getterIl.Emit(OpCodes.Ret);

        var property = type.DefineProperty("Status", PropertyAttributes.None, typeof(object), Type.EmptyTypes);
        property.SetGetMethod(getter);

        return type.CreateType();
    }

    private static Exception LegacyRpcException(object? status) =>
        (Exception)Activator.CreateInstance(LegacyShape, [status])!;

    [Test]
    public void The_emitted_shape_is_named_like_the_real_gRPC_exception_and_has_no_direct_status_code()
    {
        // Guards the test itself: if either of these stops holding, the fallback
        // below would be exercising nothing and would silently pass.
        Assert.Multiple(() =>
        {
            Assert.That(LegacyShape.FullName, Is.EqualTo("Grpc.Core.RpcException"));
            Assert.That(LegacyShape.GetProperty("StatusCode"), Is.Null,
                "the whole point of this shape is that the direct read finds nothing");
            Assert.That(LegacyShape.GetProperty("Status"), Is.Not.Null);
        });
    }

    [TestCase(14)]
    [TestCase(4)]
    [TestCase(10)]
    public void A_retryable_nested_status_code_is_transient(int statusCode)
    {
        var exception = LegacyRpcException(new StatusWithCode(statusCode));

        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(exception), Is.True);
    }

    [TestCase(GrpcStatusCode.Unavailable)]
    [TestCase(GrpcStatusCode.DeadlineExceeded)]
    [TestCase(GrpcStatusCode.Aborted)]
    public void A_retryable_nested_status_code_expressed_as_an_enum_is_transient(GrpcStatusCode statusCode)
    {
        // The real Grpc.Core.Status nests an enum, and the classifier converts
        // whatever it finds to Int32 - so the enum shape must classify the same.
        var exception = LegacyRpcException(new StatusWithEnumCode(statusCode));

        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(exception), Is.True);
    }

    [Test]
    public void A_non_retryable_nested_status_code_is_not_transient()
    {
        var exception = LegacyRpcException(new StatusWithEnumCode(GrpcStatusCode.PermissionDenied));

        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(exception), Is.False);
    }

    [Test]
    public void A_null_status_is_not_transient()
    {
        // Fail-safe: an RpcException-named type that exposes neither a direct
        // status code nor a status value must not be retried blindly.
        var exception = LegacyRpcException(null);

        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(exception), Is.False);
    }

    [Test]
    public void A_status_without_a_status_code_is_not_transient()
    {
        var exception = LegacyRpcException(new StatusWithoutCode());

        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(exception), Is.False);
    }

    [Test]
    public void The_nested_status_shape_is_also_unwrapped_from_an_aggregate()
    {
        // Streaming-call faults surface as aggregates, so the fallback has to
        // survive the flatten-and-recurse the classifier does first.
        var exception = new AggregateException(LegacyRpcException(new StatusWithCode(14)));

        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(exception), Is.True);
    }
}
