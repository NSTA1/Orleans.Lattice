using Grpc.Core;
using NSubstitute;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

public sealed partial class LatticeDataApiGrpcServiceUnitTests
{
    [Test]
    public void Set_validation_refusal_is_structured_invalid_argument_without_raw_exception_or_value()
    {
        _api.SetAsync("t", "k", Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new ValidationFault()));

        var ex = Assert.ThrowsAsync<RpcException>(
            () => _service.Set(new DataSetRequest { TreeId = "t", Key = "k", Value = "secret-value"u8.ToArray() }, Context()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex.Status.Detail, Does.Contain("JSON").And.Contain("Correct the value"));
            Assert.That(ex.Trailers.GetValue("lattice-validation-code"), Is.EqualTo("schema-policy"));
            Assert.That(ex.Trailers.GetValue("lattice-validation-tree"), Is.EqualTo("t"));
            Assert.That(ex.Trailers.GetValue("lattice-validation-key"), Is.EqualTo("k"));
            Assert.That(ex.Trailers.GetValue("lattice-validation-reason"), Is.EqualTo("The value is not well-formed JSON.?"));
            Assert.That(ex.ToString(), Does.Not.Contain("secret-value").And.Not.Contain("raw-fault-secret"));
        });
    }

    private sealed class ValidationFault : Exception, ILatticeValueValidationFault
    {
        public ValidationFault() : base("raw-fault-secret") { }
        public string Code => "schema-policy";
        public string TreeId => "t";
        public string Key => "k";
        public string Reason => "The value is not well-formed JSON.\n";
    }
}
