namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="ProcessEnvironmentVariableReader"/>, the default
/// <see cref="IEnvironmentVariableReader"/> backed by the process environment.
/// </summary>
[TestFixture]
public sealed class ProcessEnvironmentVariableReaderTests
{
    [Test]
    public void GetVariable_when_name_is_null_throws()
    {
        var reader = new ProcessEnvironmentVariableReader();

        Assert.That(() => reader.GetVariable(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void GetVariable_when_variable_is_set_returns_its_value()
    {
        var name = "ORLEANS_LATTICE_STATE_GRPC_TEST_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(name, "the-value");
        try
        {
            var reader = new ProcessEnvironmentVariableReader();

            Assert.That(reader.GetVariable(name), Is.EqualTo("the-value"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Test]
    public void GetVariable_when_variable_is_unset_returns_null()
    {
        var name = "ORLEANS_LATTICE_STATE_GRPC_TEST_" + Guid.NewGuid().ToString("N");
        var reader = new ProcessEnvironmentVariableReader();

        Assert.That(reader.GetVariable(name), Is.Null);
    }
}
