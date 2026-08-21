using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Tests.Configuration;

/// <summary>
/// Direct unit tests for <see cref="ProcessExplorerEnvironment"/>, the default
/// environment reader, so its variable lookup and argument guard are covered
/// without depending on a larger bootstrap flow.
/// </summary>
[TestFixture]
public class ProcessExplorerEnvironmentTests
{
    [Test]
    public void GetVariable_reads_present_and_absent_variables()
    {
        var name = "LATTICE_EXPLORER_COVERAGE_PROBE_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(name, "present");
        try
        {
            var environment = new ProcessExplorerEnvironment();

            Assert.Multiple(() =>
            {
                Assert.That(environment.GetVariable(name), Is.EqualTo("present"));
                Assert.That(environment.GetVariable(name + "_ABSENT"), Is.Null);
            });
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Test]
    public void GetVariable_null_name_throws()
    {
        Assert.That(() => new ProcessExplorerEnvironment().GetVariable(null!), Throws.ArgumentNullException);
    }
}
