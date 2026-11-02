using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Parity;

/// <summary>
/// Storage that cannot be reached fails a journey when <c>LATTICE_E2E_REQUIRED=1</c> (as the CI
/// lane sets it) and may only go inconclusive without it (#5359, scope item 5). This is the
/// false green the testing master warns about, closed.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
[NonParallelizable]
public sealed class JourneyStorageFailClosedTests
{
    /// <summary>Required storage that is unavailable is a failure, never an inconclusive skip.</summary>
    [Test]
    public void Unavailable_required_storage_is_a_failure_and_optional_storage_inconclusive()
    {
        Assert.Multiple(() =>
        {
            Assert.That(JourneyStorage.Unavailable("down", required: true), Is.TypeOf<InvalidOperationException>());
            Assert.That(JourneyStorage.Unavailable("down", required: false), Is.TypeOf<InconclusiveException>());
        });
    }

    /// <summary>With storage named but not answering and the lane's flag set, provisioning throws a failure.</summary>
    [Test]
    public async Task Provisioning_against_unreachable_storage_fails_when_required()
    {
        var closed = LoopbackPorts.Next();
        var unreachable = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + $"BlobEndpoint=http://127.0.0.1:{closed}/devstoreaccount1;TableEndpoint=http://127.0.0.1:{closed}/devstoreaccount1;";
        var saved = Save(JourneyStorage.StorageVariable, JourneyStorage.BackupStorageVariable, JourneyStorage.RequiredVariable);
        try
        {
            Environment.SetEnvironmentVariable(JourneyStorage.StorageVariable, unreachable);
            Environment.SetEnvironmentVariable(JourneyStorage.BackupStorageVariable, unreachable);
            Environment.SetEnvironmentVariable(JourneyStorage.RequiredVariable, "1");

            var failure = await Catch(() => JourneyStorage.ProvisionAsync(TimeSpan.FromSeconds(3)));

            Assert.That(failure, Is.TypeOf<InvalidOperationException>(), "a required estate fails, it does not go inconclusive");
            Assert.That(failure!.Message, Does.Contain(JourneyStorage.RequiredVariable));
        }
        finally
        {
            foreach (var (name, value) in saved)
            {
                Environment.SetEnvironmentVariable(name, value);
            }
        }
    }

    private static Dictionary<string, string?> Save(params string[] names) =>
        names.ToDictionary(name => name, Environment.GetEnvironmentVariable);

    private static async Task<Exception?> Catch(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
