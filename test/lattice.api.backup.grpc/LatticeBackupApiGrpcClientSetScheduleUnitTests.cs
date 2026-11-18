using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Unit coverage for the backup-set schedule verbs of
/// <see cref="LatticeBackupApiGrpcClient"/> (#6067), driven over a
/// <see cref="UnaryResponseCallInvoker"/>: each shapes its request message and
/// unwraps the response, and the status codes the server uses for an unknown
/// schedule on run-now (<see cref="StatusCode.NotFound"/>) and for a facade
/// without backup-set schedules (<see cref="StatusCode.Unimplemented"/>) surface
/// as the exceptions the facade contract names.
/// </summary>
[TestFixture]
public sealed class LatticeBackupApiGrpcClientSetScheduleUnitTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private (LatticeBackupApiGrpcClient Client, UnaryResponseCallInvoker Invoker) Client(object response)
    {
        var invoker = new UnaryResponseCallInvoker(response);
        return (LatticeBackupApiGrpcClient.Create(invoker, _services), invoker);
    }

    [Test]
    public async Task ScheduleBackupSetAsync_sends_the_request_and_returns_the_status()
    {
        var status = BackupSetScheduleTestData.Status();
        var request = BackupSetScheduleTestData.Request();
        var (client, invoker) = Client(new BackupSetScheduleStatusResponse { Found = true, Status = status });

        var result = await client.ScheduleBackupSetAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(status));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeBackupGrpcMethods.ScheduleBackupSetMethodName));
            Assert.That(((BackupSetScheduleRequestMessage)invoker.LastRequest!).Request, Is.SameAs(request));
        });
    }

    [Test]
    public async Task CancelBackupSetScheduleAsync_sends_the_name_and_returns_the_result()
    {
        var (client, invoker) = Client(new BackupSetScheduleActionResponse { Result = true });

        var result = await client.CancelBackupSetScheduleAsync("nightly");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.True);
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeBackupGrpcMethods.CancelBackupSetScheduleMethodName));
            Assert.That(((BackupSetScheduleNameRequestMessage)invoker.LastRequest!).Name, Is.EqualTo("nightly"));
        });
    }

    [Test]
    public async Task RemoveBackupSetScheduleAsync_sends_the_name_and_flag_and_returns_the_result()
    {
        var (client, invoker) = Client(new BackupSetScheduleActionResponse { Result = true });

        var result = await client.RemoveBackupSetScheduleAsync("nightly", deleteRetainedSets: true);

        var sent = (BackupSetScheduleRemoveRequestMessage)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.True);
            Assert.That((sent.Name, sent.DeleteRetainedSets), Is.EqualTo(("nightly", true)));
        });
    }

    [Test]
    public async Task GetBackupSetScheduleStatusAsync_returns_the_status_when_found_and_null_otherwise()
    {
        var status = BackupSetScheduleTestData.Status();

        var found = await Client(new BackupSetScheduleStatusResponse { Found = true, Status = status }).Client
            .GetBackupSetScheduleStatusAsync("nightly");
        var missing = await Client(new BackupSetScheduleStatusResponse { Found = false }).Client
            .GetBackupSetScheduleStatusAsync("absent");

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.SameAs(status));
            Assert.That(missing, Is.Null);
        });
    }

    [Test]
    public async Task ListBackupSetSchedulesAsync_returns_every_status()
    {
        var statuses = new[] { BackupSetScheduleTestData.Status("a"), BackupSetScheduleTestData.Status("b") };
        var (client, invoker) = Client(new BackupSetScheduleListResponse { Schedules = statuses });

        var result = await client.ListBackupSetSchedulesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(statuses));
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeBackupGrpcMethods.ListBackupSetSchedulesMethodName));
        });
    }

    [Test]
    public async Task RunBackupSetScheduleNowAsync_sends_the_name_and_returns_the_result()
    {
        var (client, invoker) = Client(new BackupSetScheduleActionResponse { Result = true });

        var started = await client.RunBackupSetScheduleNowAsync("nightly");

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.True);
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeBackupGrpcMethods.RunBackupSetScheduleNowMethodName));
        });
    }

    [Test]
    public async Task GetBackupSetScheduleProgressAsync_returns_the_progress_or_null_and_throws_when_not_found()
    {
        var progress = BackupSetScheduleTestData.Progress();
        var (client, invoker) = Client(new BackupSetScheduleProgressResponse { Found = true, Progress = progress });

        var running = await client.GetBackupSetScheduleProgressAsync("nightly");
        var idle = await Client(new BackupSetScheduleProgressResponse { Found = true }).Client
            .GetBackupSetScheduleProgressAsync("nightly");

        Assert.Multiple(() =>
        {
            Assert.That(running, Is.SameAs(progress));
            Assert.That(idle, Is.Null);
            Assert.That(invoker.LastMethodName, Is.EqualTo(LatticeBackupGrpcMethods.GetBackupSetScheduleProgressMethodName));
            Assert.That(((BackupSetScheduleNameRequestMessage)invoker.LastRequest!).Name, Is.EqualTo("nightly"));
            Assert.That(
                async () => await Client(new BackupSetScheduleProgressResponse { Found = false }).Client
                    .GetBackupSetScheduleProgressAsync("absent"),
                Throws.TypeOf<KeyNotFoundException>().With.Message.Contains("absent"));
            Assert.That(
                async () => await Client(new RpcException(new Status(StatusCode.Unimplemented, "unsupported"))).Client
                    .GetBackupSetScheduleProgressAsync("nightly"),
                Throws.TypeOf<NotSupportedException>());
            Assert.That(async () => await client.GetBackupSetScheduleProgressAsync(string.Empty), Throws.ArgumentException);
        });
    }

    [Test]
    public void RunBackupSetScheduleNowAsync_maps_not_found_to_key_not_found()
    {
        var (client, _) = Client(new RpcException(new Status(StatusCode.NotFound, "No backup-set schedule is named 'absent'.")));

        Assert.That(
            async () => await client.RunBackupSetScheduleNowAsync("absent"),
            Throws.TypeOf<KeyNotFoundException>().With.Message.Contains("absent"));
    }

    [Test]
    public void Set_schedule_verbs_map_unimplemented_to_not_supported()
    {
        var (client, _) = Client(new RpcException(new Status(StatusCode.Unimplemented, "unsupported")));

        Assert.That(async () => await client.ListBackupSetSchedulesAsync(), Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Set_schedule_verbs_validate_their_arguments()
    {
        var (client, _) = Client(new BackupSetScheduleActionResponse());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await client.ScheduleBackupSetAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await client.CancelBackupSetScheduleAsync(string.Empty), Throws.ArgumentException);
            Assert.That(async () => await client.RemoveBackupSetScheduleAsync(string.Empty, false), Throws.ArgumentException);
            Assert.That(async () => await client.GetBackupSetScheduleStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(async () => await client.RunBackupSetScheduleNowAsync(string.Empty), Throws.ArgumentException);
        });
    }
}
