using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// A circuit's imports (issue #5365) belong to the caller that started them: another
/// caller in the same circuit is never shown one, and a running import is cancelled as
/// soon as the caller changes, since its calls would otherwise go on as the new caller.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataImportSessionsTests
{
    private readonly FakeAuthSession _auth = new();
    private readonly FakeExplorerSession _session = new FakeExplorerSession(new FakeStateConnection())
        .Configured(new ExplorerConfiguration { Endpoint = "https://cluster.example:5001" });

    [SetUp]
    public void SetUp() => _auth.SignIn("alice");

    [Test]
    public async Task A_finished_import_is_shown_to_its_caller_and_never_to_the_next()
    {
        using var caller = new ShellCaller(_auth, _session);
        using var services = new ServiceCollection().AddSingleton(caller).BuildServiceProvider();
        using var sessions = new DataImportSessions(services);
        var data = DataImportRunTests.FakeStreamingData.Create();
        var bytes = Encoding.UTF8.GetBytes("{\"id\":\"a\"}\n");
        var run = new DataImportRun("orders", new DataImportSettings { KeyTemplate = "/id" }, () => new MemoryStream(bytes), bytes.Length, data, admin: null);

        sessions.Start(run);
        await WaitUntilAsync(() => run.IsFinished);
        var alice = sessions.Find("orders");
        _auth.SignIn("bob");

        Assert.Multiple(() =>
        {
            Assert.That(alice, Is.SameAs(run));
            Assert.That(sessions.Find("orders"), Is.Null, "alice's import, and its refused keys, are not shown to bob");
        });
    }

    [Test]
    public async Task A_running_import_is_cancelled_when_the_caller_changes()
    {
        using var caller = new ShellCaller(_auth, _session);
        using var services = new ServiceCollection().AddSingleton(caller).BuildServiceProvider();
        using var sessions = new DataImportSessions(services);
        var data = DataImportRunTests.FakeStreamingData.Create();
        var run = new DataImportRun("orders", new DataImportSettings { KeyTemplate = "/id" }, () => new EndlessLines(), 0, data, admin: null, channelCapacity: 16);

        sessions.Start(run);
        await WaitUntilAsync(() => run.EntriesCommitted > 0);
        _auth.SignIn("bob");
        await WaitUntilAsync(() => run.IsFinished);

        Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Cancelled));
    }

    [Test]
    public void The_sessions_need_the_circuit_services() =>
        Assert.That(() => new DataImportSessions(null!), Throws.ArgumentNullException);

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!condition())
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "the condition was never met");
            await Task.Delay(10);
        }
    }

    /// <summary>A file that never ends: one NDJSON line per read, a little apart.</summary>
    private sealed class EndlessLines : Stream
    {
        private long _next;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            Thread.Sleep(2);
            var line = Encoding.UTF8.GetBytes("{\"id\":\"k" + _next++.ToString("D12", System.Globalization.CultureInfo.InvariantCulture) + "\"}\n");
            line.CopyTo(buffer);
            return line.Length;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(2, cancellationToken);
            var line = Encoding.UTF8.GetBytes("{\"id\":\"k" + _next++.ToString("D12", System.Globalization.CultureInfo.InvariantCulture) + "\"}\n");
            line.CopyTo(buffer.Span);
            return line.Length;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
