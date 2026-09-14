using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// End-to-end HTTP tests of the real <c>/health/backup</c> endpoint over a TestServer,
/// mapped through the same <see cref="RepoContextBackupHealthEndpoint.CreateOptions"/>
/// factory the host uses, so a drift between the mapping under test and the one the
/// host serves is caught.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2980. The endpoint carried a bare tag predicate and no response writer, so
/// the framework default wrote the aggregate status word alone and
/// <see cref="RepoContextBackupHealthCheck"/>'s description - which tree, what the
/// sink holds, when, and the last failure text - was computed on every probe and
/// dropped at the HTTP boundary. This was confirmed off the wire before it was fixed:
/// a live container answered <c>503</c> with a body of exactly <c>Unhealthy</c>.
/// </para>
/// <para>
/// The property under test is <b>attributability</b>, not that a body exists. The two
/// assertions that fail if the response writer is removed are
/// <see cref="Two_unhealthy_backup_states_are_distinguishable_on_the_wire"/> and
/// <see cref="A_disabled_backup_is_distinguishable_from_a_protected_one"/>. The second
/// is the more alarming pair: both are <c>Healthy</c> with a 200, so without the
/// writer a container capturing nothing anywhere and a container with a verified
/// backup are byte-identical on this endpoint.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextBackupHealthEndpointTests
{
    private const string Tree = "sys-repocontext-memory";

    private static async Task<(HttpStatusCode Status, string Body)> ProbeAsync(
        RepoContextBackupStatus status)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(status);
        builder.Services.AddHealthChecks()
            .AddCheck<RepoContextBackupHealthCheck>(
                RepoContextBackupHealthCheck.Name,
                tags: new[] { RepoContextHostBuilder.BackupTag });

        await using var app = builder.Build();
        app.MapHealthChecks(
            RepoContextHostBuilder.BackupPath,
            RepoContextBackupHealthEndpoint.CreateOptions(RepoContextHostBuilder.BackupTag));

        await app.StartAsync();
        try
        {
            using var client = app.GetTestClient();
            using var response = await client.GetAsync(RepoContextHostBuilder.BackupPath);
            return (response.StatusCode, await response.Content.ReadAsStringAsync());
        }
        finally
        {
            await app.StopAsync();
        }
    }

    private static RepoContextBackupStatus Disabled() => new(enabled: false, scopedTreeId: Tree);

    private static RepoContextBackupStatus Protected()
    {
        var status = new RepoContextBackupStatus(enabled: true, scopedTreeId: Tree);
        status.RecordSinkInventory(3, "backup-0003", new DateTimeOffset(2026, 9, 13, 4, 5, 6, TimeSpan.Zero));
        status.RecordCapture(
            "backup-0003",
            Tree,
            entryCount: 4271,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: new DateTimeOffset(2026, 9, 13, 4, 5, 6, TimeSpan.Zero));
        return status;
    }

    private static RepoContextBackupStatus CapturedNothing()
    {
        var status = new RepoContextBackupStatus(enabled: true, scopedTreeId: Tree);
        status.RecordCapture(
            "backup-0001",
            Tree,
            entryCount: 0,
            isFull: true,
            requestedIncremental: false,
            capturedAtUtc: new DateTimeOffset(2026, 9, 13, 4, 5, 6, TimeSpan.Zero));
        return status;
    }

    // RecordCapture clears the last failure, so the order here is load-bearing: a
    // capture that succeeded and was then followed by a failure is a broken cadence
    // over recoverable data, which is a materially better position than never having
    // captured at all, and the two are reported apart.
    private static RepoContextBackupStatus FailingAfterCapture()
    {
        var status = Protected();
        status.RecordFailure("sink refused the write: 403 AuthorizationFailure");
        return status;
    }

    private static RepoContextBackupStatus FailingUnprotected()
    {
        var status = new RepoContextBackupStatus(enabled: true, scopedTreeId: Tree);
        status.RecordFailure("sink unreachable: connection timed out");
        return status;
    }

    [Test]
    public async Task The_body_names_the_component_and_carries_its_diagnosis()
    {
        var (status, body) = await ProbeAsync(Protected());

        Assert.That(status, Is.EqualTo(HttpStatusCode.OK));

        var lines = body.Split('\n');
        Assert.Multiple(() =>
        {
            Assert.That(lines[0], Is.EqualTo("Healthy"), "the first line must remain the aggregate status word");
            Assert.That(lines, Has.Length.EqualTo(2), "one component must occupy exactly one line");
            Assert.That(lines[1], Does.StartWith($"{RepoContextBackupHealthCheck.Name}: Healthy: "));
            Assert.That(lines[1], Does.Contain(Tree), "the body must name which tree is in scope");
        });
    }

    /// <summary>
    /// The anti-vacuity assertion. Both states are Unhealthy with a 503, so without
    /// the response writer both bodies collapse to the identical string "Unhealthy"
    /// and the two conditions become indistinguishable on the wire - despite having
    /// materially different remedies, because only one of them has recoverable data.
    /// </summary>
    [Test]
    public async Task Two_unhealthy_backup_states_are_distinguishable_on_the_wire()
    {
        var (afterCaptureStatus, afterCaptureBody) = await ProbeAsync(FailingAfterCapture());
        var (unprotectedStatus, unprotectedBody) = await ProbeAsync(FailingUnprotected());

        Assert.Multiple(() =>
        {
            Assert.That(afterCaptureStatus, Is.EqualTo(HttpStatusCode.ServiceUnavailable));
            Assert.That(unprotectedStatus, Is.EqualTo(HttpStatusCode.ServiceUnavailable));

            Assert.That(
                afterCaptureBody,
                Is.Not.EqualTo(unprotectedBody),
                "two Unhealthy backup states must not serialise identically");

            Assert.That(afterCaptureBody, Does.Contain("earlier captures from this container are still"));
            Assert.That(unprotectedBody, Does.Contain("has never captured anything"));
        });
    }

    /// <summary>
    /// The sharper pair: both are Healthy with a 200. Without the response writer a
    /// container whose memory is captured nowhere at all and a container with a
    /// verified backup produce byte-identical responses on this endpoint.
    /// </summary>
    [Test]
    public async Task A_disabled_backup_is_distinguishable_from_a_protected_one()
    {
        var (disabledStatus, disabledBody) = await ProbeAsync(Disabled());
        var (protectedStatus, protectedBody) = await ProbeAsync(Protected());

        Assert.Multiple(() =>
        {
            Assert.That(disabledStatus, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(protectedStatus, Is.EqualTo(HttpStatusCode.OK));

            Assert.That(
                disabledBody,
                Is.Not.EqualTo(protectedBody),
                "a disabled backup and a protected one must not serialise identically");

            Assert.That(disabledBody, Does.Contain("DISABLED"));
            Assert.That(disabledBody, Does.Contain("NOT being captured anywhere"));
            Assert.That(protectedBody, Does.Not.Contain("DISABLED"));
        });
    }

    /// <summary>
    /// The four things <c>docs/lattice.api.mcp.repocontext/container.md</c> claims the
    /// response body carries. The claim was written against the check rather than the
    /// wire and was false until issue #2980; this pins it so it cannot become false
    /// again without a red test.
    /// </summary>
    [Test]
    public async Task The_body_carries_the_four_things_the_documentation_claims()
    {
        var (_, body) = await ProbeAsync(FailingAfterCapture());

        Assert.Multiple(() =>
        {
            Assert.That(body, Does.Contain(Tree), "which tree");
            Assert.That(body, Does.Contain("4271"), "how many entries");
            Assert.That(body, Does.Contain("2026-09-13T04:05:06"), "when");
            Assert.That(
                body,
                Does.Contain("403 AuthorizationFailure"),
                "the last failure text");
        });
    }

    [Test]
    public async Task A_capture_describing_zero_entries_is_visible_on_the_wire()
    {
        var (status, body) = await ProbeAsync(CapturedNothing());

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.OK), "Degraded maps to 200 by framework default");
            Assert.That(body.Split('\n')[0], Is.EqualTo("Degraded"));
            Assert.That(body, Does.Contain("ZERO entries"));
        });
    }

    /// <summary>
    /// Pins the verdict. The added detail must not move the status code or the first
    /// line, because those are what an orchestrator routes on and what the acceptance
    /// evidence records.
    /// </summary>
    [Test]
    public async Task The_status_code_and_first_line_are_unchanged_by_the_added_detail()
    {
        var cases = new (RepoContextBackupStatus Status, HttpStatusCode Expected, string FirstLine)[]
        {
            (Protected(), HttpStatusCode.OK, "Healthy"),
            (Disabled(), HttpStatusCode.OK, "Healthy"),
            (CapturedNothing(), HttpStatusCode.OK, "Degraded"),
            (FailingUnprotected(), HttpStatusCode.ServiceUnavailable, "Unhealthy"),
        };

        foreach (var (status, expected, firstLine) in cases)
        {
            var (actual, body) = await ProbeAsync(status);

            Assert.Multiple(() =>
            {
                Assert.That(actual, Is.EqualTo(expected), $"status code for {firstLine}");
                Assert.That(body.Split('\n')[0], Is.EqualTo(firstLine));
            });
        }
    }

    /// <summary>
    /// The predicate must select the backup component and nothing else. A widened
    /// predicate would turn this endpoint into a second readiness signal, which is
    /// exactly what it must not be: a failing backup must not pull the container out
    /// of rotation.
    /// </summary>
    [Test]
    public async Task The_endpoint_reports_only_the_backup_component()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(Protected());
        builder.Services.AddSingleton(new RepoContextReadinessState());
        builder.Services.AddHealthChecks()
            .AddCheck<RepoContextBackupHealthCheck>(
                RepoContextBackupHealthCheck.Name,
                tags: new[] { RepoContextHostBuilder.BackupTag })
            .AddCheck<RepoContextReadinessHealthCheck>(
                RepoContextReadinessHealthCheck.Name,
                tags: new[] { RepoContextHostBuilder.ReadinessTag });

        await using var app = builder.Build();
        app.MapHealthChecks(
            RepoContextHostBuilder.BackupPath,
            RepoContextBackupHealthEndpoint.CreateOptions(RepoContextHostBuilder.BackupTag));

        await app.StartAsync();
        try
        {
            using var client = app.GetTestClient();
            using var response = await client.GetAsync(RepoContextHostBuilder.BackupPath);
            var body = await response.Content.ReadAsStringAsync();

            Assert.Multiple(() =>
            {
                Assert.That(body.Split('\n'), Has.Length.EqualTo(2));
                Assert.That(body, Does.Contain(RepoContextBackupHealthCheck.Name + ": "));
                Assert.That(
                    body,
                    Does.Not.Contain(RepoContextReadinessHealthCheck.Name + ": "),
                    "the readiness component must not appear on the backup endpoint");
            });
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Test]
    public void The_factory_rejects_a_null_or_empty_tag()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => RepoContextBackupHealthEndpoint.CreateOptions(null!));
            Assert.Throws<ArgumentException>(
                () => RepoContextBackupHealthEndpoint.CreateOptions(string.Empty));
        });
    }
}
