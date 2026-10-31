using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Orleans.Lattice.Schema;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// The schema rule workshop (U6, #5385), established as sam. Consumes the policy-as-code
/// pipeline's rule on <c>factory-lines</c> (<see cref="ContosoHarness.AuthorLinesPipelineRuleAsync"/>).
/// Produces a non-strict policy on <c>factory-lines</c> using every builder card and keeping
/// the pipeline's rule verbatim (<see cref="ContosoSchema.LinesWorkshop"/>), and versioning in
/// family <see cref="ContosoSchema.LinesFamily"/> advanced and migrated to version 2.
/// </summary>
public sealed class U6Contract() : ChapterContract("U6")
{
    private static readonly TimeSpan OperationPatience = TimeSpan.FromMinutes(6);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        await ContosoHarness.AuthorLinesPipelineRuleAsync(context.Estate);
        var schema = ContractFacades.Of(context, Personas.Sam.Id).Schema;
        var lines = Names.Trees.FactoryLines;

        if (!ContosoSchema.SameAs(await schema.GetPolicyAsync(lines), ContosoSchema.LinesWorkshop))
        {
            await schema.SetPolicyAsync(lines, ContosoSchema.LinesWorkshop);
        }

        if (await schema.GetVersionConfigAsync(lines) is null)
        {
            await schema.SetVersionConfigAsync(lines, new LatticeSchemaVersionConfig(ContosoSchema.LinesFamily, targetVersion: 1));
        }

        if (!(await schema.GetRegistryAsync(lines)).Descriptors.Any(d => d.SchemaId == ContosoSchema.LinesFamily))
        {
            await schema.RegisterSchemaAsync(lines, ContosoSchema.ParseRegistration(ContosoSchema.LinesRegistrationJson));
        }

        if ((await schema.GetVersionConfigAsync(lines))?.TargetVersion != 2)
        {
            var advance = await schema.StartAdvanceAndMigrateAsync(lines, 2);
            await ContractWait.SucceededAsync("the lines' advance and migrate to version 2", () => schema.GetSchemaOperationStatusAsync(advance.OperationId), OperationPatience);
        }
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var schema = ContractFacades.Of(context).Schema;
        var lines = Names.Trees.FactoryLines;
        var policy = await schema.GetPolicyAsync(lines);

        Require(policy is { StrictIngest: false }, "factory-lines has a non-strict policy");
        Require(ContosoSchema.Holds(policy, ContosoSchema.LinesPipelineRule), "the pipeline's rule is kept verbatim");
        Require(policy!.Rules.Count >= 10, $"the policy uses every card ({policy.Rules.Count} rules)");
        Require(policy.Rules.Any(rule => rule.Kind == LatticeSchemaRuleKind.Regex && rule.MemberPath == "owner"), "owner has a common format (email)");
        Require(policy.Rules.Any(rule => rule.Kind == LatticeSchemaRuleKind.Regex && rule.MemberPath == "since"), "since has a common format (ISO date)");
        Require(policy.Rules.Any(rule => rule.EncodingKind == LatticeSchemaEncodingKind.Json), "the whole value is well-formed JSON");
        Require(policy.Rules.Any(rule => rule.MaxByteLength is not null), "the whole value has a largest size");

        var config = await schema.GetVersionConfigAsync(lines);
        Require(config is { SchemaId: ContosoSchema.LinesFamily, TargetVersion: 2 }, "factory-lines is versioned at version 2");
    }
}
