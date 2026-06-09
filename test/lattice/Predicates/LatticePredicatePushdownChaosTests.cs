using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Chaos coverage of the predicate spine (translator + server-side evaluator).
/// Unlike the cluster-based chaos suites, the "varying and conflicting
/// operations" here are a storm of concurrently generated, structurally random
/// predicates evaluated against a shared, concurrently read pool of encoded
/// documents. The oracle is differential: for every randomly built
/// <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c>, the IR fold produced by
/// <see cref="LatticePredicateEvaluator"/> must agree with the compiled lambda
/// on every document - if the translator and evaluator ever disagree under
/// concurrency, the run fails with the offending predicate and document.
/// </summary>
[TestFixture]
[Category("Chaos")]
public class LatticePredicatePushdownChaosTests
{
    private const int WorkerCount = 8;
    private const int DocumentPoolSize = 256;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(10);

    private sealed record EncodedDocument(PredicatePerson Person, byte[] Bytes);

    private static PredicatePerson RandomPerson(Random rng)
    {
        string[] names = ["Alice", "Bob", "Carol", "Anil", "zoe", "Abba", "bobby", "Cleo", "", "AZ"];
        string[] cities = ["London", "Paris", "Berlin", "Rome", "Oslo"];
        string[] countries = ["UK", "FR", "DE", "IT", "NO"];

        return new PredicatePerson(
            Name: names[rng.Next(names.Length)],
            Age: rng.Next(-5, 100),
            Active: rng.Next(2) == 0,
            Score: Math.Round(rng.NextDouble(), 3),
            Nickname: rng.Next(3) == 0 ? null : names[rng.Next(names.Length)],
            Address: rng.Next(4) == 0
                ? null
                : new PredicateAddress(cities[rng.Next(cities.Length)], countries[rng.Next(countries.Length)]));
    }

    private static Expression BuildLeaf(Random rng, ParameterExpression p)
    {
        switch (rng.Next(9))
        {
            case 0:
                return Expression.GreaterThanOrEqual(Expression.Property(p, nameof(PredicatePerson.Age)), Expression.Constant(rng.Next(-5, 100)));
            case 1:
                return Expression.LessThan(Expression.Property(p, nameof(PredicatePerson.Age)), Expression.Constant(rng.Next(-5, 100)));
            case 2:
                return Expression.Equal(Expression.Property(p, nameof(PredicatePerson.Age)), Expression.Constant(rng.Next(-5, 100)));
            case 3:
                return Expression.GreaterThan(Expression.Property(p, nameof(PredicatePerson.Score)), Expression.Constant(Math.Round(rng.NextDouble(), 3)));
            case 4:
                return Expression.Property(p, nameof(PredicatePerson.Active));
            case 5:
            {
                string[] prefixes = ["A", "b", "C", "z", ""];
                var name = Expression.Property(p, nameof(PredicatePerson.Name));
                var method = typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!;
                return Expression.Call(name, method, Expression.Constant(prefixes[rng.Next(prefixes.Length)]));
            }
            case 6:
            {
                string[] needles = ["o", "b", "z", "a"];
                var name = Expression.Property(p, nameof(PredicatePerson.Name));
                var method = typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;
                return Expression.Call(name, method, Expression.Constant(needles[rng.Next(needles.Length)]));
            }
            case 7:
            {
                var nick = Expression.Property(p, nameof(PredicatePerson.Nickname));
                return Expression.Equal(nick, Expression.Constant(null, typeof(string)));
            }
            default:
            {
                string[] cities = ["London", "Paris", "Berlin", "Rome", "Oslo"];
                var addr = Expression.Property(p, nameof(PredicatePerson.Address));
                var city = Expression.Property(addr, nameof(PredicateAddress.City));
                return Expression.Equal(city, Expression.Constant(cities[rng.Next(cities.Length)]));
            }
        }
    }

    private static Expression BuildTree(Random rng, ParameterExpression p, int depth)
    {
        if (depth <= 0 || rng.Next(2) == 0)
        {
            var leaf = BuildLeaf(rng, p);
            return rng.Next(4) == 0 ? Expression.Not(leaf) : leaf;
        }

        var left = BuildTree(rng, p, depth - 1);
        var right = BuildTree(rng, p, depth - 1);
        return rng.Next(2) == 0 ? Expression.AndAlso(left, right) : Expression.OrElse(left, right);
    }

    private static Expression<Func<PredicatePerson, bool>> RandomPredicate(Random rng)
    {
        var p = Expression.Parameter(typeof(PredicatePerson), "p");
        var body = BuildTree(rng, p, depth: 3);
        return Expression.Lambda<Func<PredicatePerson, bool>>(body, p);
    }

    [Test]
    public async Task Chaos_evaluator_agrees_with_compiled_lambda_under_concurrency()
    {
        // Shared, concurrently read document pool (the "conflicting" reads).
        var seedRng = new Random(20260609);
        var pool = new EncodedDocument[DocumentPoolSize];
        for (int i = 0; i < pool.Length; i++)
        {
            var person = RandomPerson(seedRng);
            pool[i] = new EncodedDocument(person, JsonLatticeSerializer<PredicatePerson>.Default.Serialize(person));
        }

        var failures = new ConcurrentBag<string>();
        var evaluations = new ConcurrentDictionary<int, long>();
        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();
        for (int w = 0; w < WorkerCount; w++)
        {
            int workerId = w;
            workers.Add(Task.Run(() =>
            {
                var rng = new Random(workerId * 7919 + 17);
                long local = 0;
                while (!ct.IsCancellationRequested)
                {
                    var predicate = RandomPredicate(rng);
                    Func<PredicatePerson, bool> compiled;
                    LatticePredicateNode ir;
                    try
                    {
                        compiled = predicate.Compile();
                        ir = LatticePredicateTranslator.Translate(predicate);
                    }
                    catch (Exception ex)
                    {
                        failures.Add($"worker{workerId}: translate/compile threw for '{predicate}': {ex.GetType().Name}: {ex.Message}");
                        break;
                    }

                    foreach (var doc in pool)
                    {
                        bool expected;
                        try
                        {
                            expected = compiled(doc.Person);
                        }
                        catch (NullReferenceException)
                        {
                            // Null-safe evaluator divergence on a null
                            // intermediate member; no ground truth to compare.
                            continue;
                        }

                        bool actual = LatticePredicateEvaluator.Matches(doc.Bytes, ir);
                        local++;
                        if (expected != actual)
                        {
                            failures.Add($"worker{workerId}: mismatch for '{predicate}' on {doc.Person.Name}/{doc.Person.Age}: expected {expected}, got {actual}");
                        }
                    }
                }

                evaluations[workerId] = local;
            }, ct));
        }

        await Task.WhenAll(workers);

        long total = evaluations.Values.Sum();
        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} evaluator/lambda disagreements (first 20):\n" +
                string.Join("\n", failures.Take(20)));
            Assert.That(total, Is.GreaterThan(0), "Workers must have performed at least one evaluation.");
        });

        TestContext.Out.WriteLine($"Chaos predicate evaluations: {total} across {WorkerCount} workers.");
    }
}
