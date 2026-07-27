using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Leaderboard.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using ISession = Cassandra.ISession;

namespace Coflnet.Leaderboard.Services;

public class LeaderboardServiceTests
{
    private ISession session;
    private LeaderboardService service;
    private IConfiguration config;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        config = new ConfigurationBuilder().AddInMemoryCollection(new System.Collections.Generic.Dictionary<string, string?>
        {
            { "CASSANDRA:HOSTS", "localhost" },
            { "CASSANDRA:KEYSPACE", "leaderboards" },
            { "CASSANDRA:REPLICATION_CLASS", "SimpleStrategy" },
            { "CASSANDRA:REPLICATION_FACTOR", "1" },
        }).Build();

        var cluster = global::Cassandra.Cluster.Builder().AddContactPoint("127.0.0.1").Build();
        var bootstrapSession = cluster.Connect();
        bootstrapSession.Execute("CREATE KEYSPACE IF NOT EXISTS leaderboards WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session = cluster.Connect("leaderboards");

        service = new LeaderboardService(config, NullLogger<LeaderboardService>.Instance, session);
    }

    [OneTimeTearDown]
    public void OneTimeTeardown()
    {
        session?.Dispose();
    }

    [Test]
    public async Task DeleteUserScores_RemovesUserAcrossAllBucketsButKeepsOthers()
    {
        await service.Create();
        var table = new Table<BoardScore>(session);
        var boardSlug = "test-delete-" + Guid.NewGuid().ToString("N");
        var targetUser = "target-user";
        var otherUserA = "other-user-a";
        var otherUserB = "other-user-b";

        // seed the target user's scores across three separate buckets (a user can end up
        // with stray rows across buckets via MoveScore/RezizeBucket in production)
        for (long bucketId = 0; bucketId < 3; bucketId++)
        {
            await Insert(table, session, boardSlug, bucketId, targetUser, 100 + bucketId);
            // other users' scores in the same buckets, must survive the delete
            await Insert(table, session, boardSlug, bucketId, otherUserA, 200 + bucketId);
            await Insert(table, session, boardSlug, bucketId, otherUserB, 300 + bucketId);
        }


        var deletedCount = await service.DeleteUserScores(boardSlug, targetUser);

        Assert.That(deletedCount, Is.EqualTo(3));

        var remainingTargetScores = await table.Where(f => f.Slug == boardSlug && f.UserId == targetUser).ExecuteAsync();
        Assert.That(remainingTargetScores.Count(), Is.EqualTo(0));

        var remainingOtherAScores = (await table.Where(f => f.Slug == boardSlug && f.UserId == otherUserA).ExecuteAsync()).ToList();
        Assert.That(remainingOtherAScores.Count, Is.EqualTo(3));

        var remainingOtherBScores = (await table.Where(f => f.Slug == boardSlug && f.UserId == otherUserB).ExecuteAsync()).ToList();
        Assert.That(remainingOtherBScores.Count, Is.EqualTo(3));

        // every bucket should still contain the other users' rows, just not the target user's
        for (long bucketId = 0; bucketId < 3; bucketId++)
        {
            var bucketScores = (await table.Where(f => f.Slug == boardSlug && f.BucketId == bucketId).ExecuteAsync()).ToList();
            Assert.That(bucketScores.Any(s => s.UserId == targetUser), Is.False, $"target user still present in bucket {bucketId}");
            Assert.That(bucketScores.Count(s => s.UserId == otherUserA || s.UserId == otherUserB), Is.EqualTo(2));
        }
    }

    private static async Task Insert(Table<BoardScore> table, ISession session, string boardSlug, long bucketId, string userId, long score)
    {
        var statement = table.Insert(new BoardScore()
        {
            Slug = boardSlug,
            BucketId = bucketId,
            UserId = userId,
            Score = score,
            Confidence = 100,
            TimeStamp = DateTime.UtcNow
        });
        statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
        await session.ExecuteAsync(statement);
    }
}
