using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Leaderboard.Models;

namespace Coflnet.Leaderboard.Services;
public class LeaderboardService
{
    IConfiguration config;
    Cassandra.ISession _session;
    private bool ranCreate;
    private ILogger<LeaderboardService> logger;
    private SemaphoreSlim modificationLock = new SemaphoreSlim(1, 1);

    public LeaderboardService(IConfiguration config, ILogger<LeaderboardService> logger)
    {
        this.config = config;
        this.logger = logger;
    }

    public async Task<Cassandra.ISession> GetSession()
    {
        if (_session != null)
            return _session;
        var builderBuilder = () => Cluster.Builder()
                            .WithCredentials(config["CASSANDRA:USER"], config["CASSANDRA:PASSWORD"])
                            .AddContactPoints(config["CASSANDRA:HOSTS"]?.Split(",") ?? throw new System.Exception("No ASSANDRA:HOSTS defined in config"));
        var cluster = builderBuilder()
                            .WithDefaultKeyspace(config["CASSANDRA:KEYSPACE"])
                            .Build();
        try
        {

            _session = await cluster.ConnectAsync();
        }
        catch (Cassandra.InvalidQueryException e)
        {
            logger.LogError(e, "Could not connect to cassandra");
            if (e.Message != $"Keyspace '{config["CASSANDRA:KEYSPACE"]}' does not exist")
                throw;
            var replication = new Dictionary<string, string>()
            {
                {"class", config["CASSANDRA:REPLICATION_CLASS"]},
                {"replication_factor", config["CASSANDRA:REPLICATION_FACTOR"]}
            };
            var session = await builderBuilder().Build().ConnectAsync();
            session.CreateKeyspaceIfNotExists(config["CASSANDRA:KEYSPACE"], replication);
            session.ChangeKeyspace(config["CASSANDRA:KEYSPACE"]);
            _session = session;
        }
        return _session;
    }

    public async Task<IEnumerable<BoardScore>> GetScoresAround(string boardSlug, string userId, int before = 1, int after = 0)
    {
        var session = await GetSession();
        //var mapper = new Mapper(session);
        var table = new Table<BoardScore>(session);
        var userScore = (await table.Where(f => f.Slug == boardSlug && f.UserId == userId).Take(1).ExecuteAsync()).First();
        var belowTask = table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId && f.Score < userScore.Score)
                    .OrderByDescending(s => s.Score).Take(after).ExecuteAsync();
        var aboveTask = table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId && f.Score >= userScore.Score)
                    .OrderBy(s => s.Score).Take(before + 1).ExecuteAsync();
        var below = await belowTask;
        var above = await aboveTask;
        var scores = new List<BoardScore>();
        scores.Add(userScore);
        scores.AddRange(below);
        scores.AddRange(above.Where(s => s.UserId != userId));
        return scores.OrderBy(s => s.Score);
    }

    public async Task<IEnumerable<BoardScore>> GetScores(string boardSlug, int offset, int amount)
    {
        var session = await GetSession();
        //var mapper = new Mapper(session);
        var table = new Table<BoardScore>(session);
        var bucketTable = new Table<Bucket>(session);
        long bucketId = offset / 1000;
        var extraOffset = offset % 1000;
        var scores = (await table.Where(f => f.Slug == boardSlug && f.BucketId == bucketId)
                    .OrderByDescending(s => s.Score).Take(amount + extraOffset).ExecuteAsync()).ToList();
        logger.LogInformation($"Getting scores for {boardSlug} with offset {offset} and amount {amount} and extra offset {extraOffset} and bucket {bucketId}");
        if (bucketId > 0 && scores.Count() < amount + extraOffset)
            await RezizeBucket(boardSlug, session, table, bucketId - 1);
        var deduplicated = scores.GroupBy(s => s.UserId).Select(g => g.OrderByDescending(s => s.TimeStamp).First()).ToList();
        if(deduplicated.Count != scores.Count)
        {
            logger.LogInformation($"Deduplicated {scores.Count - deduplicated.Count} scores");
            SheduleBoardModification(boardSlug, async () => await CleanBucket(boardSlug, bucketId));
        }
        return deduplicated.Skip(extraOffset);
    }

    private void SheduleBoardModification(string boardId, Func<Task> todo)
    {
        Task.Run(async () =>
        {
            await modificationLock.WaitAsync();
            try
            {
                await todo();
            }
            finally
            {
                modificationLock.Release();
            }
        });
    }

    private async Task CleanBucket(string boardSlug, long bucketId)
    {
        var session = await GetSession();
        var table = new Table<BoardScore>(session);
        // remove dupplicate scores and keep newest 
        var scores = (await table.Where(f => f.Slug == boardSlug && f.BucketId == bucketId)
                    .OrderByDescending(s => s.Score).Take(2000)
                    .ExecuteAsync()).ToList();
        var dupplicates = scores.GroupBy(s => s.UserId).Where(g => g.Count() > 1).SelectMany(g => g.OrderByDescending(s=>s.TimeStamp).Skip(1)).ToList();
        foreach (var item in dupplicates)
        {
            await table.Where(f => f.Slug == boardSlug && f.BucketId == bucketId && f.UserId == item.UserId && f.Score == item.Score).Delete().ExecuteAsync();
        }
    }

    public async Task<long> GetOwnRank(string boardSlug, string userId)
    {
        var session = await GetSession();
        //var mapper = new Mapper(session);
        var table = new Table<BoardScore>(session);
        var userScores = (await table.Where(f => f.Slug == boardSlug && f.UserId == userId).Take(10).ExecuteAsync()).OrderByDescending(s => s.TimeStamp).ToList();
        if (userScores.Count == 0)
            return -1;
        var userScore = userScores.First();
        foreach (var item in userScores.Skip(1))
        {
            // delete all other scores
            await table.Where(f => f.Slug == boardSlug && f.UserId == userId && f.BucketId == item.BucketId && f.Score == item.Score).Delete().ExecuteAsync();
            logger.LogInformation($"Deleted score {item.Score} in bucket {item.BucketId} for user {userId}");
        }

        var scores = (await table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId && f.Score >= userScore.Score)
                    .ThenByDescending(k => k.Score).Select(s => s.Score)
                    .ExecuteAsync()).ToList();
        var userOffset = scores.LastIndexOf(userScore.Score);
        Console.WriteLine($"User offset {scores.Count()} {string.Join(",", scores.Take(10))}");


        // if offset is above 1000 the bucket needs to be adjusted
        if (userOffset > 1000)
        {
            var bucketId = userScore.BucketId;
            SheduleBoardModification(boardSlug, async () => await RezizeBucket(boardSlug, session, table, bucketId));
        }
        if (scores.FirstOrDefault() == userScore.Score && userScore.BucketId > 0)
        {
            // move down a bucket
            var toBeMoved = (await table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId && f.Score == userScore.Score && f.UserId == userScore.UserId).ExecuteAsync())
                    .OrderByDescending(s => s.Score).First();
            await MoveScore(toBeMoved, session, table, toBeMoved.BucketId - 1);
        }
        logger.LogInformation($"User {userId} has offset {userOffset} in bucket {userScore.BucketId} with score {userScore.Score}");

        // every bucket holds exactly 1000 scores
        return userScore.BucketId * 1000 + userOffset + 1;
    }

    private async Task RezizeBucket(string boardSlug, Cassandra.ISession session, Table<BoardScore> table, long bucketId)
    {
        var bucketTable = new Table<Bucket>(session);
        var bucketTask = bucketTable.Where(f => f.Slug == boardSlug).ExecuteAsync();
        var toBeMoved = (await table.Where(f => f.Slug == boardSlug && f.BucketId == bucketId).ExecuteAsync())
                .OrderByDescending(s => s.Score).Skip(1000).ToList();

        // move all scores to the next bucket
        await Parallel.ForEachAsync(toBeMoved, async (score, token) =>
        {
            var nextBucket = score.BucketId + 1;
            await MoveScore(score, session, table, nextBucket);
        });
        // adjust bucket min score
        await bucketTable.Insert(new Bucket()
        {
            Slug = boardSlug,
            BucketId = bucketId,
            MinimumScore = toBeMoved.First().Score + 1
        }).ExecuteAsync();
        var buckets = (await bucketTask).ToList();
    }

    private async Task MoveScore(BoardScore score, Cassandra.ISession session, Table<BoardScore> table, long nextBucket)
    {
        var deleteStatement = table.Where(f => f.Slug == score.Slug && f.BucketId == score.BucketId && f.Score == score.Score && f.UserId == score.UserId).Delete();

        deleteStatement.SetConsistencyLevel(ConsistencyLevel.Quorum);
        var res = await session.ExecuteAsync(deleteStatement);
        var statement = table.Insert(new BoardScore()
        {
            BucketId = nextBucket,
            Score = score.Score,
            Slug = score.Slug,
            UserId = score.UserId,
            Confidence = score.Confidence,
            TimeStamp = score.TimeStamp
        });
        statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
        await session.ExecuteAsync(statement);
        if (score.Score % 25 == 0)
        {
            logger.LogInformation(deleteStatement.ToString() + res.FirstOrDefault()?.ToString());
            logger.LogInformation($"Moving score {score.Score} from {score.UserId} from bucket {score.BucketId} {nextBucket}");
        }
    }

    public async Task AddScore(string boardSlug, string userId, long score, byte confidence)
    {
        await Create();
        var session = await GetSession();
        var table = new Table<BoardScore>(session);
        var userScore = (await table.Where(f => f.Slug == boardSlug && f.UserId == userId).Take(1).ExecuteAsync()).FirstOrDefault();
        logger.LogInformation($"Adding score {score} for user {userId}");
        if (userScore != null)
        {
            if(userScore.Score == score && userScore.Confidence == confidence)
            {
                logger.LogDebug($"Score {score} for user {userId} already exists");
                return;
            }
            var newScore = new BoardScore()
            {
                Score = score,
                Confidence = confidence,
                UserId = userId,
                Slug = boardSlug,
                BucketId = userScore.BucketId,
            };
            var statement = table.Insert(newScore);
            statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            await session.ExecuteAsync(statement);
            // delete old score
            var deleteStatement = table.Where(f => f.Slug == boardSlug && f.UserId == userId && f.BucketId == userScore.BucketId && f.Score == userScore.Score).Delete();
            deleteStatement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            await session.ExecuteAsync(deleteStatement);
            // check if score needs to be moved to bucket above or below 
            if(userScore.Score > score)
            {
                // score is now smaller
                var topScoreInNextBucket = (await table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId + 1).OrderByDescending(s => s.Score).Take(1).ExecuteAsync()).FirstOrDefault();
                logger.LogInformation($"Top score in next bucket {topScoreInNextBucket?.Score} for user {userId} in bucket {userScore.BucketId + 1}");
                if(topScoreInNextBucket != null && topScoreInNextBucket.Score > score)
                {
                    // move down
                    await MoveScore(newScore, session, table, userScore.BucketId + 1);
                }
            }
            else if(userScore.Score < score)
            {
                var lowestScoreInPreviousBucket = (await table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId - 1).OrderBy(s => s.Score).Take(1).ExecuteAsync()).FirstOrDefault();
                logger.LogInformation($"Lowest score in previous bucket {lowestScoreInPreviousBucket?.Score} for user {userId} in bucket {userScore.BucketId - 1}");
                if(lowestScoreInPreviousBucket != null && lowestScoreInPreviousBucket.Score < score)
                {
                    // move up
                    await MoveScore(newScore, session, table, userScore.BucketId - 1);
                }
            }
        }
        else
        {
            // find bucket 
            var bucket = await FindBucket(boardSlug, score, session);
            var statement = table.Insert(new BoardScore()
            {
                Slug = boardSlug,
                BucketId = bucket.BucketId,
                UserId = userId,
                Score = score,
                Confidence = confidence
            });
            logger.LogInformation($"Inserting score {score} for user {userId} into bucket {bucket.BucketId} with min score {bucket.MinimumScore}");

            statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            await session.ExecuteAsync(statement);
        }
    }

    public async Task Create()
    {
        if (ranCreate)
            return;
        ranCreate = true;

        var session = await GetSession();
        //session.DeleteKeyspace("leaderboards");

        var table = new Table<BoardScore>(session);
        table.CreateIfNotExists();
        var bucketTable = new Table<Bucket>(session);
        // drop table
        //session.Execute("DROP TABLE IF EXISTS buckets");

        bucketTable.CreateIfNotExists();
    }

    private async Task<Bucket?> FindBucket(string boardSlug, long score, Cassandra.ISession session)
    {
        var bucketTable = new Table<Bucket>(session);
        bucketTable.CreateIfNotExists();
        var bucket = (await bucketTable.Where(f => f.Slug == boardSlug && f.MinimumScore <= score).Take(1).ExecuteAsync()).ToList().FirstOrDefault();
        if (bucket == null)
        {
            // create new bucket
            var newBucket = new Bucket(boardSlug, 0);
            var bucketCreate = await bucketTable.Where(b => b.Slug == boardSlug && b.MinimumScore == long.MinValue).Select(f => new Bucket() { BucketId = 0 }).Update().ExecuteAsync();
            //bucketCreate.SetConsistencyLevel(ConsistencyLevel.Quorum);
            //await session.ExecuteAsync(bucketCreate);
            logger.LogInformation($"Created new bucket {newBucket.BucketId} for {boardSlug}");
            return (await bucketTable.Where(f => f.Slug == boardSlug && f.MinimumScore <= score).Take(1).ExecuteAsync()).ToList().FirstOrDefault();
        }
        return bucket;
    }
}

