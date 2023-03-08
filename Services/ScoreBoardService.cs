using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Scoreboard.Models;

namespace Coflnet.Scoreboard.Services;
public class ScoreboardService
{
    IConfiguration config;
    Cassandra.ISession _session;
    private bool ranCreate;
    private ILogger<ScoreboardService> logger;

    public ScoreboardService(IConfiguration config, ILogger<ScoreboardService> logger)
    {
        this.config = config;
        this.logger = logger;
    }

    public async Task<Cassandra.ISession> GetSession(string keyspace = "scoreboards")
    {
        if (_session != null)
            return _session;
        var cluster = Cluster.Builder()
                            .WithCredentials(config["CASSANDRA:USER"], config["CASSANDRA:PASSWORD"])
                            .AddContactPoints(config["CASSANDRA:HOSTS"]?.Split(",") ?? throw new System.Exception("No ASSANDRA:HOSTS defined in config"))
                            .Build();
        if (keyspace == null)
            return await cluster.ConnectAsync();
        _session = await cluster.ConnectAsync(keyspace);
        return _session;
    }

    public async Task<IEnumerable<BoardScore>> GetScoresAround(string boardSlug, string userId, int count = 1)
    {
        var session = await GetSession();
        //var mapper = new Mapper(session);
        var table = new Table<BoardScore>(session);
        table.CreateIfNotExists();
        var userScore = (await table.Where(f => f.Slug == boardSlug && f.UserId == userId).Take(1).ExecuteAsync()).First();
        var belowTask = table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId && f.Score < userScore.Score)
                    .OrderByDescending(s => s.Score).Take(count).ExecuteAsync();
        var aboveTask = table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId && f.Score > userScore.Score)
                    .OrderBy(s => s.Score).Take(count).ExecuteAsync();
        var below = await belowTask;
        var above = await aboveTask;
        var scores = new List<BoardScore>();
        scores.Add(userScore);
        scores.AddRange(below);
        scores.AddRange(above);
        return scores.OrderBy(s => s.Score);
    }

    public async Task<long> GetOwnRank(string boardSlug, string userId)
    {
        var session = await GetSession();
        //var mapper = new Mapper(session);
        var table = new Table<BoardScore>(session);
        table.CreateIfNotExists();
        var userScore = (await table.Where(f => f.Slug == boardSlug && f.UserId == userId).Take(1).ExecuteAsync()).FirstOrDefault();
        if (userScore == null)
            return -1;
        var bucketTable = new Table<Bucket>(session);

        var userOffset = (await table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId && f.Score > userScore.Score).Select(s => s.BucketId).ExecuteAsync()).Count();


        // if offset is above 1000 the bucket needs to be adjusted
        if (userOffset > 1000)
        {
            var bucketTask = bucketTable.Where(f => f.Slug == boardSlug).ExecuteAsync();
            var toBeMoved = (await table.Where(f => f.Slug == boardSlug && f.BucketId == userScore.BucketId).ExecuteAsync())
                    .OrderByDescending(s => s.Score).Skip(1000).ToList();

            // move all scores to the next bucket
            foreach (var score in toBeMoved)
            {
                var deleteStatement = table.Where(f => f.Slug == score.Slug && f.BucketId == score.BucketId && f.Score == score.Score && f.UserId == score.UserId).Delete();
                
                deleteStatement.SetConsistencyLevel(ConsistencyLevel.Quorum);
                var res =await session.ExecuteAsync(deleteStatement);
                score.BucketId++;
                var statement = table.Insert(score);
                statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
                await session.ExecuteAsync(statement);
                if (score.Score % 10 == 0)
                {
                    logger.LogInformation(deleteStatement.ToString() + res.FirstOrDefault()?.ToString());
                    logger.LogInformation($"Moving score {score.Score} from {score.UserId} to bucket {score.BucketId}");
                }
            }
            // adjust bucket min score
            await bucketTable.Insert(new Bucket()
            {
                Slug = boardSlug,
                BucketId = userScore.BucketId + 1,
                MinimumScore = toBeMoved.First().Score + 1
            }).ExecuteAsync();
            var buckets = (await bucketTask).ToList();
        }
        logger.LogInformation($"User {userId} has offset {userOffset} in bucket {userScore.BucketId}");

        // every bucket holds exactly 1000 scores
        return userScore.BucketId * 1000 + userOffset + 1;
    }

    public async Task AddScore(string boardSlug, string userId, int score, byte confidence)
    {
        await Create();
        var session = await GetSession();
        var table = new Table<BoardScore>(session);
        var userScore = (await table.Where(f => f.Slug == boardSlug && f.UserId == userId).Take(1).ExecuteAsync()).FirstOrDefault();
        logger.LogInformation($"Adding score {score} for user {userId}");
        if (userScore != null)
        {
            var statement = table.Insert(new BoardScore()
            {
                Score = score,
                Confidence = confidence,
                UserId = userId,
                Slug = boardSlug,
                BucketId = userScore.BucketId,
            });
            statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            await session.ExecuteAsync(statement);
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

            statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
            await session.ExecuteAsync(statement);
        }
    }

    public async Task Create()
    {
        if (ranCreate)
            return;
        ranCreate = true;

        var session = await GetSession(null);
        session.DeleteKeyspace("scoreboards");

        var replication = new Dictionary<string, string>()
            {
                {"class", config["CASSANDRA:REPLICATION_CLASS"]},
                {"replication_factor", config["CASSANDRA:REPLICATION_FACTOR"]}
            };
        session.CreateKeyspaceIfNotExists("scoreboards", replication);
        session.ChangeKeyspace("scoreboards");

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
        var bucket = (await bucketTable.Where(f => f.Slug == boardSlug && f.MinimumScore < score).Take(1).ExecuteAsync()).ToList().FirstOrDefault();
        if (bucket == null)
        {
            // create new bucket
            var newBucket = new Bucket(boardSlug, 0);
            var bucketCreate = await bucketTable.Where(b => b.Slug == boardSlug && b.MinimumScore == 0).Select(f => new Bucket() { BucketId = 0 }).Update().ExecuteAsync();
            //bucketCreate.SetConsistencyLevel(ConsistencyLevel.Quorum);
            //await session.ExecuteAsync(bucketCreate);
            logger.LogInformation($"Created new bucket {newBucket.BucketId} for {boardSlug}");
            return (await bucketTable.Where(f => f.Slug == boardSlug && f.MinimumScore <= score).Take(1).ExecuteAsync()).ToList().FirstOrDefault();
        }
        return bucket;
    }
}

