using Coflnet.Leaderboard.Models;
using Coflnet.Leaderboard.Services;
using Microsoft.AspNetCore.Mvc;

namespace Coflnet.Leaderboard;

[ApiController]
[Route("[controller]")]
public class ScoresController : ControllerBase
{
    private readonly ILogger<ScoresController> _logger;
    private readonly LeaderboardService service;

    public ScoresController(ILogger<ScoresController> logger, LeaderboardService service)
    {
        _logger = logger;
        this.service = service;
    }
    /// <summary>
    /// Add a score to the leaderboard
    /// </summary>
    /// <param name="leaderboardSlug"></param>
    /// <param name="userId"></param>
    /// <param name="score"></param>
    /// <returns></returns>
    [Route("{leaderboardSlug}")]
    [HttpPost]
    public async Task AddScore(string leaderboardSlug, ScoreCreate score)
    {
        await service.AddScore(leaderboardSlug, score);
    }

    [Route("{leaderboardSlug}/mock")]
    [HttpPost]
    public async Task AddMockScore(string leaderboardSlug, int start = 0)
    {
        for (int i = start; i < start + 2000; i++)
        {
            await service.AddScore(leaderboardSlug, new ScoreCreate()
            {
                UserId = i.ToString(),
                Score = i,
                Confidence = 100
            });
        }
    }
    /// <summary>
    /// Get the leaderboard for a specific slug
    /// </summary>
    /// <param name="leaderboardSlug"></param>
    /// <param name="userId"></param>
    /// <returns></returns>
    [Route("{leaderboardSlug}/user/{userId}")]
    [HttpGet]
    public async Task<IEnumerable<BoardScore>> GetLeaderboard(string leaderboardSlug, string userId, int before = 10, int after = 10)
    {
        return await service.GetScoresAround(leaderboardSlug, userId, before, after);
    }

    /// <summary>
    /// GetOwnRank
    /// </summary>
    /// <param name="leaderboardSlug"></param>
    /// <param name="userId"></param>
    /// <returns></returns>
    [Route("{leaderboardSlug}/user/{userId}/rank")]
    [HttpGet]
    public async Task<long> GetOwnRank(string leaderboardSlug, string userId)
    {
        return await service.GetOwnRank(leaderboardSlug, userId);
    }

    /// <summary>
    /// Get the leaderboard for a specific slug
    /// </summary>
    /// <param name="leaderboardSlug"></param>
    /// <param name="offset"></param>
    /// <param name="limit"></param>
    /// <returns></returns>
    [Route("{leaderboardSlug}")]
    [HttpGet]
    public async Task<IEnumerable<BoardScore>> GetLeaderboard(string leaderboardSlug, int offset = 0, int limit = 10)
    {
        return await service.GetScores(leaderboardSlug, offset, limit);
    }
}
