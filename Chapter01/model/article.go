package model

import (
	"github.com/go-redis/redis/v7"
	"log"
	"redisInAction/Chapter01/common"
	"strconv"
	"strings"
	"time"
)

type Article interface {
	ArticleVote(string, string)
	PostArticle(string, string, string) string
	GetArticles(int64, string) []map[string]string
	AddRemoveGroups(string, []string, []string)
	GetGroupArticles(string, string, int64) []map[string]string
	Reset()
}

type ArticleRepo struct {
	Conn *redis.Client
}

func NewArticleRepo(conn *redis.Client) *ArticleRepo {
	return &ArticleRepo{Conn: conn}
}

// ArticleVote 给文章投票
func (r *ArticleRepo) ArticleVote(article, user string) {
	// 文章发布一周之后，不再允许投票
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	if r.Conn.ZScore("time:", article).Val() < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	if r.Conn.SAdd("voted:"+articleId, user).Val() != 0 {
		r.Conn.ZIncrBy("score:", common.VoteScore, article) // 文章分数增加
		r.Conn.HIncrBy(article, "votes", 1)                 // 文章投票数增加
	}
}

// PostArticle 发布文章
func (r *ArticleRepo) PostArticle(user, title, link string) string {
	articleId := strconv.Itoa(int(r.Conn.Incr("article:").Val()))

	voted := "voted:" + articleId
	r.Conn.SAdd(voted, user)                                  // 把发布者添加到已投票用户列表
	r.Conn.Expire(voted, common.OneWeekInSeconds*time.Second) // 设置过期时间为一周

	now := time.Now().Unix()
	article := "article:" + articleId
	r.Conn.HMSet(article, map[string]interface{}{
		"title":  title,
		"link":   link,
		"poster": user,
		"time":   now,
		"votes":  1,
	})

	r.Conn.ZAdd("score:", &redis.Z{Score: float64(now + common.VoteScore), Member: article}) // 初始化文章分数为时间+投票分数
	r.Conn.ZAdd("time:", &redis.Z{Score: float64(now), Member: article})                     // 初始化文章发布时间
	return articleId
}

// GetArticles 获取文章列表
func (r *ArticleRepo) GetArticles(page int64, order string) []map[string]string {
	// 默认按照分数排序
	if order == "" {
		order = "score:"
	}
	start := (page - 1) * common.ArticlesPerPage
	end := start + common.ArticlesPerPage - 1

	ids := r.Conn.ZRevRange(order, start, end).Val()
	var articles []map[string]string
	for _, id := range ids {
		articleData := r.Conn.HGetAll(id).Val()
		articleData["id"] = id
		articles = append(articles, articleData)
	}
	return articles
}

// AddRemoveGroups 添加或删除文章所属的组
func (r *ArticleRepo) AddRemoveGroups(articleId string, toAdd, toRemove []string) {
	article := "article:" + articleId
	for _, group := range toAdd {
		r.Conn.SAdd("group:"+group, article)
	}
	for _, group := range toRemove {
		r.Conn.SRem("group:"+group, article)
	}
}

// GetGroupArticles 获取组内文章列表
func (r *ArticleRepo) GetGroupArticles(group, order string, page int64) []map[string]string {
	if order == "" {
		order = "score:"
	}
	key := order + group
	if r.Conn.Exists(key).Val() == 0 {
		// ZInterStore 将多个有序集合的交集存储在新的有序集合中
		res := r.Conn.ZInterStore(key, &redis.ZStore{Aggregate: "MAX", Keys: []string{"group:" + group, order}}).Val()
		if res <= 0 {
			log.Println("ZInterStore return 0")
		}
		r.Conn.Expire(key, 60*time.Second)
	}
	return r.GetArticles(page, key)
}

// Reset 重置数据库
func (r *ArticleRepo) Reset() {
	r.Conn.FlushDB()
}

// ArticleDisVote 给文章投反对票
func (r *ArticleRepo) ArticleDisVote(article, user string) {
	// 文章发布一周之后，不再允许投票
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	if r.Conn.ZScore("time:", article).Val() < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	if r.Conn.SAdd("disvoted:"+articleId, user).Val() != 0 {
		r.Conn.ZIncrBy("score:", common.DisVoteScore, article) // 文章分数减少
		r.Conn.HIncrBy(article, "disvotes", 1)                 // 文章反对票数增加
	}
}

// ExchangeVote 对调投票和反对票
func (r *ArticleRepo) ExchangeVote(article, user string) {
	// 文章发布一周之后，不再允许投票
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	if r.Conn.ZScore("time:", article).Val() < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	if r.Conn.SIsMember("voted:"+articleId, user).Val() {
		r.Conn.SMove("voted:"+articleId, "disvoted:"+articleId, user)
		r.Conn.ZIncrBy("score:", common.DisVoteScore-common.VoteScore, article) // 文章分数减少
		r.Conn.HIncrBy(article, "disvotes", 1)                                  // 文章反对票数增加
		r.Conn.HIncrBy(article, "votes", -1)                                    // 文章投票数减少
	} else if r.Conn.SIsMember("disvoted:"+articleId, user).Val() {
		r.Conn.SMove("disvoted:"+articleId, "voted:"+articleId, user)
		r.Conn.ZIncrBy("score:", common.VoteScore-common.DisVoteScore, article) // 文章分数增加
		r.Conn.HIncrBy(article, "votes", 1)                                     // 文章投票数增加
		r.Conn.HIncrBy(article, "disvotes", -1)                                 // 文章反对票数减少
	}
}
