package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"log"
	"redisInAction/Chapter03/common"
	"redisInAction/utils"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}
func (r *Client) Reset() {
	r.Conn.FlushDB()
}

func (r *Client) NoTrans() {
	fmt.Println(r.Conn.Incr("notrans: ").Val())
	time.Sleep(100 * time.Millisecond)
	fmt.Println(r.Conn.Decr("notrans: ").Val())
}

func (r *Client) Trans() {
	pipeline := r.Conn.Pipeline()
	pipeline.Incr("trans:")
	time.Sleep(100 * time.Millisecond)
	pipeline.Decr("trans:")
	_, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline failed, the err is: ", err)
	}
}

// Publisher 发布订阅
func (r *Client) Publisher(n int) {
	// 为了让订阅者有足够的时间订阅频道，这里等待一秒钟
	time.Sleep(1 * time.Second)
	for n > 0 {
		r.Conn.Publish("channel", n)
		// 发布消息后进行短暂的休眠，让消息可以一条接一条地出现
		n--
	}
}

// RunPubsub 订阅
func (r *Client) RunPubsub() {
	pubsub := r.Conn.Subscribe("channel")
	defer pubsub.Close()

	var count int32 = 0
	for item := range pubsub.Channel() {
		fmt.Println(item.String())
		atomic.AddInt32(&count, 1)
		fmt.Println(count)

		switch count {
		case 4:
			if err := pubsub.Unsubscribe("channel"); err != nil {
				log.Println("unsubscribe fail, err: ", err)
			} else {
				fmt.Println("unsubscribe success")
			}
		case 5:
			break
		default:
		}
	}
}

func (r *Client) UpdateToken(token, user, item string) {
	timestamp := time.Now().Unix()
	r.Conn.HSet("login:", token, user)
	r.Conn.ZAdd("recent", &redis.Z{Score: float64(timestamp), Member: token})
	if item != "" {
		key := "viewed" + token
		r.Conn.LRem(key, 1, item)
		r.Conn.RPush(key, item)
		r.Conn.LTrim(key, -25, -1)
		r.Conn.ZIncrBy("viewed:", -1, item)
	}
}

// ArticleVote 投票，事务
func (r *Client) ArticleVote(article, user string) {
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	posted := r.Conn.ZScore("time", article).Val()
	if posted < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	// 事务
	pipeline := r.Conn.Pipeline()
	pipeline.SAdd("voted:"+articleId, user)
	pipeline.Expire("voted:"+articleId, time.Duration(int(posted-float64(cutoff)))*time.Second)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline failed, the err is: ", err)
	}
	if res[0] != nil {
		pipeline.ZIncrBy("score:", common.VoteScore, article)
		r.Conn.HIncrBy(article, "votes", 1)
		if _, err := pipeline.Exec(); err != nil {
			log.Println("pipeline failed, the err is: ", err)
		}
	}
}

// PostArticle 发布文章
func (r *Client) PostArticle(user, title, link string) string {
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

// GetArticles 通过事务获取一页文章
func (r *Client) GetArticles(page int64, order string) []map[string]string {
	if order == "" {
		order = "score:"
	}
	start := utils.Max(page-1, 0) * common.ArticlesPerPage
	end := start + common.ArticlesPerPage - 1

	ids := r.Conn.ZRevRange(order, start, end).Val()
	pipeline := r.Conn.Pipeline()
	for _, id := range ids {
		pipeline.HGetAll(id)
	}
	cmders, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline failed, the err is: ", err)
	}
	var articles []map[string]string
	for _, cmder := range cmders {
		articleData, _ := cmder.(*redis.StringStringMapCmd).Result()
		articleData["id"] = cmder.Args()[1].(string)
		articles = append(articles, articleData)
	}
	return articles
}

func (r *Client) CheckToken(token string) string {
	return r.Conn.Get("login:" + token).String()
}

func (r *Client) AddToCart(session, item string, count int) {
	switch {
	case count <= 0:
		r.Conn.HDel("cart:"+session, item)
	default:
		r.Conn.HSet("cart:"+session, item, count)
	}
	r.Conn.Expire("cart:"+session, common.THIRTYDAYS)
}

func (r *Client) UpdateTokenCh3(token, user, item string) {
	r.Conn.Set("login:"+token, user, common.THIRTYDAYS)
	key := "viewed:" + token
	if item != "" {
		r.Conn.LRem(key, 1, item)
		r.Conn.RPush(key, item)
		r.Conn.LTrim(key, -25, -1)
		r.Conn.ZIncrBy("viewed:", -1, item)
	}
	r.Conn.Expire(key, common.THIRTYDAYS)
}
