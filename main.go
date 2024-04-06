package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"net/http"
)

type User struct {
	Username string `json:"username" binding:"required"`
	Points   int    `json:"points" binding:"required"`
	Rank     int    `json:"rank"`
}

type Database struct {
	Client *redis.Client
}

type Leaderboard struct {
	Count int `json:"count"`
	Users []*User
}

var (
	ListenAddr     = "178.253.32.178:8080"
	RedisAddr      = "178.253.32.178:6379"
	ErrNil         = errors.New("no matching record found in redis database")
	Ctx            = context.TODO()
	leaderboardKey = "leaderboard"
)

func main() {

	database, err := NewDatabase(RedisAddr)
	if err != nil {
		fmt.Println("Failed to connect to redis %s", err.Error())
	}
	val, err := database.Client.Get(context.TODO(), "name2").Result()
	fmt.Println(val)

	router := initRouter(database)
	router.Run(ListenAddr)
}

func NewDatabase(adress string) (*Database, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     adress,
		Password: "135980Aa@",
		DB:       0,
	})

	if err := client.Ping(Ctx).Err(); err != nil {
		return nil, err
	}
	return &Database{
		Client: client,
	}, nil
}

func (db *Database) GetLeaderboard() (*Leaderboard, error) {
	scores := db.Client.ZRangeWithScores(Ctx, leaderboardKey, 0, -1)
	if scores == nil {
		return nil, ErrNil
	}
	count := len(scores.Val())
	users := make([]*User, count)
	for idx, member := range scores.Val() {
		users[idx] = &User{
			Username: member.Member.(string),
			Points:   int(member.Score),
			Rank:     idx,
		}
	}
	leaderboard := &Leaderboard{
		Count: count,
		Users: users,
	}
	return leaderboard, nil
}

func initRouter(database *Database) *gin.Engine {
	r := gin.Default()

	r.POST("/points", func(c *gin.Context) {
		var userJson User
		if err := c.ShouldBindJSON(&userJson); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		member := redis.Z{
			Score:  float64(userJson.Points),
			Member: userJson.Username,
		}
		pipe := database.Client.TxPipeline()
		pipe.ZAdd(Ctx, "leaderboard", member)
		rank := pipe.ZRank(Ctx, leaderboardKey, userJson.Username)
		_, err := pipe.Exec(Ctx)
		fmt.Println(rank.Val(), err)
		userJson.Rank = int(rank.Val())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
		c.JSON(http.StatusOK, gin.H{"user": userJson})
	})

	return r
}
