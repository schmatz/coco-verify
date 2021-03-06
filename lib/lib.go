package lib

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo/bson"
	"time"
)

const DatabaseName string = "coco"
const CollectionName string = "level.sessions"
const MongoURL string = "localhost"
const MongoUsername string = "example_username"
const MongoPassword string = "example_password"
const RedisPassword string = "example_password"
const RedisHost string = "localhost:6379"
const UnprocessedSetName string = "unprocessed"
const ProcessedSetName string = "processed"
const ProcessingName string = "processing"

type GameSession struct {
	ID      bson.ObjectId `bson:"_id,omitempty"`
	Team    string
	Creator string `bson:"creator,omitempty"`
}

type GameSessionPair [2]GameSession

func (g GameSessionPair) RedisQueueKey() string {
	return g[0].ID.Hex() + g[1].ID.Hex()
}
func (g *GameSession) GetWinningRedisKey() string {
	return g.ID.Hex() + "w"
}
func (g *GameSession) GetLosingRedisKey() string {
	return g.ID.Hex() + "l"
}
func (g *GameSession) GetTieRedisKey() string {
	return g.ID.Hex() + "t"
}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", RedisHost)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", RedisPassword); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func ConnectToRedis() redis.Conn {
	redisConnection, err := redis.Dial("tcp", RedisHost)
	if err != nil {
		panic(err)
	}
	/*isAuth, err := redisConnection.Do("AUTH", RedisPassword)
	if isAuth != "OK" || err != nil {
		panic("Redis authentication failed!")
	}*/
	fmt.Println("Connected to Redis!")
	return redisConnection
}

func ConnectToRedisPooled() *redis.Pool {
	return newPool()
}
