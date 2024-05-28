package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	redisClient *redis.Client
)

func Init(ctx context.Context) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		fmt.Println("invalid REDIS_ADDR")
		os.Exit(1)
	}

	redisDBStr := os.Getenv("REDIS_DB")
	redisDB, err := strconv.Atoi(redisDBStr)
	if err != nil {
		fmt.Println("invalid REDIS_DB")
		os.Exit(1)
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   redisDB,
	})
}

func Set(ctx context.Context, key string, value interface{}) bool {
	statusCmd := redisClient.Set(ctx, key, value, 0)
	if statusCmd.Err() != nil {
		fmt.Println("Error to set value in redis: ", statusCmd.Err())
		return false
	}
	return true
}

func Get(ctx context.Context, key string) ([]bson.M, error) {
	var emptyRet []bson.M
	redisData, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		return emptyRet, nil
	}
	var highlights []bson.M
	err = json.Unmarshal([]byte(redisData), &highlights)
	if err != nil {
		return emptyRet, nil
	}
	return highlights, nil
}

func Publish(ctx context.Context, key string) bool {
	intCmd := redisClient.Publish(ctx, key, "OK")
	if intCmd.Err() != nil {
		fmt.Println("Error to set value in redis: ", intCmd.Err())
		return false
	}
	return true
}
