package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"example.com/internal/consumer"
	"example.com/internal/handler"
	"example.com/internal/repository"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	rdb *redis.Client
	db  *pgx.Conn
	_   struct{}
}

func NewServer() *Server {
	redisDB, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid redis db: %v\n", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST"),
		Password: os.Getenv("REDIS_PASS"),
		DB:       redisDB,
	})

	// TODO ctx deadline
	_, err = rdb.Ping(context.Background()).Result()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to cache: %v\n", err)
		os.Exit(1)
	}
	log.Println("Cache connected.")

	// TODO ctx deadline
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	log.Println("Database connected.")

	return &Server{
		rdb: rdb,
		db:  conn,
	}
}

func main() {
	secretKey := os.Getenv("SECRET_KEY")
	if secretKey == "" {
		fmt.Fprintf(os.Stderr, "Invalid secret key\n")
	}

	server := NewServer()
	// TODO ctx deadline
	ctx := context.Background()
	defer server.db.Close(ctx)
	defer server.rdb.Close()

	repo := repository.New(server.db, server.rdb)
	h := handler.New(repo)
	app := fiber.New()

	authMiddleware := func(c fiber.Ctx) error {
		return handler.AuthMiddleware(c, secretKey)
	}

	app.Get("/downloads/", h.GetDownloadRequests, authMiddleware)
	app.Post("/downloads/", h.CreateDownloadRequest, authMiddleware)
	app.Post("/register/", h.Register)
	app.Post("/login/", func(c fiber.Ctx) error { return h.Login(c, secretKey) })

	consumer.Start(ctx, repo, 3)
	// repo.PushDownloadRequest(ctx, 12)

	log.Println("Serving ...")
	app.Listen(":8080")
}
