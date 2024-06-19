package handler

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
	"strings"
	"time"

	"example.com/internal/repository"
	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type handler struct {
	repo repository.Repository
	_    struct{}
}

type Handler interface {
	// Get list of downloads
	GetDownloadRequests(c fiber.Ctx) error
	// Command: download a file
	CreateDownloadRequest(c fiber.Ctx) error
	// User Registeration
	Register(c fiber.Ctx) error
	// User Login
	Login(c fiber.Ctx, jwtSecret string) error
}

func generateFileName(userID int64, link string) string {
	h := fnv.New32a()
	h.Write([]byte(link))
	fmt.Fprint(h, userID)
	return fmt.Sprintf("%d", h.Sum32())
}

func validateUserCredentials(c fiber.Ctx) (string, string, string, error) {
	var payload struct {
		Username string `json:"username" validate:"required"`
		Password string `json:"password" validate:"required"`
	}

	if err := json.Unmarshal(c.Body(), &payload); err != nil {
		return "", "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "could not parse request body"})
	}

	username := payload.Username
	if username == "" {
		return "", "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "username is required"})
	}

	password := payload.Password
	if username == "" {
		return "", "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "password is required"})
	}

	if len(password) < 8 {
		return "", "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "password must be at least 8 characters long"})
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		log.Println(err)
		return "", "", "", c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "something went wrong"})
	}

	return username, password, string(hashedPassword), nil
}

func AuthMiddleware(c fiber.Ctx, secretKey string) error {
	authHeader := c.Get("Authorization")
	if authHeader == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "missing authorization header"})
	}

	tokenString := strings.TrimPrefix(authHeader, "Bearer ")
	if tokenString == authHeader {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid authorization header format"})
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fiber.NewError(fiber.StatusUnauthorized, "unexpected signing method")
		}
		return []byte(secretKey), nil
	})

	if err != nil {
		log.Printf("invalid token: %v", err)
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid token"})
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		userID, ok := claims["user_id"].(float64)
		if !ok {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid token claims"})
		}

		c.Locals("userID", int64(userID))
		return c.Next()
	}

	return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid token"})
}

func (h *handler) GetDownloadRequests(c fiber.Ctx) error {
	userID := c.Locals("userID").(int64)

	page, err := strconv.Atoi(c.Params("page"))
	if err != nil {
		page = 0
	}
	limit, err := strconv.Atoi(c.Params("limit"))
	if err != nil {
		limit = 0
	}

	downloads, err := h.repo.GetDownloadRequests(c.Context(), userID, int64(page), int64(limit))
	if err != nil {
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "something went wrong"})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{"downloads": downloads})
}

func (h *handler) CreateDownloadRequest(c fiber.Ctx) error {
	userID := c.Locals("userID").(int64)

	var payload struct {
		Link string `json:"link" validate:"required"`
	}

	if err := json.Unmarshal(c.Body(), &payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "could not parse request body"})
	}

	link := payload.Link
	if link == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "link is required"})
	}

	fileName := generateFileName(userID, link)
	downloadID, err := h.repo.CreateDownloadRequest(c.Context(), userID, link, fileName)
	if err != nil {
		// TODO handle duplicate link per user error separatly
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "something went wrong"})
	}

	// Important: Even if this push fails, the background job pushes again later.
	err = h.repo.PushDownloadRequest(c.Context(), downloadID)
	if err != nil {
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "something went wrong"})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{"message": "done"})
}

func (h *handler) Register(c fiber.Ctx) error {
	username, _, hashedPassword, err := validateUserCredentials(c)
	if err != nil {
		return err
	}

	userID, err := h.repo.CreateUser(c.Context(), username, hashedPassword)
	if err != nil {
		// TODO handle duplicate user
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "something went wrong"})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{"user_id": userID})
}

func (h *handler) Login(c fiber.Ctx, jwtSecret string) error {
	username, password, _, err := validateUserCredentials(c)
	if err != nil {
		return err
	}

	userID, err := h.repo.AuthUser(c.Context(), username, password)
	if err != nil {
		// TODO better error handling for user does not exist
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "something went wrong"})
	}
	if userID == 0 {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid username or password"})
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(time.Hour * 72).Unix(),
	})

	tokenString, err := token.SignedString([]byte(jwtSecret))
	if err != nil {
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not create token"})
	}

	return c.JSON(fiber.Map{"token": tokenString})
}

func New(repo repository.Repository) Handler {
	return &handler{
		repo: repo,
	}
}
