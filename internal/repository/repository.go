package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/context"
)

const DownloadRequestsKey = "download_requests"

var NoMoreDownloadRequestErr = errors.New("There is no more download request in queue")

type downloadRequest struct {
	ID        int64
	UserID    int64
	Link      string // remote link to download
	FileName  string // relative path (either stored in local disk or S3)
	Completed bool
	Error     string // any error happended during downloading from destination
}

type repository struct {
	db  *pgx.Conn
	rdb *redis.Client
	_   struct{}
}

type Repository interface {
	GetDownloadRequest(ctx context.Context, downloadID int64) (downloadRequest, error)
	GetDownloadRequests(ctx context.Context, userID int64, page int64, limit int64) ([]downloadRequest, error)
	CreateDownloadRequest(ctx context.Context, userID int64, link string, fileName string) (int64, error)
	CompleteDownloadRequest(ctx context.Context, downloadID int64) error
	MarkError(ctx context.Context, downloadID int64, err string) error
	CreateUser(ctx context.Context, username string, hashedPassword string) (int64, error)
	AuthUser(ctx context.Context, username string, hashedPassword string) (int64, error)
	PushDownloadRequest(ctx context.Context, downloadID int64) error
	PopDownloadRequest(ctx context.Context) (int64, error)
	AcquireLock(ctx context.Context, downloadID int64, expiration time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, downloadID int64) error
	ExtendLock(ctx context.Context, downloadID int64, expiration time.Duration) (bool, error)
}

func (r *repository) GetDownloadRequest(ctx context.Context, downloadID int64) (downloadRequest, error) {
	query := `SELECT id, user_id, link, file_name, completed, error FROM downloads WHERE id = $1`

	var req downloadRequest
	rows, err := r.db.Query(ctx, query, downloadID)
	if err != nil {
		return req, fmt.Errorf("could not retrieve download request %d: %v", downloadID, err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&req.ID, &req.UserID, &req.Link, &req.FileName, &req.Completed, &req.Error)
		if err != nil {
			return req, fmt.Errorf("could not scan download request %d: %v", downloadID, err)
		}
		return req, nil
	}

	return req, fmt.Errorf("download request %d not found", downloadID)
}

func (r *repository) GetDownloadRequests(ctx context.Context, userID int64, page int64, limit int64) ([]downloadRequest, error) {
	var downloadRequests []downloadRequest
	query := `SELECT id, user_id, link, file_name, completed, error FROM downloads OFFSET $1 LIMIT $2`

	rows, err := r.db.Query(ctx, query, page*limit, limit)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve download requests: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var req downloadRequest
		err := rows.Scan(&req.ID, &req.UserID, &req.Link, &req.FileName, &req.Completed, &req.Error)
		if err != nil {
			return nil, fmt.Errorf("could not scan download request: %v", err)
		}
		downloadRequests = append(downloadRequests, req)
	}

	return downloadRequests, nil
}

func (r *repository) CreateDownloadRequest(ctx context.Context, userID int64, link string, fileName string) (int64, error) {
	var downloadID int64
	query := `INSERT INTO downloads (user_id, link, file_name, completed, error) VALUES ($1, $2, $3, false, '') RETURNING id`
	err := r.db.QueryRow(ctx, query, userID, link, fileName).Scan(&downloadID)
	if err != nil {
		return 0, fmt.Errorf("could not create download request: user_id: %d, link: %s: %v", userID, link, err)
	}

	return downloadID, nil
}

func (r *repository) CompleteDownloadRequest(ctx context.Context, downloadID int64) error {
	_, err := r.db.Exec(ctx, `UPDATE downloads SET completed = TRUE WHERE id = $1`, downloadID)
	if err != nil {
		return fmt.Errorf("could not complete download request %d: %v", downloadID, err)
	}

	return nil
}

func (r *repository) MarkError(ctx context.Context, downloadID int64, downloadErr string) error {
	_, err := r.db.Exec(ctx, `UPDATE downloads SET error = $1 WHERE id = $2`, downloadErr, downloadID)
	if err != nil {
		return fmt.Errorf("could not update download request %d error: %v", downloadID, err)
	}

	return nil
}

func (r *repository) CreateUser(ctx context.Context, username string, hashedPassword string) (int64, error) {
	var userID int64
	query := `INSERT INTO users (username, password) VALUES ($1, $2) RETURNING id`
	err := r.db.QueryRow(ctx, query, username, hashedPassword).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("could not insert new user %s: %v", username, err)
	}

	return userID, nil
}

func (r *repository) AuthUser(ctx context.Context, username string, password string) (int64, error) {
	var retrievedUserID sql.NullInt64
	var retrievedHashedPassword sql.NullString
	err := r.db.QueryRow(ctx, `SELECT id, password FROM users WHERE username = $1`, username).Scan(&retrievedUserID, &retrievedHashedPassword)
	if err != nil || !retrievedHashedPassword.Valid {
		return 0, fmt.Errorf("could not authenticate user %s: %v", username, err)
	}

	err = bcrypt.CompareHashAndPassword([]byte(retrievedHashedPassword.String), []byte(password))
	if err != nil {
		return 0, nil
	}

	return retrievedUserID.Int64, nil
}

func (r *repository) PushDownloadRequest(ctx context.Context, downloadID int64) error {
	err := r.rdb.LPush(ctx, DownloadRequestsKey, downloadID).Err()
	if err != nil {
		return fmt.Errorf("could not push download request %d: %v", downloadID, err)
	}

	return nil
}

func (r *repository) PopDownloadRequest(ctx context.Context) (int64, error) {
	downloadIDStr, err := r.rdb.RPop(ctx, DownloadRequestsKey).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, NoMoreDownloadRequestErr
		}
		return 0, fmt.Errorf("could not pop download request: %v", err)
	}

	downloadID, _ := strconv.ParseInt(downloadIDStr, 10, 64)
	return downloadID, nil
}

func (r *repository) AcquireLock(ctx context.Context, downloadID int64, expiration time.Duration) (bool, error) {
	succeeded, err := r.rdb.SetNX(ctx, fmt.Sprint(downloadID), "locked", expiration).Result()
	if err != nil {
		return false, fmt.Errorf("Error acquiring lock: %v", err)
	}
	return succeeded, nil
}

func (r *repository) ExtendLock(ctx context.Context, downloadID int64, expiration time.Duration) (bool, error) {
	succeeded, err := r.rdb.Expire(ctx, fmt.Sprint(downloadID), expiration).Result()
	if err != nil {
		return false, fmt.Errorf("Error extending lock: %v", err)
	}
	return succeeded, nil
}

func (r *repository) ReleaseLock(ctx context.Context, downloadID int64) error {
	_, err := r.rdb.Del(ctx, fmt.Sprint(downloadID)).Result()
	if err != nil {
		return fmt.Errorf("Error releasing lock: %v", err)
	}

	return nil
}

func New(db *pgx.Conn, rdb *redis.Client) Repository {
	return &repository{
		db:  db,
		rdb: rdb,
	}
}
