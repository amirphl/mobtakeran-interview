package consumer

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"example.com/internal/repository"
)

const SleepDurationInCaseOFNoDownloadRequest = 1 * time.Second
const LinkProcessingExpTime = 60 * time.Second
const DownloadBuffSizeBytes = 131072                  // 128KB
const FlushThresholdBytes = 8 * DownloadBuffSizeBytes // 1MB

type worker struct {
	id   int
	repo repository.Repository
	_    struct{}
}

func Start(ctx context.Context, repo repository.Repository, numWorkers int) {
	workers := make([]worker, 0, numWorkers)

	for i := 0; i < numWorkers; i++ {
		w := worker{
			id:   i,
			repo: repo,
		}
		workers = append(workers, w)
		go w.run(ctx)
	}
}

func (w *worker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d is stopping\n", w.id)
			return
		default:
			downloadID, err := w.repo.PopDownloadRequest(ctx)
			if err != nil {
				if err == repository.NoMoreDownloadRequestErr {
					time.Sleep(SleepDurationInCaseOFNoDownloadRequest)
					continue
				}
				log.Printf("Worker %d: error reading from queue: %v", w.repo, err)
				continue
			}

			if err = w.processDownloadRequest(ctx, downloadID); err != nil {
				log.Printf("Worker %d: failed to process download request %d: %v", w.id, downloadID, err)
			}
		}
	}
}

func (w *worker) processDownloadRequest(ctx context.Context, downloadID int64) error {
	log.Printf("Worker %d: processing download request %d\n", w.id, downloadID)

	downloadRequest, err := w.repo.GetDownloadRequest(ctx, downloadID)
	if err != nil {
		return fmt.Errorf("Failed to retrieve download request %d: %v", downloadID, err)
	}
	log.Printf("Worker %d: download request %d: retrieved info from db\n", w.id, downloadID)

	acquired, err := w.repo.AcquireLock(ctx, downloadID, LinkProcessingExpTime)
	if err != nil {
		return fmt.Errorf("Failed to acquire lock: %v", err)
	}
	if !acquired {
		return fmt.Errorf("Download request %d is already being processed:", downloadID)
	}
	log.Printf("Worker %d: download request %d: acquired lock for %v duration\n", w.id, downloadID, LinkProcessingExpTime)

	defer w.repo.ReleaseLock(ctx, downloadID) // No need to handle the error since the lock will finally be released.

	file, offset, err := w.openFile(downloadRequest.FileName)
	if err != nil {
		dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
		if dbErr != nil {
			log.Println(dbErr)
		}
		return fmt.Errorf("Failed to open file for download request %d: %v", downloadID, err)
	}
	defer file.Close()
	log.Printf("Worker %d: download request %d: opened file: offset: %d\n", w.id, downloadID, offset)

	link := downloadRequest.Link
	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
		if dbErr != nil {
			log.Println(dbErr)
		}
		return fmt.Errorf("Failed to create HTTP request for link %s: %v", link, err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	// req.Header.Set("Accept-Encoding", "identity") // Disable compression
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

	client := &http.Client{} // TODO performance: Use http connection pool
	resp, err := client.Do(req)
	if err != nil {
		dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
		if dbErr != nil {
			log.Println(dbErr)
		}
		log.Printf("Failed to perform HTTP request for link %s: %v", link, err)
	}
	defer resp.Body.Close()
	log.Printf("Worker %d: download request %d: sent range request: offset: %d\n", w.id, downloadID, offset)

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Unexpected HTTP status code for link %s: %d", link, resp.StatusCode)
		dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
		if dbErr != nil {
			log.Println(dbErr)
		}
		return err
	}
	log.Printf("Worker %d: download request %d: received status code %d\n", w.id, downloadID, resp.StatusCode)

	buffer := make([]byte, DownloadBuffSizeBytes)
	bytesRead := int64(0)
	totalBytesRead := int64(0)
	ticker := time.NewTicker(LinkProcessingExpTime / 2)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				w.repo.ExtendLock(ctx, downloadID, LinkProcessingExpTime) // TODO handle succeeded, error
				log.Printf("Worker %d: download request %d: extended expiration time for %v duration\n", w.id, downloadID, LinkProcessingExpTime)
			case <-ctx.Done():
				// TODO What should I do here?
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			dbErr := w.repo.MarkError(ctx, downloadID, ctx.Err().Error())
			if dbErr != nil {
				log.Println(dbErr)
			}
			log.Printf("Worker %d:  download request %d: context terminated\n", w.id, downloadID)
			return ctx.Err()
		default:
			n, err := resp.Body.Read(buffer)
			if err == io.EOF {
				// TODO duplicate code

				if err := file.Sync(); err != nil {
					dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
					if dbErr != nil {
						log.Println(dbErr)
					}
					return fmt.Errorf("Error syncing file (for the last time) link %s: %v", link, err)
				}

				log.Printf("Worker %d: download request %d: flushed to disk: chunk %d: chuck size: %d bytes\n", w.id, downloadID, totalBytesRead/FlushThresholdBytes, FlushThresholdBytes)
				bytesRead = 0
				log.Printf("Worker %d:  download request %d: EOF\n", w.id, downloadID)
				err := w.repo.CompleteDownloadRequest(ctx, downloadID)
				if err != nil {
					log.Println(err)
					dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
					if dbErr != nil {
						log.Println(dbErr)
					}
					return err
				}
				log.Printf("Worker %d: download request %d: completed: received %d total bytes\n", w.id, downloadID, totalBytesRead)
				return nil
			}
			if err != nil {
				dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
				if dbErr != nil {
					log.Println(dbErr)
				}
				return fmt.Errorf("Error reading from HTTP response for link %s: %v", link, err)
			}

			if _, err := file.Write(buffer[:n]); err != nil {
				dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
				if dbErr != nil {
					log.Println(dbErr)
				}
				return fmt.Errorf("Error writing to file for link %s: %v", link, err)
			}
			// log.Printf("Worker %d: download request %d: wrote %d byte into mapped file\n", w.id, downloadID, n)

			bytesRead += int64(n)
			totalBytesRead += int64(n)
			if bytesRead >= FlushThresholdBytes {
				if err := file.Sync(); err != nil {
					dbErr := w.repo.MarkError(ctx, downloadID, err.Error())
					if dbErr != nil {
						log.Println(dbErr)
					}
					return fmt.Errorf("Error syncing file for link %s: %v", link, err)
				}
				log.Printf("Worker %d: download request %d: flushed to disk: chunk %d: chuck size: %d bytes\n", w.id, downloadID, totalBytesRead/FlushThresholdBytes, FlushThresholdBytes)
				bytesRead = 0
			}
		}
	}
}

func (w *worker) openFile(fileName string) (*os.File, int64, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	return file, info.Size(), nil
}
