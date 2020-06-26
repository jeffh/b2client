package b2

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type RetryClient struct {
	KeyID, AppKey string

	C  Client
	RC RetryConfig
}

func (c *RetryClient) isTimeoutAndThenWait(err error, attempts uint32) (timedOut, tooManyAttempts bool) {
	if IsTimeoutErr(err) {
		goto retry
	}
	if err, ok := err.(*ErrorResponse); ok && err.IsForbidden() {
		goto retry
	}
	return false, false
retry:
	if attempts < c.RC.getMaxAttempts() {
		if err, ok := err.(*ErrorResponse); ok && err.RetryAfter > 0 {
			time.Sleep(err.RetryAfter)
		} else {
			time.Sleep(ExpBackoff(attempts, c.RC.getJitter(), c.RC.getMin(), c.RC.Max, c.RC.getUnit()))
		}
		return true, false
	}
	return true, true
}

func (c *RetryClient) InvalidateAuthorization() { c.C.InvalidateAuthorization() }
func (c *RetryClient) AuthorizeIfNeeded() (*AuthorizeAccountResponse, error) {
	auth := c.C.LastAuth()
	if auth != nil {
		return auth, nil
	}

	retries := uint32(0)
	for {
		res, err := c.C.Authorize(c.KeyID, c.AppKey)
		if err != nil {
			timedOut, tooManyAttempts := c.isTimeoutAndThenWait(err, retries)
			if timedOut {
				if tooManyAttempts {
					return nil, fmt.Errorf("Error while authorizing (exceeded %d attempts): %w", c.RC.getMaxAttempts(), err)
				} else {
					retries++
					continue
				}
			}
			return nil, err
		}
		return &res, err
	}
}

func (c *RetryClient) genericRetryHandler(f func() error) error {
	retries := uint32(0)
	for {
		_, err := c.AuthorizeIfNeeded()
		if err != nil {
			return err
		}

		err = f()
		if err != nil {
			timedOut, tooManyAttempts := c.isTimeoutAndThenWait(err, retries)
			if timedOut {
				if tooManyAttempts {
					return fmt.Errorf("Error too many attempts (%d): %w", c.RC.getMaxAttempts(), err)
				} else {
					retries++
					continue
				}
			}
			if err, ok := err.(*ErrorResponse); ok && (err.IsForbidden() || (err.IsUnauthorized() && err.Code == ErrCodeExpiredAuthToken)) {
				if err.RetryAfter > 0 {
					time.Sleep(err.RetryAfter)
				} else {
					time.Sleep(ExpBackoff(retries, c.RC.getJitter(), c.RC.getMin(), c.RC.Max, c.RC.getUnit()))
				}
				retries++
				c.InvalidateAuthorization()
				continue
			}
			return err
		}
		return err
	}
	return nil
}

func (c *RetryClient) CancelLargeFile(fileId string) (res CancelLargeFileResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.CancelLargeFile(fileId)
		return err
	})
	return res, err
}

func (c *RetryClient) CopyFile(opt CopyFileOptions) (res CopyFileResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.CopyFile(opt)
		return err
	})
	return res, err
}

func (c *RetryClient) CopyPart(opt CopyPartOptions) (res CopyPartResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.CopyPart(opt)
		return err
	})
	return res, err
}

func (c *RetryClient) CreateBucket(bucketName string, bt BucketType, opt *CreateBucketOptions) (res BucketResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.CreateBucket(bucketName, bt, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) CreateKey(opt CreateKeyOptions) (res KeyResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.CreateKey(opt)
		return err
	})
	return res, err
}
func (c *RetryClient) DeleteBucket(bucketId string) (res BucketResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.DeleteBucket(bucketId)
		return err
	})
	return res, err
}
func (c *RetryClient) DeleteFileVersion(fileId, fileName string) (res DeleteFileResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.DeleteFileVersion(fileId, fileName)
		return err
	})
	return res, err
}
func (c *RetryClient) DeleteKey(appKeyId string) (res KeyResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.DeleteKey(appKeyId)
		return err
	})
	return res, err
}
func (c *RetryClient) DownloadFileById(fileId string, opt DownloadFileOptions) (res *http.Response, err error) {
	err = c.genericRetryHandler(func() error {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		res, err = c.C.DownloadFileById(fileId, opt)
		return err
	})
	return res, err
}
func (c *RetryClient) DownloadFileByName(bucketName, fileName string, opt DownloadFileOptions) (res *http.Response, err error) {
	err = c.genericRetryHandler(func() error {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		res, err = c.C.DownloadFileByName(bucketName, fileName, opt)
		return err
	})
	return res, err
}
func (c *RetryClient) FinishLargeFile(fileId string, partSha1s []string) (res FinishLargeFileResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.FinishLargeFile(fileId, partSha1s)
		return err
	})
	return res, err
}
func (c *RetryClient) GetDownloadAuthorization(opt GetDownloadAuthorizationOptions) (res GetDownloadAuthorizationResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.GetDownloadAuthorization(opt)
		return err
	})
	return res, err
}
func (c *RetryClient) GetFileInfo(fileId string) (res GetFileInfoResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.GetFileInfo(fileId)
		return err
	})
	return res, err
}

func (c *RetryClient) HideFile(bucketId, fileName string) (res HideFileResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.HideFile(bucketId, fileName)
		return err
	})
	return res, err
}

func (c *RetryClient) ListBuckets(opt *ListBucketsOptions) (res ListBucketsResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.ListBuckets(opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListFileNames(bucketId string, opt *ListFileNamesOptions) (res ListFileNamesResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.ListFileNames(bucketId, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListFileVersions(bucketId string, opt ListFileVersionsOptions) (res ListFileVersionsResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.ListFileVersions(bucketId, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListKeys(opt ListKeysOptions) (res ListKeysResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.ListKeys(opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListParts(fileId string, opt ListPartsOptions) (res ListPartsResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.ListParts(fileId, opt)
		return err
	})
	return res, err
}
func (c *RetryClient) ListUnfinishedLargeFiles(bucketId string, opt ListUnfinishedLargeFilesOptions) (res ListUnfinishedLargeFilesResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.ListUnfinishedLargeFiles(bucketId, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) StartLargeFile(bucketId, fileName, contentType string, fileInfo *FileInfo) (res StartLargeFileResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.StartLargeFile(bucketId, fileName, contentType, fileInfo)
		return err
	})
	return res, err
}

func (c *RetryClient) UpdateBucket(bucketId string, opt UpdateBucketOptions) (res UpdateBucketResponse, err error) {
	err = c.genericRetryHandler(func() error {
		res, err = c.C.UpdateBucket(bucketId, opt)
		return err
	})
	return res, err
}

// UploadFile uploads a file to a given bucket at a location.
// Will automatically Authorize, GetUploadURL, and start UploadFile -- with retries as per B2's integration guide.
func (c *RetryClient) UploadFile(bucketId string, opt UploadFileOptions) (UploadFileResponse, error) {
	retries := uint32(0)
	var uploadUrlRes GetUploadURLResponse
	for {
		_, err := c.AuthorizeIfNeeded()
		if err != nil {
			return UploadFileResponse{}, err
		}

		for {
			var err error
			uploadUrlRes, err = c.C.GetUploadURL(bucketId)
			if err != nil {
				timedOut, tooManyAttempts := c.isTimeoutAndThenWait(err, retries)
				if timedOut {
					if tooManyAttempts {
						return UploadFileResponse{}, fmt.Errorf("Error while requesting upload url (exceeded %d attempts): %w", c.RC.getMaxAttempts(), err)
					} else {
						retries++
						continue
					}
				}
				return UploadFileResponse{}, fmt.Errorf("Error while requesting upload url: %w", err)
			}
			break
		}

		res, err := c.C.UploadFile(uploadUrlRes.UploadURL, uploadUrlRes.AuthorizationToken, opt)
		if err != nil {
			if IsTimeoutErr(err) {
				goto prepRetry
			}
			/*
				These indicate that you should get a new upload URL and try again:

				- Unable to make an HTTP connection, including connection timeout.
				- Status of 401 Unauthorized, and an error code of expired_auth_token
				- Status of 408 Request Timeout (jeff: covered by above)
				- Any HTTP status in the 5xx range, including 503 Service Unavailable
				- "Broken pipe" sending the contents of the file.
				- A timeout waiting for a response (socket timeout). (jeff: covered from above)
			*/
			if err, ok := err.(*ErrorResponse); ok {
				if err.IsUnauthorized() && err.Code == ErrCodeExpiredAuthToken {
					goto prepRetry
				}
				if err.Status >= 500 && err.Status <= 599 {
					goto prepRetry
				}
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				goto prepRetry
			}
			return UploadFileResponse{}, fmt.Errorf("Error while uploading file: %w", err)
		prepRetry:
			retries++
			if err, ok := err.(*ErrorResponse); ok && err.RetryAfter > 0 {
				time.Sleep(err.RetryAfter)
			} else {
				time.Sleep(ExpBackoff(retries, c.RC.getJitter(), c.RC.getMin(), c.RC.Max, c.RC.getUnit()))
			}
			continue
		}
		return res, err
	}
}
