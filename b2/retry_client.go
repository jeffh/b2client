package b2

import (
	"context"
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

func (c *RetryClient) isTimeoutAndThenWait(ctx context.Context, err error, attempts uint32) (timedOut, tooManyAttempts bool) {
	select {
	case <-ctx.Done():
		res := ctx.Err() != nil
		return res, res
	default:
	}
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

// InvalidateAuthorization clears authorization tokens stored internally,
// requiring a reauth.
func (c *RetryClient) InvalidateAuthorization() { c.C.InvalidateAuthorization() }

// AuthorizeIfNeeded attempts to authorize using the RetryClient's KeyID and
// AppKey if an authorization token is missing.
func (c *RetryClient) AuthorizeIfNeeded(ctx context.Context) (*AuthorizeAccountResponse, error) {
	auth := c.C.LastAuth()
	if auth != nil {
		return auth, nil
	}

	retries := uint32(0)
	for {
		res, err := c.C.Authorize(ctx, c.KeyID, c.AppKey)
		if err != nil {
			timedOut, tooManyAttempts := c.isTimeoutAndThenWait(ctx, err, retries)
			if timedOut {
				if tooManyAttempts {
					select {
					case <-ctx.Done():
						if err := ctx.Err(); err != nil {
							return nil, fmt.Errorf("Error while authorizing (context error): %w", err)
						}
					default:
					}
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

func (c *RetryClient) genericRetryHandler(ctx context.Context, f func(context.Context) error) error {
	retries := uint32(0)
	for {
		_, err := c.AuthorizeIfNeeded(ctx)
		if err != nil {
			return err
		}

		err = f(ctx)
		if err != nil {
			timedOut, tooManyAttempts := c.isTimeoutAndThenWait(ctx, err, retries)
			if timedOut {
				if tooManyAttempts {
					select {
					case <-ctx.Done():
						if err := ctx.Err(); err != nil {
							return fmt.Errorf("Context error: %w", err)
						}
					default:
					}
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

// CancelLargeFile cancels an inprogress file upload. Authorizes as needed.
func (c *RetryClient) CancelLargeFile(ctx context.Context, fileId string) (res CancelLargeFileResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.CancelLargeFile(ctx, fileId)
		return err
	})
	return res, err
}

// CopyFile copies a file in the bucket to another location. Authorizes as
// needed.
func (c *RetryClient) CopyFile(ctx context.Context, opt CopyFileOptions) (res CopyFileResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.CopyFile(ctx, opt)
		return err
	})
	return res, err
}

// CopyPart copies a part of a large file in the bucket to another location.
// Authorizes as needed.
func (c *RetryClient) CopyPart(ctx context.Context, opt CopyPartOptions) (res CopyPartResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.CopyPart(ctx, opt)
		return err
	})
	return res, err
}

// CreateBucket creates a new bucket in the given account. Authorizes as
// needed.
func (c *RetryClient) CreateBucket(ctx context.Context, bucketName string, bt BucketType, opt *CreateBucketOptions) (res BucketResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.CreateBucket(ctx, bucketName, bt, opt)
		return err
	})
	return res, err
}

// CreateKey creates a new API key with permissions. Authorizes as needed.
func (c *RetryClient) CreateKey(ctx context.Context, opt CreateKeyOptions) (res KeyResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.CreateKey(ctx, opt)
		return err
	})
	return res, err
}

// DeleteBucket deletes an existing bucket within an account. Authorizes as
// needed.
func (c *RetryClient) DeleteBucket(ctx context.Context, bucketId string) (res BucketResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.DeleteBucket(ctx, bucketId)
		return err
	})
	return res, err
}

// DeleteFileVersion deletes a version of a file. Authorizes as needed.
func (c *RetryClient) DeleteFileVersion(ctx context.Context, fileId, fileName string) (res DeleteFileResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.DeleteFileVersion(ctx, fileId, fileName)
		return err
	})
	return res, err
}

// DeleteKey deletes an API key. Authorizes as needed.
func (c *RetryClient) DeleteKey(ctx context.Context, appKeyId string) (res KeyResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.DeleteKey(ctx, appKeyId)
		return err
	})
	return res, err
}

// DownloadFileByID downloads a file using the authorization previously retrieved via Authorize.
// Requires readFiles capabilities. Authorizes as needed.
func (c *RetryClient) DownloadFileByID(ctx context.Context, fileId string, opt *DownloadFileOptions) (res *http.Response, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		res, err = c.C.DownloadFileByID(ctx, fileId, opt)
		return err
	})
	return res, err
}

// DownloadFileByName downloads a file using the authorization previously
// retrieved via Authorize. Requires readFiles capabilities. Authorizes as
// needed.
func (c *RetryClient) DownloadFileByName(ctx context.Context, bucketName, fileName string, opt DownloadFileOptions) (res *http.Response, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		res, err = c.C.DownloadFileByName(ctx, bucketName, fileName, opt)
		return err
	})
	return res, err
}

// FinishLargeFile combines all previously uploaded file parts into one large
// file. Authorizes as needed. If this call times out, use GetFileInfo to
// verify if the file has been merged.
func (c *RetryClient) FinishLargeFile(ctx context.Context, fileId string, partSha1s []string) (res FinishLargeFileResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.FinishLargeFile(ctx, fileId, partSha1s)
		return err
	})
	return res, err
}

// GetDownloadAuthorization Generates a temporary authorization token to
// download a file via DownloadFileByName. Authorizes as needed.
func (c *RetryClient) GetDownloadAuthorization(ctx context.Context, opt GetDownloadAuthorizationOptions) (res GetDownloadAuthorizationResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.GetDownloadAuthorization(ctx, opt)
		return err
	})
	return res, err
}

// GetFileInfo returns metadata about a file stored in B2. Authorizes as
// needed.
func (c *RetryClient) GetFileInfo(ctx context.Context, fileId string) (res GetFileInfoResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.GetFileInfo(ctx, fileId)
		return err
	})
	return res, err
}

func (c *RetryClient) HideFile(ctx context.Context, bucketId, fileName string) (res HideFileResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.HideFile(ctx, bucketId, fileName)
		return err
	})
	return res, err
}

func (c *RetryClient) ListBuckets(ctx context.Context, opt *ListBucketsOptions) (res ListBucketsResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.ListBuckets(ctx, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListFileNames(ctx context.Context, bucketId string, opt *ListFileNamesOptions) (res ListFileNamesResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.ListFileNames(ctx, bucketId, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListFileVersions(ctx context.Context, bucketId string, opt *ListFileVersionsOptions) (res ListFileVersionsResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.ListFileVersions(ctx, bucketId, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListKeys(ctx context.Context, opt ListKeysOptions) (res ListKeysResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.ListKeys(ctx, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) ListParts(ctx context.Context, fileId string, opt ListPartsOptions) (res ListPartsResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.ListParts(ctx, fileId, opt)
		return err
	})
	return res, err
}
func (c *RetryClient) ListUnfinishedLargeFiles(ctx context.Context, bucketId string, opt ListUnfinishedLargeFilesOptions) (res ListUnfinishedLargeFilesResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.ListUnfinishedLargeFiles(ctx, bucketId, opt)
		return err
	})
	return res, err
}

func (c *RetryClient) StartLargeFile(ctx context.Context, bucketId, fileName, contentType string, fileInfo *FileInfo) (res StartLargeFileResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.StartLargeFile(ctx, bucketId, fileName, contentType, fileInfo)
		return err
	})
	return res, err
}

func (c *RetryClient) UpdateBucket(ctx context.Context, bucketId string, opt UpdateBucketOptions) (res UpdateBucketResponse, err error) {
	err = c.genericRetryHandler(ctx, func(ctx context.Context) error {
		res, err = c.C.UpdateBucket(ctx, bucketId, opt)
		return err
	})
	return res, err
}

// UploadFile uploads a file to a given bucket at a location.
// Will automatically Authorize, GetUploadURL, and start UploadFile -- with retries as per B2's integration guide.
func (c *RetryClient) UploadFile(ctx context.Context, bucketId string, opt UploadFileOptions) (UploadFileResponse, error) {
	retries := uint32(0)
	var uploadUrlRes GetUploadURLResponse
	for {
		_, err := c.AuthorizeIfNeeded(ctx)
		if err != nil {
			return UploadFileResponse{}, err
		}

		for {
			var err error
			uploadUrlRes, err = c.C.GetUploadURL(ctx, bucketId)
			if err != nil {
				timedOut, tooManyAttempts := c.isTimeoutAndThenWait(ctx, err, retries)
				if timedOut {
					if tooManyAttempts {
						select {
						case <-ctx.Done():
							if err := ctx.Err(); err != nil {
								return UploadFileResponse{}, fmt.Errorf("Error while requesting upload url (context error): %w", err)
							}
						default:
						}
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

		res, err := c.C.UploadFile(ctx, uploadUrlRes.UploadURL, uploadUrlRes.AuthorizationToken, opt)
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
