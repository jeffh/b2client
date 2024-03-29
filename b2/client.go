package b2

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	debugRequests        = false
	debugResponses       = false
	testRetries          = false
	testExpireSomeTokens = false
	testCapExceeded      = false
)

// Logger is the interface for B2 Client Logging
type Logger interface {
	Printf(format string, values ...interface{})
}

// TempStorage is the interface to provide temporary storage for B2 Client to
// store objects during multipart uploads
type TempStorage interface {
	// Store returns the length of the given reader.
	// It is expected to return a new reader with the same contents, the length of the reader, and any io error
	// This is useful to transfer reader contents to some temporary storage to do this counting.
	// Closing the return reader indicates that the storage for that reader's
	// contents is no longer needed and can be cleaned up.
	Store(r io.Reader) (rc io.ReadCloser, size int64, err error)
}

// TempFileStorage implements a local-filesystem based TempStorage using the
// Operating System's temporary file storage.
type TempFileStorage struct {
	Dir     string
	Pattern string
}

var _ TempStorage = (*TempFileStorage)(nil)

func (fs *TempFileStorage) Store(r io.Reader) (io.ReadCloser, int64, error) {
	f, err := ioutil.TempFile(fs.Dir, fs.Pattern)
	if err != nil {
		return nil, 0, err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		return nil, 0, err
	}
	_, err = f.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, 0, err
	}

	return f, n, nil
}

// Client manages most of the low-level operations for the B2 API.
// Client is not thread safe.
// Most likely you're looking for RetryClient
type Client struct {
	UserAgent string      // UserAgent for us to B2 (Defaults to DefaultUserAgent())
	C         http.Client // Underlying HTTP Client
	L         Logger      // nilable, optional logger
	TS        TempStorage // nilable, used for temp storage of uploads

	m        sync.Mutex
	lastAuth *AuthorizeAccountResponse // last successful auth response
}

func (c *Client) InvalidateAuthorization() {
	c.m.Lock()
	defer c.m.Unlock()
	c.lastAuth = nil
}

func (c *Client) LastAuth() *AuthorizeAccountResponse {
	c.m.Lock()
	defer c.m.Unlock()
	if c.lastAuth != nil {
		auth := *c.lastAuth
		return &auth
	}
	return nil
}

func (c *Client) logf(format string, values ...interface{}) {
	if c.L != nil {
		c.L.Printf(format, values...)
	}
}

func (c *Client) getUserAgent() string {
	if c.UserAgent == "" {
		c.UserAgent = DefaultUserAgent()
	}
	return c.UserAgent
}

func (c *Client) request(ctx context.Context, baseURL, method, endpoint string, body interface{}) (*http.Request, error) {
	if baseURL == "" {
		baseURL = "https://api.backblazeb2.com"
	}
	var req *http.Request
	var err error
	if body == nil {
		req, err = http.NewRequestWithContext(ctx, method, baseURL+endpoint, nil)
	} else {
		buf := &bytes.Buffer{}
		e := json.NewEncoder(buf)
		if err := e.Encode(body); err != nil {
			return nil, err
		}
		if debugRequests {
			c.logf("request-body: %s", buf.String())
		}
		req, err = http.NewRequest(method, baseURL+endpoint, buf)
	}
	if req != nil {
		req.Header.Set("User-Agent", c.getUserAgent())
		if testRetries {
			req.Header.Set("X-Bz-Test-Mode", "fail_some_uploads")
		}
		if testExpireSomeTokens {
			req.Header.Set("X-Bz-Test-Mode", "expire_some_account_authorization_tokens")
		}
		if testCapExceeded {
			req.Header.Set("X-Bz-Test-Mode", "force_cap_exceeded")
		}
	}
	return req, err
}

func (c *Client) authRequest(ctx context.Context, method, endpoint string, body interface{}) (*http.Request, error) {
	auth := c.LastAuth()
	if auth == nil {
		return nil, ErrAuthTokenMissing
	}

	req, err := c.request(ctx, auth.APIURL, method, endpoint, body)
	if err != nil {
		return req, err
	}
	req.Header.Add("Authorization", auth.AuthorizationToken)
	return req, err
}

func (c *Client) uploadRequest(ctx context.Context, uploadURL, uploadAuthToken string) (*http.Request, error) {
	req, err := c.request(ctx, uploadURL, "POST", "", nil)
	if err != nil {
		return req, err
	}
	req.Header.Add("Authorization", uploadAuthToken)
	return req, err
}

func (c *Client) downloadRequest(ctx context.Context, method, endpoint string, body interface{}) (*http.Request, error) {
	auth := c.LastAuth()
	if auth == nil {
		return nil, ErrAuthTokenMissing
	}

	req, err := c.request(ctx, auth.DownloadURL, method, endpoint, body)
	if err != nil {
		return req, err
	}
	req.Header.Add("Authorization", auth.AuthorizationToken)
	return req, err
}

func (c *Client) do(req *http.Request, out interface{}) error {
	start := time.Now()
	c.logf("http=request method=%s url=%s raw=false time=%s", req.Method, req.URL.String(), logStrTime(start))
	if debugRequests {
		c.logf("request-headers: %#v", req.Header)
	}
	res, err := c.C.Do(req)
	if err != nil {
		end := time.Now()
		c.logf("http=response method=%s url=%s ok=false raw=false time=%s duration=%s err_type=network err=%#v", req.Method, req.URL.String(), logStrTime(end), end.Sub(start).String(), err.Error())
		return err
	}
	defer res.Body.Close()

	d := json.NewDecoder(res.Body)
	if res.StatusCode == 200 {
		err := d.Decode(out)
		if err != nil {
			end := time.Now()
			c.logf("http=response method=%s url=%s ok=false raw=false status=%d time=%s duration=%s err_type=json-decode err=%#v", req.Method, req.URL.String(), res.StatusCode, logStrTime(end), end.Sub(start).String(), err.Error())
			return fmt.Errorf("Failed to parse JSON from response: %w", err)
		}
	} else {
		resErr := &ErrorResponse{}
		err := d.Decode(&resErr)
		if err != nil {
			end := time.Now()
			c.logf("http=response method=%s url=%s ok=false raw=false status=%d time=%s duration=%s err_type=json-decode err=%#v", req.Method, req.URL.String(), res.StatusCode, logStrTime(end), end.Sub(start).String(), err.Error())
			return fmt.Errorf("Failed to parse JSON from response: %w", err)
		}
		seconds, err := strconv.Atoi(res.Header.Get("Retry-After"))
		if err == nil {
			resErr.RetryAfter = time.Duration(seconds) * time.Second
		}
		end := time.Now()
		c.logf("http=response method=%s url=%s ok=false raw=false status=%d time=%s duration=%s err_type=api-error err=%#v", req.Method, req.URL.String(), res.StatusCode, logStrTime(end), end.Sub(start).String(), resErr.Error())
		if debugResponses {
			c.logf("response-body: %#v", resErr)
		}
		return resErr
	}
	end := time.Now()
	c.logf("http=response method=%s url=%s ok=true raw=false status=%d time=%s duration=%s", req.Method, req.URL.String(), res.StatusCode, logStrTime(end), end.Sub(start).String())
	if debugResponses {
		c.logf("response-body: %#v", out)
	}
	return nil
}

func (c *Client) doRaw(req *http.Request) (*http.Response, error) {
	start := time.Now()
	c.logf("http=request method=%s url=%s raw=true time=%s", req.Method, req.URL.String(), logStrTime(start))
	res, err := c.C.Do(req)
	if err != nil {
		end := time.Now()
		c.logf("http=response method=%s url=%s ok=false raw=true time=%s duration=%s err_type=network err=%#v", req.Method, req.URL.String(), logStrTime(end), end.Sub(start).String(), err.Error())
		return res, err
	}

	if res.StatusCode != 200 {
		d := json.NewDecoder(res.Body)
		resErr := &ErrorResponse{}
		err := d.Decode(&resErr)
		if err != nil {
			end := time.Now()
			c.logf("http=response method=%s url=%s ok=false raw=true status=%d time=%s duration=%s err_type=json-decode err=%#v", req.Method, req.URL.String(), res.StatusCode, logStrTime(end), end.Sub(start).String(), err.Error())
			return res, fmt.Errorf("Failed to parse JSON from response: %w", err)
		}
		end := time.Now()
		c.logf("http=response method=%s url=%s ok=false raw=true status=%d time=%s duration=%s err_type=api-error err=%#v", req.Method, req.URL.String(), res.StatusCode, logStrTime(end), end.Sub(start).String(), resErr.Error())
		return res, resErr
	}
	end := time.Now()
	c.logf("http=response method=%s url=%s ok=true raw=true status=%d time=%s duration=%s", req.Method, req.URL.String(), res.StatusCode, logStrTime(end), end.Sub(start).String())
	return res, nil
}

// Authorize exchanges a keyId and appKey for an authorization token. Auth
// tokens can be used for other API calls. Stores authorization for future API
// calls.
func (c *Client) Authorize(ctx context.Context, keyId, appKey string) (AuthorizeAccountResponse, error) {
	req, err := c.request(ctx, "", "GET", "/b2api/v2/b2_authorize_account", nil)
	if err != nil {
		return AuthorizeAccountResponse{}, err
	}
	req.SetBasicAuth(keyId, appKey)
	var r AuthorizeAccountResponse
	err = c.do(req, &r)
	if err == nil {
		c.m.Lock()
		c.lastAuth = &r
		c.m.Unlock()
	}
	return r, err
}

// CancelLargeFile cancels an inprogress file upload. Requires Authorize to be called first.
func (c *Client) CancelLargeFile(ctx context.Context, fileId string) (CancelLargeFileResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_cancel_large_file", &requestByFileID{fileId})
	if err != nil {
		return CancelLargeFileResponse{}, err
	}

	var r CancelLargeFileResponse
	err = c.do(req, &r)
	return r, err
}

type MetadataDirective string

const (
	MetadataDirectiveNone    MetadataDirective = ""
	MetadataDirectiveCopy                      = "COPY"
	MetadataDirectiveReplace                   = "REPLACE"
)

type CopyFileOptions struct {
	SourceFileId        string            `json:"sourceFileId"` // required
	FileName            string            `json:"fileName"`     // required
	DestinationBucketId string            `json:"destinationBucketId,omitempty"`
	Range               string            `json:"range,omitempty"` // in form: "bytes=1000-2000"
	MetadataDirective   MetadataDirective `json:"metadataDirective,omitempty"`
	ContentType         string            `json:"contentType,omitempty"`
	FileInfo            FileInfo          `json:"fileInfo,omitempty"`
}

// CopyFile copies a file in the bucket to another location. Requires Authorize to be called first.
func (c *Client) CopyFile(ctx context.Context, opt CopyFileOptions) (CopyFileResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_copy_file", &opt)
	if err != nil {
		return CopyFileResponse{}, err
	}

	var r CopyFileResponse
	err = c.do(req, &r)
	return r, err
}

type CopyPartOptions struct {
	SourceFileId string `json:"sourceFileId"`    // required
	LargeFileId  string `json:"largeFileId"`     // required
	PartNumber   int    `json:"partNumber"`      // required
	Range        string `json:"range,omitempty"` // in form: "bytes=1000-2000"
}

// CopyPart copies a part of a large file in the bucket to another location.
// Requires Authorize to be called first.
func (c *Client) CopyPart(ctx context.Context, opt CopyPartOptions) (CopyPartResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_copy_part", &opt)
	if err != nil {
		return CopyPartResponse{}, err
	}

	var r CopyPartResponse
	err = c.do(req, &r)
	return r, err
}

type CreateBucketOptions struct {
	BucketInfo     BucketInfo      // optional
	CorsRules      []CorsRule      // optional
	LifecycleRules []LifecycleRule // optional
}

// CreateBucket creates a new bucket in the given account. Requires Authorize to be called first.
func (c *Client) CreateBucket(ctx context.Context, bucketName string, bt BucketType, opt *CreateBucketOptions) (BucketResponse, error) {
	type request struct {
		AccountId      string          `json:"accountId"`  // required
		BucketName     string          `json:"bucketName"` // required
		BucketType     BucketType      `json:"bucketType"` // required
		BucketInfo     BucketInfo      `json:"bucketInfo,omitempty"`
		CorsRules      []CorsRule      `json:"corsRules,omitempty"`
		LifecycleRules []LifecycleRule `json:"lifecycleRules,omitempty"`
	}
	var o CreateBucketOptions
	if opt != nil {
		o = *opt
	}
	auth := c.LastAuth()
	if auth == nil {
		return BucketResponse{}, ErrAuthTokenMissing
	}
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_create_bucket", &request{
		auth.AccountID,
		bucketName,
		bt,
		o.BucketInfo,
		o.CorsRules,
		o.LifecycleRules,
	})
	if err != nil {
		return BucketResponse{}, err
	}

	var r BucketResponse
	err = c.do(req, &r)
	return r, err
}

type CreateKeyOptions struct {
	AccountId              string   `json:"accountId"`    // required
	Capabilities           []string `json:"capabilities"` // required
	KeyName                string   `json:"keyName"`      // required
	ValidDurationInSeconds int      `json:"validDurationInSeconds,omitempty"`
	BucketId               string   `json:"bucketId,omitempty"`
	NamePrefix             string   `json:"namePrefix,omitempty"`
}

// CreateKey creates a new API key with permissions. Requires Authorize to be called first.
func (c *Client) CreateKey(ctx context.Context, opt CreateKeyOptions) (KeyResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_create_key", &opt)
	if err != nil {
		return KeyResponse{}, err
	}

	var r KeyResponse
	err = c.do(req, &r)
	return r, err
}

// DeleteBucket deletes an existing bucket within an account. Requires Authorize to be called first.
func (c *Client) DeleteBucket(ctx context.Context, bucketId string) (BucketResponse, error) {
	type request struct {
		AccountId string `json:"accountId"`
		BucketId  string `json:"bucketId"`
	}
	auth := c.LastAuth()
	if auth == nil {
		return BucketResponse{}, ErrAuthTokenMissing
	}
	accountId := auth.AccountID
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_delete_bucket", &request{accountId, bucketId})
	if err != nil {
		return BucketResponse{}, err
	}

	var r BucketResponse
	err = c.do(req, &r)
	return r, err
}

// DeleteFileVersion deletes a version of a file. Requires Authorize to be called first.
func (c *Client) DeleteFileVersion(ctx context.Context, fileId, fileName string) (DeleteFileResponse, error) {
	type request struct {
		FileId   string `json:"fileId"`
		FileName string `json:"fileName"`
	}
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_delete_file_version", &request{fileId, fileName})
	if err != nil {
		return DeleteFileResponse{}, err
	}

	var r DeleteFileResponse
	err = c.do(req, &r)
	return r, err
}

// DeleteKey deletes an API key. Requires Authorize to be called first
func (c *Client) DeleteKey(ctx context.Context, appKeyId string) (KeyResponse, error) {
	type request struct {
		AppKeyId string `json:"applicationKeyId"`
	}
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_delete_key", &request{appKeyId})
	if err != nil {
		return KeyResponse{}, err
	}

	var r KeyResponse
	err = c.do(req, &r)
	return r, err
}

type DownloadFileOptions struct {
	Range              string // optional
	ContentDisposition string // optional, overrides file specified value
	ContentLanguage    string // optional, overrides file specified value
	Expires            string // optional, RFC 2616, overrides file specified value
	CacheControl       string // optional, overrides file specified value
	ContentEncoding    string // optional, overrides file specified value
	ContentType        string // optional, overrides file specified value
}

func (opt DownloadFileOptions) setOnRequest(req *http.Request, fileId string) {
	q := req.URL.Query()
	if fileId != "" {
		q.Set("fileId", fileId)
	}

	if opt.ContentDisposition != "" {
		q.Set("b2ContentDisposition", opt.ContentDisposition)
	}
	if opt.ContentLanguage != "" {
		q.Set("b2ContentLanguage", opt.ContentLanguage)
	}
	if opt.Expires != "" {
		q.Set("b2Expires", opt.Expires)
	}
	if opt.CacheControl != "" {
		q.Set("b2CacheControl", opt.CacheControl)
	}
	if opt.ContentEncoding != "" {
		q.Set("b2ContentEncoding", opt.ContentEncoding)
	}
	if opt.ContentType != "" {
		q.Set("b2ContentType", opt.ContentType)
	}
	req.URL.RawQuery = q.Encode()
}

// DownloadFileByID downloads a file using the authorization previously retrieved via Authorize.
// Requires readFiles capabilities
func (c *Client) DownloadFileByID(ctx context.Context, fileId string, opt *DownloadFileOptions) (*http.Response, error) {
	req, err := c.downloadRequest(ctx, "GET", "/b2api/v2/b2_download_file_by_id", nil)
	if err != nil {
		return nil, err
	}

	var o DownloadFileOptions
	if opt != nil {
		o = *opt
	}
	o.setOnRequest(req, fileId)

	return c.doRaw(req)
}

// DownloadFileByName downloads a file using the authorization previously retrieved via Authorize.
// Requires readFiles capabilities
func (c *Client) DownloadFileByName(ctx context.Context, bucketName, fileName string, opt DownloadFileOptions) (*http.Response, error) {
	path := fmt.Sprintf("/files/%s/%s", bucketName, fileName)
	req, err := c.downloadRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}

	opt.setOnRequest(req, "")

	return c.doRaw(req)
}

// FinishLargeFile combines all previously uploaded file parts into one large
// file. Requires Authorize to have been called. If this call times out, use
// GetFileInfo to verify if the file has been merged
func (c *Client) FinishLargeFile(ctx context.Context, fileId string, partSha1s []string) (FinishLargeFileResponse, error) {
	type request struct {
		FileId        string   `json:"fileId"`
		PartSha1Array []string `json:"partSha1Array"`
	}
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_finish_large_files", &request{fileId, partSha1s})
	if err != nil {
		return FinishLargeFileResponse{}, err
	}

	var r FinishLargeFileResponse
	err = c.do(req, &r)
	return r, err
}

type GetDownloadAuthorizationOptions struct {
	BucketId               string `json:"bucketId"`                       // required
	FileNamePrefix         string `json:"fileNamePrefix"`                 // required
	ValidDurationInSeconds int    `json:"validDurationInSeconds"`         // required, min is 1 second, max is 604800 which is one week in seconds
	ContentDisposition     string `json:"b2ContentDisposition,omitempty"` // optional, RFC 6266
	ContentLanguage        string `json:"b2ContentLanguage,omitempty"`    // optional, RFC 2616
	Expires                string `json:"b2Expires,omitempty"`            // optional, RFC 2616
	CacheControl           string `json:"b2CacheControl,omitempty"`       // optional, RFC 2616
	ContentEncoding        string `json:"b2ContentEncoding,omitempty"`    // optional, RFC 2616
	ContentType            string `json:"b2ContentType,omitempty"`        // optional, RFC 2616
}

// GetDownloadAuthorization Generates a temporary authorization token to
// download a file via DownloadFileByName. Requires Authorize to have been
// called.
func (c *Client) GetDownloadAuthorization(ctx context.Context, opt GetDownloadAuthorizationOptions) (GetDownloadAuthorizationResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_get_download_authorization", opt)
	if err != nil {
		return GetDownloadAuthorizationResponse{}, err
	}

	var r GetDownloadAuthorizationResponse
	err = c.do(req, &r)
	return r, err
}

// GetFileInfo returns metadata about a file stored in B2. Requires Authorize
// to have been called.
func (c *Client) GetFileInfo(ctx context.Context, fileId string) (GetFileInfoResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_get_file_info", &requestByFileID{fileId})
	if err != nil {
		return GetFileInfoResponse{}, err
	}

	var r GetFileInfoResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) GetUploadPartURL(ctx context.Context, fileId string) (GetUploadPartURLResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_get_upload_part_url", &requestByFileID{fileId})
	if err != nil {
		return GetUploadPartURLResponse{}, err
	}

	var r GetUploadPartURLResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) GetUploadURL(ctx context.Context, bucketId string) (GetUploadURLResponse, error) {
	type request struct {
		BucketId string `json:"bucketId"`
	}
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_get_upload_url", &request{bucketId})
	if err != nil {
		return GetUploadURLResponse{}, err
	}

	var r GetUploadURLResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) HideFile(ctx context.Context, bucketId, fileName string) (HideFileResponse, error) {
	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_hide_file", &requestByFileName{bucketId, fileName})
	if err != nil {
		return HideFileResponse{}, err
	}

	var r HideFileResponse
	err = c.do(req, &r)
	return r, err
}

type ListBucketsOptions struct {
	BucketId    string       // optional
	BucketName  string       // optional
	BucketTypes []BucketType // optional
}

func (c *Client) ListBuckets(ctx context.Context, opt *ListBucketsOptions) (ListBucketsResponse, error) {
	type request struct {
		AccountId   string       `json:"accountId"`             // required
		BucketId    string       `json:"bucketId,omitempty"`    // optional
		BucketName  string       `json:"bucketName,omitempty"`  // optional
		BucketTypes []BucketType `json:"bucketTypes,omitempty"` // optional
	}

	var o ListBucketsOptions

	if opt != nil {
		o = *opt
	}

	auth := c.LastAuth()
	if auth == nil {
		return ListBucketsResponse{}, ErrAuthTokenMissing
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_list_buckets", &request{
		auth.AccountID,
		o.BucketId,
		o.BucketName,
		o.BucketTypes,
	})
	if err != nil {
		return ListBucketsResponse{}, err
	}

	var r ListBucketsResponse
	err = c.do(req, &r)
	return r, err
}

type ListFileNamesOptions struct {
	StartFileName string // optional, starting offset filename for pagination
	MaxFileCount  int    // optional, number of files to return, 0 = default of 100, fee on every 1000 items returned
	Prefix        string // optional, objects must have this key prefix
	Delimiter     string // optional, empty means list all files, "/" means list top level files and folders
}

func (c *Client) ListFileNames(ctx context.Context, bucketId string, opt *ListFileNamesOptions) (ListFileNamesResponse, error) {
	type request struct {
		BucketId      string `json:"bucketId"`
		StartFileName string `json:"startFileName,omitempty"`
		MaxFileCount  int    `json:"maxFileCount,omitempty"`
		Prefix        string `json:"prefix,omitempty"`
		Delimiter     string `json:"delimiter,omitempty"`
	}
	var o ListFileNamesOptions
	if opt != nil {
		o = *opt
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_list_file_names", &request{
		bucketId,
		o.StartFileName,
		o.MaxFileCount,
		o.Prefix,
		o.Delimiter,
	})
	if err != nil {
		return ListFileNamesResponse{}, err
	}

	var r ListFileNamesResponse
	err = c.do(req, &r)
	return r, err
}

type ListFileVersionsOptions struct {
	StartFileName string // optional, starting offset filename for pagination
	StartFileId   string // optional, first file id to return, must set StartFileName if this is provided
	MaxFileCount  int    // optional, number of files to return, 0 = default of 100, fee on every 1000 items returned
	Prefix        string // optional, objects must have this key prefix
	Delimiter     string // optional, empty means list all files, "/" means list top level files and folders
}

func (c *Client) ListFileVersions(ctx context.Context, bucketId string, opt *ListFileVersionsOptions) (ListFileVersionsResponse, error) {
	type request struct {
		BucketId      string `json:"bucketId"`
		StartFileName string `json:"startFileName,omitempty"`
		StartFileId   string `json:"startFileId,omitempty"`
		MaxFileCount  int    `json:"maxFileCount,omitempty"`
		Prefix        string `json:"prefix,omitempty"`
		Delimiter     string `json:"delimiter,omitempty"`
	}

	var o ListFileVersionsOptions
	if opt != nil {
		o = *opt
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_list_file_versions", &request{
		bucketId,
		o.StartFileName,
		o.StartFileId,
		o.MaxFileCount,
		o.Prefix,
		o.Delimiter,
	})
	if err != nil {
		return ListFileVersionsResponse{}, err
	}

	var r ListFileVersionsResponse
	err = c.do(req, &r)
	return r, err
}

type ListKeysOptions struct {
	MaxKeyCount   int    // optional, max number of keys to return, default means 100
	StartAppKeyId string // optional, first key to return for paginated results
}

func (c *Client) ListKeys(ctx context.Context, opt ListKeysOptions) (ListKeysResponse, error) {
	type request struct {
		AccountId             string `json:"accountId"`
		MaxKeyCount           int    `json:"maxKeyCount"`
		StartApplicationKeyId string `json:"startApplicationKeyId"`
	}

	auth := c.LastAuth()
	if auth == nil {
		return ListKeysResponse{}, ErrAuthTokenMissing
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_list_keys", &request{
		auth.AccountID,
		opt.MaxKeyCount,
		opt.StartAppKeyId,
	})
	if err != nil {
		return ListKeysResponse{}, err
	}

	var r ListKeysResponse
	err = c.do(req, &r)
	return r, err
}

type ListPartsOptions struct {
	StartPartNumber *int // optional, max number of keys to return, default means 100
	MaxPartCount    *int // optional, first key to return for paginated results
}

func (c *Client) ListParts(ctx context.Context, fileId string, opt ListPartsOptions) (ListPartsResponse, error) {
	type request struct {
		FileId          string `json:"fileId"`
		StartPartNumber *int   `json:"startPartNumber,omitempty"`
		MaxPartCount    *int   `json:"maxPartCount,omitempty"`
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_list_parts", &request{
		fileId,
		opt.StartPartNumber,
		opt.MaxPartCount,
	})
	if err != nil {
		return ListPartsResponse{}, err
	}

	var r ListPartsResponse
	err = c.do(req, &r)
	return r, err
}

type ListUnfinishedLargeFilesOptions struct {
	NamePrefix   string // optional
	StartFileId  string // optional
	MaxFileCount int    // optional, max number of files to return, default is 100, max is 100
}

func (c *Client) ListUnfinishedLargeFiles(ctx context.Context, bucketId string, opt ListUnfinishedLargeFilesOptions) (ListUnfinishedLargeFilesResponse, error) {
	type request struct {
		BucketId     string `json:"bucketId"`
		NamePrefix   string `json:"namePrefix"`
		StartFileId  string `json:"startFileId"`
		MaxPartCount int    `json:"maxPartCount"`
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_list_unfinished_large_files", &request{
		bucketId,
		opt.NamePrefix,
		opt.StartFileId,
		opt.MaxFileCount,
	})
	if err != nil {
		return ListUnfinishedLargeFilesResponse{}, err
	}

	var r ListUnfinishedLargeFilesResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) StartLargeFile(ctx context.Context, bucketId, fileName, contentType string, fileInfo *FileInfo) (StartLargeFileResponse, error) {
	type request struct {
		BucketId    string    `json:"bucketId"`
		FileName    string    `json:"namePrefix"`
		ContentType string    `json:"startFileId"`
		FileInfo    *FileInfo `json:"fileInfo,omitempty"`
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_list_unfinished_large_files", &request{
		bucketId,
		fileName,
		contentType,
		fileInfo,
	})
	if err != nil {
		return StartLargeFileResponse{}, err
	}

	var r StartLargeFileResponse
	err = c.do(req, &r)
	return r, err
}

type UpdateBucketOptions struct {
	BucketType     BucketType      // optional
	BucketInfo     BucketInfo      // optional
	CorsRules      []CorsRule      // optional
	LifecycleRules []LifecycleRule // optional
	IfRevisionIs   *int            // optional
}

func (c *Client) UpdateBucket(ctx context.Context, bucketId string, opt UpdateBucketOptions) (UpdateBucketResponse, error) {
	type request struct {
		AccountId      string          `json:"accountId"`
		BucketId       string          `json:"bucketId"`
		BucketType     BucketType      `json:"bucketType,omitempty"`
		BucketInfo     BucketInfo      `json:"bucketInfo,omitempty"`
		CorsRules      []CorsRule      `json:"corsRules,omitempty"`
		LifecycleRules []LifecycleRule `json:"lifecycleRules,omitempty"`
		IfRevisionIs   *int            `json:"ifRevisionIs,omitempty"`
	}

	auth := c.LastAuth()
	if auth == nil {
		return UpdateBucketResponse{}, ErrAuthTokenMissing
	}

	req, err := c.authRequest(ctx, "POST", "/b2api/v2/b2_update_bucket", &request{
		auth.AccountID,
		bucketId,
		opt.BucketType,
		opt.BucketInfo,
		opt.CorsRules,
		opt.LifecycleRules,
		opt.IfRevisionIs,
	})
	if err != nil {
		return UpdateBucketResponse{}, err
	}

	var r UpdateBucketResponse
	err = c.do(req, &r)
	return r, err
}

type UploadFileOptions struct {
	FileName      string        // required
	ContentType   string        // required, use ContentTypeHide to hide, empty defaults to auto
	ContentLength int64         // required, use ContentLengthDetermineUsingTempStorage to determine it using temp storage
	Body          io.ReadCloser // required

	ContentSha1 string // required, leave empty to interpret from body

	SrcLastModified     *time.Time        // optional
	ContentDisposition  string            // optional, RFC 2616
	ContentLanguage     string            // optional, RFC 2616
	Expires             string            // optional, RFC 2616
	CacheControl        string            // optional
	ContentEncoding     string            // optional, RFC 2616
	DownloadContentType string            // optional, RFC 2616
	ExtraHeaders        map[string]string // extra headers to add, currently must be prefixed with "X-Bz-Info-*" and * should use underscores over hyphens
}

func (c *Client) UploadFile(ctx context.Context, uploadURL, authToken string, opt UploadFileOptions) (UploadFileResponse, error) {
	req, err := c.uploadRequest(ctx, uploadURL, authToken)
	if err != nil {
		return UploadFileResponse{}, err
	}

	err = opt.setOnRequest(req, c.TS)
	if err != nil {
		return UploadFileResponse{}, err
	}

	var r UploadFileResponse
	err = c.do(req, &r)
	return r, err
}

func readerLength(ts TempStorage, r io.ReadCloser) (io.ReadCloser, int64, error) {
	if ts == nil {
		buf := bytes.NewBuffer(nil)
		n, err := io.Copy(buf, r)
		if err != nil {
			return nil, 0, err
		}
		return Closer(buf), n, r.Close()
	} else {
		f, n, err := ts.Store(r)
		if err != nil {
			return nil, 0, err
		}
		return f, n, r.Close()
	}
}

func (opt *UploadFileOptions) setOnRequest(r *http.Request, ts TempStorage) error {
	r.Header.Set("X-Bz-File-Name", opt.FileName)
	if opt.ContentType == "" {
		r.Header.Set("Content-Type", ContentTypeAuto)
	} else {
		r.Header.Set("Content-Type", opt.ContentType)
	}

	var body = opt.Body
	length := opt.ContentLength

	if length < 0 {
		var err error
		body, length, err = readerLength(ts, body)
		if err != nil {
			return err
		}
	}

	if opt.ContentSha1 == "" || opt.ContentSha1 == Sha1AtEnd {
		rdr := &HashedPostfixedReader{R: body, H: sha1.New()}
		r.Body = rdr
		length += 40 // sha1 -> hex is 40 bytes
		r.Header.Set("X-Bz-Content-Sha1", Sha1AtEnd)
	} else {
		r.Body = body
		r.Header.Set("X-Bz-Content-Sha1", opt.ContentSha1)
	}
	r.ContentLength = length

	if opt.SrcLastModified != nil {
		r.Header.Set("X-Bz-src_last_modified_millis", strconv.Itoa(int(opt.SrcLastModified.Unix())))
	}

	if opt.ContentDisposition != "" {
		r.Header.Set("X-Bz-Info-b2-content-disposition", opt.ContentDisposition)
	}

	if opt.ContentLanguage != "" {
		r.Header.Set("X-Bz-Info-b2-content-language", opt.ContentLanguage)
	}

	if opt.Expires != "" {
		r.Header.Set("X-Bz-Info-b2-expires", opt.Expires)
	}

	if opt.CacheControl != "" {
		r.Header.Set("X-Bz-Info-b2-cache-control", opt.CacheControl)
	}

	if opt.ContentEncoding != "" {
		r.Header.Set("X-Bz-Info-b2-content-encoding", opt.ContentEncoding)
	}

	if opt.DownloadContentType != "" {
		r.Header.Set("X-Bz-Info-b2-content-type", opt.DownloadContentType)
	}

	for k, v := range opt.ExtraHeaders {
		r.Header.Set(k, v)
	}
	return nil
}

type UploadFilePartOptions struct {
	ContentType   string        // required, use ContentTypeHide to hide, empty defaults to auto
	ContentLength int64         // required, if negative use temp storage to buffer the result for caching
	Body          io.ReadCloser // required
	ContentSha1   string        // required, sha1 of the part being uploaded, leave empty to interpret from body
}

func (c *Client) UploadPart(ctx context.Context, uploadPartURL, uploadPartAuthToken string, opt UploadFilePartOptions) (UploadPartResponse, error) {
	req, err := c.uploadRequest(ctx, uploadPartURL, uploadPartAuthToken)
	if err != nil {
		return UploadPartResponse{}, err
	}

	err = opt.setOnRequest(req, c.TS)
	if err != nil {
		return UploadPartResponse{}, err
	}

	var r UploadPartResponse
	err = c.do(req, &r)
	return r, err
}

func (opt *UploadFilePartOptions) setOnRequest(r *http.Request, ts TempStorage) error {
	if opt.ContentType == "" {
		r.Header.Set("Content-Type", ContentTypeAuto)
	} else {
		r.Header.Set("Content-Type", opt.ContentType)
	}

	var body = opt.Body
	length := opt.ContentLength

	if length < 0 {
		var err error
		body, length, err = readerLength(ts, body)
		if err != nil {
			return err
		}
	}

	if opt.ContentSha1 == "" {
		rdr := &HashedPostfixedReader{R: opt.Body, H: sha1.New()}
		r.Body = rdr
		length += 40 // sha1 -> hex is 40 bytes
	} else {
		r.Body = opt.Body
		r.Header.Set("X-Bz-Content-Sha1", opt.ContentSha1)
	}
	r.ContentLength = length
	return nil
}
