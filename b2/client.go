package b2

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	debugRequests        = false
	debugResponses       = false
	testRetries          = false
	testExpireSomeTokens = false
	testCapExceeded      = false
)

type Logger interface {
	Printf(format string, values ...interface{})
}

// Client manages most of the low-level operations.
// Client is not thread safe.
type Client struct {
	AuthURL   string
	UserAgent string
	C         http.Client
	L         Logger
	LastAuth  *AuthorizeAccountResponse // last successful auth response
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

func (c *Client) request(baseURL, method, endpoint string, body interface{}) (*http.Request, error) {
	if baseURL == "" {
		baseURL = "https://api.backblazeb2.com"
	}
	var req *http.Request
	var err error
	if body == nil {
		req, err = http.NewRequest(method, baseURL+endpoint, nil)
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

func (c *Client) authRequest(method, endpoint string, body interface{}) (*http.Request, error) {
	if c.LastAuth == nil {
		return nil, ErrAuthTokenMissing
	}

	authToken := c.LastAuth.AuthorizationToken
	req, err := c.request(c.LastAuth.APIURL, method, endpoint, body)
	if err != nil {
		return req, err
	}
	req.Header.Add("Authorization", authToken)
	return req, err
}

func (c *Client) uploadRequest(uploadURL, uploadAuthToken string) (*http.Request, error) {
	if c.LastAuth == nil {
		return nil, ErrAuthTokenMissing
	}

	req, err := c.request(uploadURL, "POST", "", nil)
	if err != nil {
		return req, err
	}
	req.Header.Add("Authorization", uploadAuthToken)
	return req, err
}

func (c *Client) downloadRequest(method, endpoint string, body interface{}) (*http.Request, error) {
	if c.LastAuth == nil {
		return nil, ErrAuthTokenMissing
	}

	authToken := c.LastAuth.AuthorizationToken
	req, err := c.request(c.LastAuth.DownloadURL, method, endpoint, body)
	if err != nil {
		return req, err
	}
	req.Header.Add("Authorization", authToken)
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

// Authorize exchanges a keyId and appKey for an authorization token. Auth tokens can be used for other API calls.
func (c *Client) Authorize(keyId, appKey string) (AuthorizeAccountResponse, error) {
	req, err := c.request("", "GET", "/b2api/v2/b2_authorize_account", nil)
	if err != nil {
		return AuthorizeAccountResponse{}, err
	}
	req.SetBasicAuth(keyId, appKey)
	var r AuthorizeAccountResponse
	err = c.do(req, &r)
	if err == nil {
		c.LastAuth = &r
	}
	return r, err
}

// CancelLargeFile cancels an inprogress file upload. Requires Authorize to be called first.
func (c *Client) CancelLargeFile(fileId string) (CancelLargeFileResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_cancel_large_file", &requestByFileID{fileId})
	if err != nil {
		return CancelLargeFileResponse{}, err
	}

	var r CancelLargeFileResponse
	err = c.do(req, &r)
	return r, err
}

const (
	MetadataDirectiveCopy    = "COPY"
	MetadataDirectiveReplace = "REPLACE"
)

type CopyFileOptions struct {
	SourceFileId        string   `json:"sourceFileId"` // required
	FileName            string   `json:"fileName"`     // required
	DestinationBucketId string   `json:"destinationBucketId,omitempty"`
	Range               string   `json:"range,omitempty"` // in form: "bytes=1000-2000"
	MetadataDirective   string   `json:"metadataDirective,omitempty"`
	ContentType         string   `json:"contentType,omitempty"`
	FileInfo            FileInfo `json:"fileInfo,omitempty"`
}

// CopyFile copies a file in the bucket to another location. Requires Authorize to be called first.
func (c *Client) CopyFile(opt CopyFileOptions) (CopyFileResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_copy_file", &opt)
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

// CopyPart copies a part of a file file in the bucket to another location. Requires Authorize to be called first.
func (c *Client) CopyPart(opt CopyPartOptions) (CopyPartResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_copy_part", &opt)
	if err != nil {
		return CopyPartResponse{}, err
	}

	var r CopyPartResponse
	err = c.do(req, &r)
	return r, err
}

type CreateBucketOptions struct {
	AccountId      string          `json:"accountId"`  // required
	BucketName     string          `json:"bucketName"` // required
	BucketType     BucketType      `json:"bucketType"` // required
	BucketInfo     BucketInfo      `json:"bucketInfo,omitempty"`
	CorsRules      []CorsRule      `json:"corsRules,omitempty"`
	LifecycleRules []LifecycleRule `json:"lifecycleRules,omitempty"`
}

// CreateBucket creates a new bucket in the given account. Requires Authorize to be called first.
func (c *Client) CreateBucket(opt CreateBucketOptions) (BucketResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_create_bucket", &opt)
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
func (c *Client) CreateKey(opt CreateKeyOptions) (KeyResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_create_key", &opt)
	if err != nil {
		return KeyResponse{}, err
	}

	var r KeyResponse
	err = c.do(req, &r)
	return r, err
}

// DeleteBucket deletes an existing bucket within an account. Requires Authorize to be called first.
func (c *Client) DeleteBucket(bucketId string) (BucketResponse, error) {
	type request struct {
		AccountId string `json:"accountId"`
		BucketId  string `json:"bucketId"`
	}
	accountId := c.LastAuth.AccountID
	req, err := c.authRequest("POST", "/b2api/v2/b2_delete_bucket", &request{accountId, bucketId})
	if err != nil {
		return BucketResponse{}, err
	}

	var r BucketResponse
	err = c.do(req, &r)
	return r, err
}

// DeleteFileVersion deletes a version of a file. Requires Authorize to be called first.
func (c *Client) DeleteFileVersion(fileId, fileName string) (DeleteFileResponse, error) {
	type request struct {
		FileId   string `json:"fileId"`
		FileName string `json:"fileName"`
	}
	req, err := c.authRequest("POST", "/b2api/v2/b2_delete_file_version", &request{fileId, fileName})
	if err != nil {
		return DeleteFileResponse{}, err
	}

	var r DeleteFileResponse
	err = c.do(req, &r)
	return r, err
}

// DeleteKey deletes an API key. Requires Authorize to be called first
func (c *Client) DeleteKey(appKeyId string) (KeyResponse, error) {
	type request struct {
		AppKeyId string `json:"applicationKeyId"`
	}
	req, err := c.authRequest("POST", "/b2api/v2/b2_delete_key", &request{appKeyId})
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

// DownloadFileById downloads a file using the authorization previously retrieved via Authorize.
// Requires readFiles capabilities
func (c *Client) DownloadFileById(fileId string, opt DownloadFileOptions) (*http.Response, error) {
	req, err := c.downloadRequest("GET", "/b2api/v2/b2_download_file_by_id", nil)
	if err != nil {
		return nil, err
	}

	opt.setOnRequest(req, fileId)

	return c.doRaw(req)
}

// DownloadFileByName downloads a file using the authorization previously retrieved via Authorize.
// Requires readFiles capabilities
func (c *Client) DownloadFileByName(bucketName, fileName string, opt DownloadFileOptions) (*http.Response, error) {
	path := fmt.Sprintf("/files/%s/%s", bucketName, fileName)
	req, err := c.downloadRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	opt.setOnRequest(req, "")

	return c.doRaw(req)
}

// FinishLargeFile combines all previously uploaded file parts into one large file. Requires Authorize to have been called.
// If this call times out, use GetFileInfo to verify if the file has been merged
func (c *Client) FinishLargeFile(fileId string, partSha1s []string) (FinishLargeFileResponse, error) {
	type request struct {
		FileId        string   `json:"fileId"`
		PartSha1Array []string `json:"partSha1Array"`
	}
	req, err := c.authRequest("POST", "/b2api/v2/b2_finish_large_files", &request{fileId, partSha1s})
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

func (c *Client) GetDownloadAuthorization(opt GetDownloadAuthorizationOptions) (GetDownloadAuthorizationResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_get_download_authorization", opt)
	if err != nil {
		return GetDownloadAuthorizationResponse{}, err
	}

	var r GetDownloadAuthorizationResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) GetFileInfo(fileId string) (GetFileInfoResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_get_file_info", &requestByFileID{fileId})
	if err != nil {
		return GetFileInfoResponse{}, err
	}

	var r GetFileInfoResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) GetUploadPartURL(fileId string) (GetUploadPartURLResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_get_upload_part_url", &requestByFileID{fileId})
	if err != nil {
		return GetUploadPartURLResponse{}, err
	}

	var r GetUploadPartURLResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) GetUploadURL(bucketId string) (GetUploadURLResponse, error) {
	type request struct {
		BucketId string `json:"bucketId"`
	}
	req, err := c.authRequest("POST", "/b2api/v2/b2_get_upload_url", &request{bucketId})
	if err != nil {
		return GetUploadURLResponse{}, err
	}

	var r GetUploadURLResponse
	err = c.do(req, &r)
	return r, err
}

func (c *Client) HideFile(bucketId, fileName string) (HideFileResponse, error) {
	req, err := c.authRequest("POST", "/b2api/v2/b2_hide_file", &requestByFileName{bucketId, fileName})
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

func (c *Client) ListBuckets(opt *ListBucketsOptions) (ListBucketsResponse, error) {
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

	req, err := c.authRequest("POST", "/b2api/v2/b2_list_buckets", &request{
		c.LastAuth.AccountID,
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

func (c *Client) ListFileNames(bucketId string, opt *ListFileNamesOptions) (ListFileNamesResponse, error) {
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

	req, err := c.authRequest("POST", "/b2api/v2/b2_list_file_names", &request{
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

func (c *Client) ListFileVersions(bucketId string, opt ListFileVersionsOptions) (ListFileVersionsResponse, error) {
	type request struct {
		BucketId      string `json:"bucketId"`
		StartFileName string `json:"startFileName,omitempty"`
		StartFileId   string `json:"startFileId,omitempty"`
		MaxFileCount  int    `json:"maxFileCount,omitempty"`
		Prefix        string `json:"prefix,omitempty"`
		Delimiter     string `json:"delimiter,omitempty"`
	}

	req, err := c.authRequest("POST", "/b2api/v2/b2_list_file_versions", &request{
		bucketId,
		opt.StartFileName,
		opt.StartFileId,
		opt.MaxFileCount,
		opt.Prefix,
		opt.Delimiter,
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

func (c *Client) ListKeys(opt ListKeysOptions) (ListKeysResponse, error) {
	type request struct {
		AccountId             string `json:"accountId"`
		MaxKeyCount           int    `json:"maxKeyCount"`
		StartApplicationKeyId string `json:"startApplicationKeyId"`
	}

	req, err := c.authRequest("POST", "/b2api/v2/b2_list_keys", &request{
		c.LastAuth.AccountID,
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

func (c *Client) ListParts(fileId string, opt ListPartsOptions) (ListPartsResponse, error) {
	type request struct {
		FileId          string `json:"fileId"`
		StartPartNumber *int   `json:"startPartNumber,omitempty"`
		MaxPartCount    *int   `json:"maxPartCount,omitempty"`
	}

	req, err := c.authRequest("POST", "/b2api/v2/b2_list_parts", &request{
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

func (c *Client) ListUnfinishedLargeFiles(bucketId string, opt ListUnfinishedLargeFilesOptions) (ListUnfinishedLargeFilesResponse, error) {
	type request struct {
		BucketId     string `json:"bucketId"`
		NamePrefix   string `json:"namePrefix"`
		StartFileId  string `json:"startFileId"`
		MaxPartCount int    `json:"maxPartCount"`
	}

	req, err := c.authRequest("POST", "/b2api/v2/b2_list_unfinished_large_files", &request{
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

func (c *Client) StartLargeFile(bucketId, fileName, contentType string, fileInfo *FileInfo) (StartLargeFileResponse, error) {
	type request struct {
		BucketId    string    `json:"bucketId"`
		FileName    string    `json:"namePrefix"`
		ContentType string    `json:"startFileId"`
		FileInfo    *FileInfo `json:"fileInfo,omitempty"`
	}

	req, err := c.authRequest("POST", "/b2api/v2/b2_list_unfinished_large_files", &request{
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

func (c *Client) UpdateBucket(bucketId string, opt UpdateBucketOptions) (UpdateBucketResponse, error) {
	type request struct {
		AccountId      string          `json:"accountId"`
		BucketId       string          `json:"bucketId"`
		BucketType     BucketType      `json:"bucketType,omitempty"`
		BucketInfo     BucketInfo      `json:"bucketInfo,omitempty"`
		CorsRules      []CorsRule      `json:"corsRules,omitempty"`
		LifecycleRules []LifecycleRule `json:"lifecycleRules,omitempty"`
		IfRevisionIs   *int            `json:"ifRevisionIs,omitempty"`
	}

	req, err := c.authRequest("POST", "/b2api/v2/b2_update_bucket", &request{
		c.LastAuth.AccountID,
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
	ContentLength int64         // required
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

func (c *Client) UploadFile(uploadURL, authToken string, opt UploadFileOptions) (UploadFileResponse, error) {
	req, err := c.uploadRequest(uploadURL, authToken)
	if err != nil {
		return UploadFileResponse{}, err
	}

	opt.setOnRequest(req)

	var r UploadFileResponse
	err = c.do(req, &r)
	return r, err
}

func (opt *UploadFileOptions) setOnRequest(r *http.Request) {
	r.Header.Set("X-Bz-File-Name", opt.FileName)
	if opt.ContentType == "" {
		r.Header.Set("Content-Type", ContentTypeAuto)
	} else {
		r.Header.Set("Content-Type", opt.ContentType)
	}
	length := opt.ContentLength
	if opt.ContentSha1 == "" || opt.ContentSha1 == Sha1AtEnd {
		rdr := &HashedPostfixedReader{R: opt.Body, H: sha1.New()}
		r.Body = rdr
		length += 40 // sha1 -> hex is 40 bytes
		r.Header.Set("X-Bz-Content-Sha1", Sha1AtEnd)
	} else {
		r.Body = opt.Body
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
}

type UploadFilePartOptions struct {
	ContentType   string        // required, use ContentTypeHide to hide, empty defaults to auto
	ContentLength int           // required
	Body          io.ReadCloser // required
	ContentSha1   string        // required, sha1 of the part being uploaded, leave empty to interpret from body
}

func (c *Client) UploadPart(uploadPartURL, uploadPartAuthToken string, opt UploadFilePartOptions) (UploadPartResponse, error) {
	req, err := c.uploadRequest(uploadPartURL, uploadPartAuthToken)
	if err != nil {
		return UploadPartResponse{}, err
	}

	opt.setOnRequest(req)

	var r UploadPartResponse
	err = c.do(req, &r)
	return r, err
}

func (opt *UploadFilePartOptions) setOnRequest(r *http.Request) {
	if opt.ContentType == "" {
		r.Header.Set("Content-Type", ContentTypeAuto)
	} else {
		r.Header.Set("Content-Type", opt.ContentType)
	}
	length := opt.ContentLength
	if opt.ContentSha1 == "" {
		rdr := &HashedPostfixedReader{R: opt.Body, H: sha1.New()}
		r.Body = rdr
		length += rdr.H.Size()
	} else {
		r.Body = opt.Body
		r.Header.Set("X-Bz-Content-Sha1", opt.ContentSha1)
	}
	r.Header.Set("Content-Length", strconv.Itoa(length))
}
